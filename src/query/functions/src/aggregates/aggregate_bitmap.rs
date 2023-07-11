// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::alloc::Layout;
use std::fmt;
use std::marker::PhantomData;
use std::ops::BitAndAssign;
use std::ops::BitOrAssign;
use std::ops::BitXorAssign;
use std::ops::SubAssign;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::*;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_io::prelude::BinaryWrite;
use roaring::RoaringTreemap;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::assert_variadic_arguments;
use super::StateAddr;
use super::StateAddrs;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::with_simple_no_number_mapped_type;

#[derive(Clone)]
struct AggregateBitmapFunction<OP, AGG> {
    display_name: String,
    _op: PhantomData<OP>,
    _agg: PhantomData<AGG>,
}

impl<OP, AGG> AggregateBitmapFunction<OP, AGG>
where
    OP: BitmapOperate,
    AGG: BitmapAggResult,
{
    fn try_create(display_name: &str) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateBitmapFunction::<OP, AGG> {
            display_name: display_name.to_string(),
            _op: PhantomData,
            _agg: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

const BITMAP_AGG_RAW: u8 = 0;
const BITMAP_AGG_COUNT: u8 = 1;

macro_rules! with_bitmap_agg_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                BITMAP_AGG_RAW   => BitmapRawResult,
                BITMAP_AGG_COUNT => BitmapCountResult,
            ],
            $($tail)*
        }
    }
}

trait BitmapAggResult: Send + Sync + 'static {
    fn merge_result(place: StateAddr, builder: &mut ColumnBuilder) -> Result<()>;

    fn return_type() -> Result<DataType>;
}

struct BitmapCountResult;
struct BitmapRawResult;

impl BitmapAggResult for BitmapCountResult {
    fn merge_result(place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = UInt64Type::try_downcast_builder(builder).unwrap();
        let state = place.get::<BitmapAggState>();
        builder.push(state.rb.as_ref().map(|rb| rb.len()).unwrap_or(0));
        Ok(())
    }

    fn return_type() -> Result<DataType> {
        Ok(UInt64Type::data_type())
    }
}

impl BitmapAggResult for BitmapRawResult {
    fn merge_result(place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = BitmapType::try_downcast_builder(builder).unwrap();
        let state = place.get::<BitmapAggState>();
        if let Some(rb) = state.rb.as_ref() {
            rb.serialize_into(&mut builder.data)?;
        };
        builder.commit_row();
        Ok(())
    }

    fn return_type() -> Result<DataType> {
        Ok(BitmapType::data_type())
    }
}

const BITMAP_AND: u8 = 0;
const BITMAP_OR: u8 = 1;
const BITMAP_XOR: u8 = 2;
const BITMAP_NOT: u8 = 3;

macro_rules! with_bitmap_op_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                BITMAP_AND => BitmapAndOp,
                BITMAP_OR  => BitmapOrOp,
                BITMAP_XOR => BitmapXorOp,
                BITMAP_NOT => BitmapNotOp,
            ],
            $($tail)*
        }
    }
}

trait BitmapOperate: Send + Sync + 'static {
    fn operate(lhs: &mut RoaringTreemap, rhs: RoaringTreemap);
}

struct BitmapAndOp;
struct BitmapOrOp;
struct BitmapXorOp;
struct BitmapNotOp;

impl BitmapOperate for BitmapAndOp {
    fn operate(lhs: &mut RoaringTreemap, rhs: RoaringTreemap) {
        lhs.bitand_assign(rhs);
    }
}

impl BitmapOperate for BitmapOrOp {
    fn operate(lhs: &mut RoaringTreemap, rhs: RoaringTreemap) {
        lhs.bitor_assign(rhs);
    }
}

impl BitmapOperate for BitmapXorOp {
    fn operate(lhs: &mut RoaringTreemap, rhs: RoaringTreemap) {
        lhs.bitxor_assign(rhs);
    }
}

impl BitmapOperate for BitmapNotOp {
    fn operate(lhs: &mut RoaringTreemap, rhs: RoaringTreemap) {
        lhs.sub_assign(rhs);
    }
}

struct BitmapAggState {
    rb: Option<RoaringTreemap>,
}

impl BitmapAggState {
    fn new() -> Self {
        Self { rb: None }
    }

    fn add<OP: BitmapOperate>(&mut self, other: RoaringTreemap) {
        match &mut self.rb {
            Some(v) => {
                OP::operate(v, other);
            }
            None => {
                self.rb = Some(other);
            }
        }
    }
}

impl<OP, AGG> AggregateFunction for AggregateBitmapFunction<OP, AGG>
where
    OP: BitmapOperate,
    AGG: BitmapAggResult,
{
    fn name(&self) -> &str {
        "AggregateBitmapFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        AGG::return_type()
    }

    fn init_state(&self, place: super::StateAddr) {
        place.write(BitmapAggState::new);
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<BitmapAggState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = BitmapType::try_downcast_column(&columns[0]).unwrap();
        if column.len() == 0 {
            return Ok(());
        }

        let column_iter = column.iter();
        let state = place.get::<BitmapAggState>();

        if let Some(validity) = validity {
            if validity.unset_bits() == column.len() {
                return Ok(());
            }

            for (data, valid) in column_iter.zip(validity.iter()) {
                if !valid {
                    continue;
                }
                let rb = RoaringTreemap::deserialize_from(data)?;
                state.add::<OP>(rb);
            }
        } else {
            for data in column_iter {
                let rb = RoaringTreemap::deserialize_from(data)?;
                state.add::<OP>(rb);
            }
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let column = BitmapType::try_downcast_column(&columns[0]).unwrap();

        for (data, place) in column.iter().zip(places.iter()) {
            let addr = place.next(offset);
            let state = addr.get::<BitmapAggState>();
            let rb = RoaringTreemap::deserialize_from(data)?;
            state.add::<OP>(rb);
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = BitmapType::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<BitmapAggState>();
        if let Some(data) = BitmapType::index_column(&column, row) {
            let rb = RoaringTreemap::deserialize_from(data)?;
            state.add::<OP>(rb);
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<BitmapAggState>();
        // flag indicate where bitmap is none
        let flag: u8 = if state.rb.is_some() { 1 } else { 0 };
        writer.write_scalar(&flag)?;
        if let Some(rb) = &state.rb {
            rb.serialize_into(writer)?;
        }
        Ok(())
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<BitmapAggState>();
        let flag = reader[0];
        state.rb = if flag == 1 {
            Some(RoaringTreemap::deserialize_from(&reader[1..])?)
        } else {
            None
        };
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<BitmapAggState>();
        let rhs = rhs.get::<BitmapAggState>();
        if let Some(rb) = &rhs.rb {
            state.add::<OP>(rb.clone());
        }
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        AGG::merge_result(place, builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<BitmapAggState>();
        std::ptr::drop_in_place(state);
    }
}

impl<OP, AGG> fmt::Display for AggregateBitmapFunction<OP, AGG> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

struct AggreagateBitmapIntersectCountFunction<T> {
    display_name: String,
    inner: AggregateBitmapFunction<BitmapOrOp, BitmapCountResult>,
    filter_values_len: usize,
    _t: PhantomData<T>,
}

impl<T> AggreagateBitmapIntersectCountFunction<T>
where T: ValueType + Send + Sync
{
    fn try_create(
        display_name: &str,
        filter_values_len: usize,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggreagateBitmapIntersectCountFunction::<T> {
            display_name: display_name.to_string(),
            inner: AggregateBitmapFunction {
                display_name: "".to_string(),
                _op: PhantomData,
                _agg: PhantomData,
            },
            filter_values_len,
            _t: PhantomData,
        };
        Ok(Arc::new(func))
    }

    fn get_filter_bitmap(&self, columns: &[Column]) -> Bitmap {
        let filter_col = T::try_downcast_column(&columns[1]).unwrap();

        let mut result = MutableBitmap::from_len_zeroed(columns[0].len());

        for i in 0..self.filter_values_len {
            let value_col = T::try_downcast_column(&columns[i + 2]).unwrap();
            let mut col_bitmap = MutableBitmap::new();
            T::iter_column(&filter_col)
                .zip(T::iter_column(&value_col))
                .for_each(|(filter, val)| {
                    col_bitmap.push(val == filter);
                });
            (&mut result).bitor_assign(&Bitmap::from(col_bitmap));
        }
        Bitmap::from(result)
    }

    fn filter_row(&self, columns: &[Column], row: usize) -> Result<bool> {
        let filter_col = T::try_downcast_column(&columns[1]).unwrap();

        let filter_val_opt = T::index_column(&filter_col, row).map(|v| T::to_owned_scalar(v));

        if let Some(filter_val) = filter_val_opt {
            for i in 0..self.filter_values_len {
                let value_col = T::try_downcast_column(&columns[i + 2]).unwrap();
                let matches = T::index_column(&value_col, row)
                    .map(|v| T::to_owned_scalar(v))
                    .map(|v| v == filter_val)
                    .unwrap_or(false);
                if matches {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn filter_place(places: &[StateAddr], predicate: &Bitmap) -> StateAddrs {
        if predicate.unset_bits() == 0 {
            return places.to_vec();
        }
        let it = predicate
            .iter()
            .zip(places.iter())
            .filter(|(v, _)| *v)
            .map(|(_, c)| *c);

        Vec::from_iter(it)
    }
}

impl<T> AggregateFunction for AggreagateBitmapIntersectCountFunction<T>
where T: ValueType + Send + Sync
{
    fn name(&self) -> &str {
        "AggreagateBitmapIntersectCountFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        self.inner.return_type()
    }

    fn init_state(&self, place: StateAddr) {
        self.inner.init_state(place);
    }

    fn state_layout(&self) -> Layout {
        self.inner.state_layout()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let predicate = self.get_filter_bitmap(columns);
        let bitmap = match validity {
            Some(validity) => validity & (&predicate),
            None => predicate,
        };
        self.inner
            .accumulate(place, columns, Some(&bitmap), input_rows)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let predicate = self.get_filter_bitmap(columns);
        let column = columns[0].filter(&predicate);

        let new_places = Self::filter_place(places, &predicate);
        let new_places_slice = new_places.as_slice();
        let row_size = predicate.len() - predicate.unset_bits();

        self.inner
            .accumulate_keys(new_places_slice, offset, vec![column].as_slice(), row_size)
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        if self.filter_row(columns, row)? {
            return self.inner.accumulate_row(place, columns, row);
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        self.inner.serialize(place, writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        self.inner.deserialize(place, reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        self.inner.merge(place, rhs)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        self.inner.merge_result(place, builder)
    }
}

impl<T> fmt::Display for AggreagateBitmapIntersectCountFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

pub fn try_create_aggregate_bitmap_function<const OP_TYPE: u8, const AGG_TYPE: u8>(
    display_name: &str,
    _params: Vec<Scalar>,
    argument_types: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, argument_types.len())?;
    let data_type = argument_types[0].clone();
    with_bitmap_op_mapped_type!(|OP| match OP_TYPE {
        OP => {
            with_bitmap_agg_mapped_type!(|AGG| match AGG_TYPE {
                AGG => {
                    match data_type {
                        DataType::Bitmap => {
                            AggregateBitmapFunction::<OP, AGG>::try_create(display_name)
                        }
                        _ => Err(ErrorCode::BadDataValueType(format!(
                            "{} does not support type '{:?}'",
                            display_name, data_type
                        ))),
                    }
                }
                _ => {
                    Err(ErrorCode::BadDataValueType(format!(
                        "Unsupported bitmap agg type for aggregate function {} (type number: {})",
                        display_name, AGG_TYPE
                    )))
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "Unsupported bitmap operate type for aggregate function {} (type number: {})",
            display_name, OP_TYPE
        ))),
    })
}

pub fn try_create_aggregate_bitmap_intersect_count_function(
    display_name: &str,
    _params: Vec<Scalar>,
    argument_types: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_variadic_arguments(display_name, argument_types.len(), (3, 32))?;

    if !argument_types[0].remove_nullable().is_bitmap() {
        return Err(ErrorCode::BadDataValueType(format!(
            "The first argument of aggregate function {} must be bitmap",
            display_name
        )));
    }

    let filter_column_type = argument_types[1].remove_nullable();

    // TODO type conversion?
    for i in 2..argument_types.len() {
        if argument_types[i].remove_nullable() != filter_column_type {
            return Err(ErrorCode::BadDataValueType(format!(
                "The filter arguments of aggregate function {} must be the same",
                display_name
            )));
        }
    }

    let filter_values_len = argument_types.len() - 2;

    with_simple_no_number_mapped_type!(|T| match filter_column_type {
        DataType::T => {
            AggreagateBitmapIntersectCountFunction::<T>::try_create(display_name, filter_values_len)
        }
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    AggreagateBitmapIntersectCountFunction::<NumberType<NUM>>::try_create(
                        display_name,
                        filter_values_len,
                    )
                }
            })
        }
        _ => {
            Err(ErrorCode::BadDataValueType(format!(
                "Unsupported type arguments of aggregate function {}",
                display_name
            )))
        }
    })
}

pub fn aggregate_bitmap_and_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_AND, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_not_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_NOT, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_or_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_OR, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_xor_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_XOR, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_union_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_OR, BITMAP_AGG_RAW>),
        features,
    )
}

pub fn aggregate_bitmap_intersect_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_AND, BITMAP_AGG_RAW>),
        features,
    )
}

pub fn aggregate_bitmap_intersect_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_intersect_count_function),
        features,
    )
}
