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

use databend_common_base::vec_ext::VecExt;
use databend_common_base::vec_ext::VecU8Ext;

use crate::types::binary::BinaryColumn;
use crate::types::decimal::DecimalColumn;
use crate::types::NumberColumn;
use crate::with_decimal_mapped_type;
use crate::with_number_mapped_type;
use crate::Column;
use crate::InputColumns;

/// The serialize_size is equal to the number of bytes required by serialization.
pub fn serialize_group_columns(
    columns: InputColumns,
    num_rows: usize,
    serialize_size: usize,
) -> BinaryColumn {
    // [`BinaryColumn`] consists of [`data`] and [`offset`], we build [`data`] and [`offset`] respectively,
    // and then call `BinaryColumn::new(data.into(), offsets.into())` to create [`BinaryColumn`].
    let mut data: Vec<u8> = Vec::with_capacity(serialize_size);
    let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
    unsafe {
        offsets.push_unchecked(0);
        for i in 0..num_rows {
            for col in columns.iter() {
                serialize_column_binary(col, i, &mut data);
            }
            offsets.push_unchecked(data.len() as u64);
        }
    }

    BinaryColumn::new(data.into(), offsets.into())
}

/// This function must be consistent with the `push_binary` function of `src/query/expression/src/values.rs`.
/// # Safety
///
/// * The size of the memory pointed by `row_space` is equal to the number of bytes required by serialization.
pub unsafe fn serialize_column_binary(column: &Column, row: usize, row_space: &mut Vec<u8>) {
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
        Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberColumn::NUM_TYPE(v) => {
                row_space.store_value_uncheckd(&v[row]);
            }
        }),
        Column::Decimal(v) => {
            with_decimal_mapped_type!(|DECIMAL_TYPE| match v {
                DecimalColumn::DECIMAL_TYPE(v, _) => {
                    row_space.store_value_uncheckd(&v[row]);
                }
            })
        }
        Column::Boolean(v) => row_space.push_unchecked(v.get_bit(row) as u8),
        Column::Binary(v) | Column::Bitmap(v) | Column::Variant(v) | Column::Geometry(v) => {
            let value = unsafe { v.index_unchecked(row) };
            let len = value.len();

            row_space.store_value_uncheckd(&(len as u64));
            row_space.extend_from_slice_unchecked(value);
        }
        Column::Geography(v) => {
            let value = unsafe { v.index_unchecked(row) };
            let value = borsh::to_vec(&value.0).unwrap();
            let len = value.len();

            row_space.store_value_uncheckd(&(len as u64));
            row_space.extend_from_slice_unchecked(&value);
        }
        Column::String(v) => {
            let value = unsafe { v.index_unchecked(row) };
            let len = value.len();

            row_space.store_value_uncheckd(&(len as u64));
            row_space.extend_from_slice_unchecked(value.as_bytes());
        }
        Column::Timestamp(v) => row_space.store_value_uncheckd(&v[row]),
        Column::Date(v) => row_space.store_value_uncheckd(&v[row]),
        Column::Array(array) | Column::Map(array) => {
            let data = array.index(row).unwrap();
            row_space.store_value_uncheckd(&(data.len() as u64));

            for i in 0..data.len() {
                serialize_column_binary(&data, i, row_space);
            }
        }
        Column::Nullable(c) => {
            let valid = c.validity.get_bit(row);

            row_space.push_unchecked(valid as u8);

            if valid {
                serialize_column_binary(&c.column, row, row_space);
            }
        }
        Column::Tuple(fields) => {
            for inner_col in fields.iter() {
                serialize_column_binary(inner_col, row, row_space);
            }
        }
    }
}
