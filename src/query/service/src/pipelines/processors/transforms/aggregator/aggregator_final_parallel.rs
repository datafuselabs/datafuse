// Copyright 2022 Datafuse Labs.
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

use std::borrow::BorrowMut;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use common_base::runtime::ThreadPool;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::HashMethod;
use common_functions::aggregates::StateAddr;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use tracing::info;

use super::estimated_key_size;
use crate::pipelines::processors::transforms::aggregator::aggregate_info::AggregateInfo;
use crate::pipelines::processors::transforms::group_by::Area;
use crate::pipelines::processors::transforms::group_by::GroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::processors::AggregatorParams;
use crate::sessions::QueryContext;

pub struct ParallelFinalAggregator<const HAS_AGG: bool, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    method: Method,
    query_ctx: Arc<QueryContext>,
    params: Arc<AggregatorParams>,
    buckets_blocks: HashMap<isize, Vec<DataBlock>>,
    generated: bool,
}

impl<Method, const HAS_AGG: bool> ParallelFinalAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    pub fn create(
        ctx: Arc<QueryContext>,
        method: Method,
        params: Arc<AggregatorParams>,
    ) -> Result<Self> {
        Ok(Self {
            params,
            method,
            query_ctx: ctx,
            buckets_blocks: HashMap::new(),
            generated: false,
        })
    }
}

impl<Method, const HAS_AGG: bool> Aggregator for ParallelFinalAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    const NAME: &'static str = "GroupByFinalTransform";

    fn consume(&mut self, block: DataBlock) -> Result<bool> {
        let mut bucket = -1;
        if let Some(meta_info) = block.get_meta() {
            if let Some(meta_info) = meta_info.as_any().downcast_ref::<AggregateInfo>() {
                bucket = meta_info.bucket;
            }
        }

        match self.buckets_blocks.entry(bucket) {
            Entry::Vacant(v) => {
                v.insert(vec![block]);
            }
            Entry::Occupied(mut v) => {
                v.get_mut().push(block);
            }
        }
        Ok(false)
    }

    fn generate(&mut self) -> Result<Vec<DataBlock>> {
        if self.generated {
            return Ok(vec![]);
        }

        let mut generate_blocks = Vec::new();
        let settings = self.query_ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;

        if max_threads <= 1
            || self.buckets_blocks.len() == 1
            || self.buckets_blocks.contains_key(&-1)
        {
            let mut data_blocks = vec![];
            for (_, bucket_blocks) in std::mem::take(&mut self.buckets_blocks) {
                data_blocks.extend(bucket_blocks);
            }

            let method = self.method.clone();
            let params = self.params.clone();
            let mut bucket_aggregator = BucketAggregator::<HAS_AGG, _>::create(method, params)?;
            generate_blocks = bucket_aggregator.merge_blocks(data_blocks)?;
        } else if self.buckets_blocks.len() > 1 {
            info!("Merge to final state using a parallel algorithm.");

            let thread_pool = ThreadPool::create(max_threads)?;
            let mut join_handles = Vec::with_capacity(self.buckets_blocks.len());

            for (_, bucket_blocks) in std::mem::take(&mut self.buckets_blocks) {
                let method = self.method.clone();
                let params = self.params.clone();
                let mut bucket_aggregator = BucketAggregator::<HAS_AGG, _>::create(method, params)?;
                join_handles.push(
                    thread_pool.execute(move || bucket_aggregator.merge_blocks(bucket_blocks)),
                );
            }

            generate_blocks.reserve(join_handles.len());
            for join_handle in join_handles {
                generate_blocks.extend(join_handle.join()?);
            }
        }
        self.generated = true;
        Ok(generate_blocks)
    }
}

pub struct BucketAggregator<const HAS_AGG: bool, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    pub(crate) area: Area,
    pub(crate) method: Method,
    pub(crate) params: Arc<AggregatorParams>,

    pub(crate) hash_table: Method::HashTable,
    pub(crate) reach_limit: bool,
    // used for deserialization only, so we can reuse it during the loop
    pub(crate) temp_place: Option<StateAddr>,
    pub(crate) generated: bool,
    pub(crate) states_dropped: bool,
}

impl<const HAS_AGG: bool, Method> Aggregator for BucketAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    const NAME: &'static str = "BucketAggregator";

    // return true if we should finish the consume opeartion
    // only returns true if there is no HAS_AGG
    fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        let block = data_block.convert_to_full();
        // 1.1 and 1.2.
        let aggregate_function_len = self.params.aggregate_functions.len();
        let keys_column = block
            .get_by_offset(aggregate_function_len)
            .value
            .as_column()
            .unwrap();
        let keys_iter = self.method.keys_iter_from_column(keys_column)?;

        if !HAS_AGG {
            unsafe {
                for key in keys_iter.iter() {
                    let _ = self.hash_table.insert_and_entry(key);
                }

                if let Some(limit) = self.params.limit {
                    if self.hash_table.len() >= limit {
                        return Ok(true);
                    }
                }
            }
        } else {
            // first state places of current block
            let places = self.lookup_state(&keys_iter);

            let states_columns = (0..aggregate_function_len)
                .map(|i| block.get_by_offset(i))
                .collect::<Vec<_>>();
            let mut states_binary_columns = Vec::with_capacity(states_columns.len());

            for agg in states_columns.iter().take(aggregate_function_len) {
                let aggr_column = agg.value.as_column().unwrap().as_string().ok_or_else(|| {
                    ErrorCode::IllegalDataType(format!(
                        "Aggregation column should be StringType, but got {:?}",
                        agg.value
                    ))
                })?;
                states_binary_columns.push(aggr_column);
            }

            let aggregate_functions = &self.params.aggregate_functions;
            let offsets_aggregate_states = &self.params.offsets_aggregate_states;
            if let Some(temp_place) = self.temp_place {
                for (row, place) in places.iter() {
                    for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                        let final_place = place.next(offsets_aggregate_states[idx]);
                        let state_place = temp_place.next(offsets_aggregate_states[idx]);

                        let mut data = unsafe { states_binary_columns[idx].index_unchecked(*row) };
                        aggregate_function.deserialize(state_place, &mut data)?;
                        aggregate_function.merge(final_place, state_place)?;
                    }
                }
            }
        }
        Ok(false)
    }

    fn generate(&mut self) -> Result<Vec<DataBlock>> {
        if self.generated {
            return Ok(vec![]);
        }
        self.generated = true;
        Self::generate_data(&self.hash_table, &self.params, &self.method)
    }
}

impl<const HAS_AGG: bool, Method> BucketAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    pub fn create(method: Method, params: Arc<AggregatorParams>) -> Result<Self> {
        let mut area = Area::create();
        let hash_table = method.create_hash_table()?;
        let temp_place = match params.aggregate_functions.is_empty() {
            true => None,
            false => Some(params.alloc_layout(&mut area)),
        };

        Ok(Self {
            area,
            method,
            params,
            hash_table,
            reach_limit: false,
            temp_place,
            generated: false,
            states_dropped: false,
        })
    }

    pub fn merge_blocks(&mut self, blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        if blocks.is_empty() {
            return Ok(vec![]);
        }
        for data_block in blocks {
            if self.consume(data_block)? {
                break;
            }
        }
        self.generate()
    }

    pub(crate) fn generate_data(
        hash_table: &Method::HashTable,
        params: &AggregatorParams,
        method: &Method,
    ) -> Result<Vec<DataBlock>> {
        let value_size = estimated_key_size(hash_table);
        let mut group_columns_builder =
            method
                .group_columns_builder(hash_table.len(), value_size, params);

        if !HAS_AGG {
            for group_entity in hash_table.iter() {
                group_columns_builder.append_value(group_entity.key());
            }

            let columns = group_columns_builder.finish()?;

            Ok(vec![DataBlock::new_from_columns(columns)])
        } else {
            let aggregate_functions = &params.aggregate_functions;
            let offsets_aggregate_states = &params.offsets_aggregate_states;

            let mut aggregates_column_builder = {
                let mut values = vec![];
                for aggregate_function in aggregate_functions {
                    let data_type = aggregate_function.return_type()?;
                    let builder = ColumnBuilder::with_capacity(&data_type, hash_table.len());
                    values.push(builder)
                }
                values
            };

            for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                let places = hash_table
                    .iter()
                    .map(|group_entity| {
                        let place = Into::<StateAddr>::into(*group_entity.get());
                        place.next(offsets_aggregate_states[idx])
                    })
                    .collect();

                let builder = aggregates_column_builder[idx].borrow_mut();
                aggregate_function.batch_merge_result(places, builder)?;
            }

            for group_entity in hash_table.iter() {
                group_columns_builder.append_value(group_entity.key());
            }

            // Build final state block.
            let mut columns = aggregates_column_builder
                .into_iter()
                .map(|builder| builder.build())
                .collect::<Vec<_>>();

            let group_columns = group_columns_builder.finish()?;
            columns.extend_from_slice(&group_columns);
            Ok(vec![DataBlock::new_from_columns(columns)])
        }
    }

    /// Allocate aggregation function state for each key(the same key can always get the same state)
    #[inline(always)]
    fn lookup_state(&mut self, keys_iter: &Method::KeysColumnIter) -> Vec<(usize, StateAddr)> {
        let iter = keys_iter.iter();
        let (len, _) = iter.size_hint();
        let mut places = Vec::with_capacity(len);

        let mut current_len = self.hash_table.len();
        unsafe {
            for (row, key) in iter.enumerate() {
                if self.reach_limit {
                    let entry = self.hash_table.entry(key);
                    if let Some(entry) = entry {
                        let place = Into::<StateAddr>::into(*entry.get());
                        places.push((row, place));
                    }
                    continue;
                }

                match self.hash_table.insert_and_entry(key) {
                    Ok(mut entry) => {
                        let place = self.params.alloc_layout(&mut self.area);
                        places.push((row, place));

                        *entry.get_mut() = place.addr();

                        if let Some(limit) = self.params.limit {
                            current_len += 1;
                            if current_len >= limit {
                                self.reach_limit = true;
                            }
                        }
                    }
                    Err(entry) => {
                        let place = Into::<StateAddr>::into(*entry.get());
                        places.push((row, place));
                    }
                }
            }
        }

        places
    }

    pub(crate) fn clear_table<T: HashtableLike<Value = usize>>(
        table: &mut T,
        aggregator_params: &AggregatorParams,
    ) {
        if table.len() == 0 {
            return;
        }

        let aggregate_functions = &aggregator_params.aggregate_functions;
        let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

        let functions = aggregate_functions
            .iter()
            .filter(|p| p.need_manual_drop_state())
            .collect::<Vec<_>>();

        let state_offsets = offsets_aggregate_states
            .iter()
            .enumerate()
            .filter(|(idx, _)| aggregate_functions[*idx].need_manual_drop_state())
            .map(|(_, s)| *s)
            .collect::<Vec<_>>();

        if !state_offsets.is_empty() {
            for group_entity in table.iter() {
                let place = Into::<StateAddr>::into(*group_entity.get());

                for (function, state_offset) in functions.iter().zip(state_offsets.iter()) {
                    unsafe { function.drop_state(place.next(*state_offset)) }
                }
            }
        }
        table.clear();
    }
}

impl<const HAS_AGG: bool, Method> Drop for BucketAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    fn drop(&mut self) {
        if self.states_dropped {
            return;
        }
        Self::clear_table(&mut self.hash_table, &self.params);
        if let Some(temp_place) = self.temp_place {
            let offsets_aggregate_states = &self.params.offsets_aggregate_states;
            let aggregate_functions = &self.params.aggregate_functions;
            
            let functions = aggregate_functions
                .iter()
                .filter(|p| p.need_manual_drop_state())
                .collect::<Vec<_>>();
            let state_offsets = offsets_aggregate_states
                .iter()
                .enumerate()
                .filter(|(idx, _)| aggregate_functions[*idx].need_manual_drop_state())
                .map(|(_, s)| *s)
                .collect::<Vec<_>>();

            for (state_offset, function) in state_offsets.iter().zip(functions.iter()) {
                let place = temp_place.next(*state_offset);
                unsafe { function.drop_state(place) }
            }
        }
        self.states_dropped = true;
    }
}
