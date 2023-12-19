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

use std::cell::SyncUnsafeCell;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::watch;
use databend_common_base::base::tokio::sync::watch::Receiver;
use databend_common_base::base::tokio::sync::watch::Sender;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::{Column, DataBlock, DataSchemaRef, Evaluator, RawExpr, type_check};
use databend_common_expression::HashMethodFixedKeys;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleString;
use databend_common_hashtable::HashJoinHashMap;
use databend_common_hashtable::HashtableKeyable;
use databend_common_hashtable::StringHashJoinHashMap;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use ethnum::U256;
use parking_lot::RwLock;
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::transforms::hash_join::build_state::BuildState;
use crate::pipelines::processors::transforms::hash_join::hash_join_build_state::{BLOOM_RUNTIME_FILTER_THRESHOLD, INLIST_RUNTIME_FILTER_THRESHOLD};
use crate::pipelines::processors::transforms::hash_join::row::RowSpace;
use crate::pipelines::processors::transforms::hash_join::util::{bloom_filter, build_schema_wrap_nullable, dedup_build_key_column};
use crate::pipelines::processors::transforms::hash_join::util::inlist_filter;
use crate::pipelines::processors::HashJoinDesc;
use crate::sessions::QueryContext;

pub struct SerializerHashJoinHashTable {
    pub(crate) hash_table: StringHashJoinHashMap,
    pub(crate) hash_method: HashMethodSerializer,
}

pub struct SingleStringHashJoinHashTable {
    pub(crate) hash_table: StringHashJoinHashMap,
    pub(crate) hash_method: HashMethodSingleString,
}

pub struct FixedKeyHashJoinHashTable<T: HashtableKeyable> {
    pub(crate) hash_table: HashJoinHashMap<T>,
    pub(crate) hash_method: HashMethodFixedKeys<T>,
}

pub enum HashJoinHashTable {
    Null,
    Serializer(SerializerHashJoinHashTable),
    SingleString(SingleStringHashJoinHashTable),
    KeysU8(FixedKeyHashJoinHashTable<u8>),
    KeysU16(FixedKeyHashJoinHashTable<u16>),
    KeysU32(FixedKeyHashJoinHashTable<u32>),
    KeysU64(FixedKeyHashJoinHashTable<u64>),
    KeysU128(FixedKeyHashJoinHashTable<u128>),
    KeysU256(FixedKeyHashJoinHashTable<U256>),
}

/// Define some shared states for hash join build and probe.
/// It will like a bridge to connect build and probe.
/// Such as build side will pass hash table to probe side by it
pub struct HashJoinState {
    pub(crate) ctx: Arc<QueryContext>,
    /// A shared big hash table stores all the rows from build side
    pub(crate) hash_table: SyncUnsafeCell<HashJoinHashTable>,
    /// It will be increased by 1 when a new hash join build processor is created.
    /// After the processor added its chunks to `HashTable`, it will be decreased by 1.
    /// When the counter is 0, it means all hash join build processors have added their chunks to `HashTable`.
    /// And the build phase is finished. Probe phase will start.
    pub(crate) hash_table_builders: AtomicUsize,
    /// After `hash_table_builders` is 0, send message to notify all probe processors.
    /// There are three types' messages:
    /// 1. **0**: it's the initial message used by creating the watch channel
    /// 2. **1**: when build side finish (the first round), the last build processor will send 1 to channel, and wake up all probe processors.
    /// 3. **2**: if spill is enabled, after the first round, probe needs to wait build again, the last build processor will send 2 to channel.
    pub(crate) build_done_watcher: Sender<u8>,
    /// A dummy receiver to make build done watcher channel open
    pub(crate) _build_done_dummy_receiver: Receiver<u8>,
    /// Some description of hash join. Such as join type, join keys, etc.
    pub(crate) hash_join_desc: HashJoinDesc,
    /// Interrupt the build phase or probe phase.
    pub(crate) interrupt: AtomicBool,
    /// If there is no data in build side, maybe we can fast return.
    pub(crate) fast_return: AtomicBool,
    /// Use the column of probe side to construct build side column.
    /// (probe index, (is probe column nullable, is build column nullable))
    pub(crate) probe_to_build: Vec<(usize, (bool, bool))>,
    /// `RowSpace` contains all rows from build side.
    pub(crate) row_space: RowSpace,
    /// `BuildState` contains all data used in probe phase.
    pub(crate) build_state: SyncUnsafeCell<BuildState>,

    /// Spill related states
    /// Spill partition set
    pub(crate) build_spilled_partitions: RwLock<HashSet<u8>>,
    /// Send message to notify all build processors to next round.
    /// Initial message is false, send true to wake up all build processors.
    pub(crate) continue_build_watcher: Sender<bool>,
    /// A dummy receiver to make continue build watcher channel open
    pub(crate) _continue_build_dummy_receiver: Receiver<bool>,
    /// After all build processors finish spill, will pick a partition
    /// tell build processors to restore data in the partition
    /// If partition_id is -1, it means all partitions are spilled.
    pub(crate) partition_id: AtomicI8,
    pub(crate) enable_spill: bool,

    /// If the join node generate runtime filters, the scan node will use it to do prune.
    pub(crate) table_index: IndexType,
}

impl HashJoinState {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        mut build_schema: DataSchemaRef,
        build_projections: &ColumnSet,
        hash_join_desc: HashJoinDesc,
        probe_to_build: &[(usize, (bool, bool))],
        table_index: IndexType,
    ) -> Result<Arc<HashJoinState>> {
        if matches!(
            hash_join_desc.join_type,
            JoinType::Left | JoinType::LeftSingle
        ) {
            build_schema = build_schema_wrap_nullable(&build_schema);
        };
        if hash_join_desc.join_type == JoinType::Full {
            build_schema = build_schema_wrap_nullable(&build_schema);
        }
        let (build_done_watcher, _build_done_dummy_receiver) = watch::channel(0);
        let (continue_build_watcher, _continue_build_dummy_receiver) = watch::channel(false);
        let mut enable_spill = false;
        if hash_join_desc.join_type == JoinType::Inner
            && ctx.get_settings().get_join_spilling_threshold()? != 0
        {
            enable_spill = true;
        }
        Ok(Arc::new(HashJoinState {
            ctx: ctx.clone(),
            hash_table: SyncUnsafeCell::new(HashJoinHashTable::Null),
            hash_table_builders: AtomicUsize::new(0),
            build_done_watcher,
            _build_done_dummy_receiver,
            hash_join_desc,
            interrupt: AtomicBool::new(false),
            fast_return: Default::default(),
            probe_to_build: probe_to_build.to_vec(),
            row_space: RowSpace::new(ctx, build_schema, build_projections)?,
            build_state: SyncUnsafeCell::new(BuildState::new()),
            build_spilled_partitions: Default::default(),
            continue_build_watcher,
            _continue_build_dummy_receiver,
            partition_id: AtomicI8::new(-2),
            enable_spill,
            table_index,
        }))
    }

    pub fn interrupt(&self) {
        self.interrupt.store(true, Ordering::Release);
    }

    /// Used by hash join probe processors, wait for the first round build phase finished.
    #[async_backtrace::framed]
    pub async fn wait_first_round_build_done(&self) -> Result<()> {
        let mut rx = self.build_done_watcher.subscribe();
        if *rx.borrow() == 1_u8 {
            return Ok(());
        }
        rx.changed()
            .await
            .map_err(|_| ErrorCode::TokioError("build_done_watcher's sender is dropped"))?;
        debug_assert!(*rx.borrow() == 1_u8);
        Ok(())
    }

    /// Used by hash join probe processors, wait for build phase finished with spilled data
    /// It's only be used when spilling is enabled.
    #[async_backtrace::framed]
    pub async fn wait_build_finish(&self) -> Result<()> {
        let mut rx = self.build_done_watcher.subscribe();
        if *rx.borrow() == 2_u8 {
            return Ok(());
        }
        rx.changed()
            .await
            .map_err(|_| ErrorCode::TokioError("build_done_watcher's sender is dropped"))?;
        debug_assert!(*rx.borrow() == 2_u8);
        Ok(())
    }

    pub fn need_outer_scan(&self) -> bool {
        matches!(
            self.hash_join_desc.join_type,
            JoinType::Full
                | JoinType::Right
                | JoinType::RightSingle
                | JoinType::RightSemi
                | JoinType::RightAnti
        )
    }

    pub fn need_mark_scan(&self) -> bool {
        matches!(self.hash_join_desc.join_type, JoinType::LeftMark)
    }

    pub fn set_spilled_partition(&self, partitions: &HashSet<u8>) {
        let mut spill_partition = self.build_spilled_partitions.write();
        spill_partition.extend(partitions);
    }

    #[async_backtrace::framed]
    pub(crate) async fn wait_probe_notify(&self) -> Result<()> {
        let mut rx = self.continue_build_watcher.subscribe();
        if *rx.borrow() {
            return Ok(());
        }
        rx.changed()
            .await
            .map_err(|_| ErrorCode::TokioError("continue_build_watcher's sender is dropped"))?;
        debug_assert!(*rx.borrow());
        Ok(())
    }

    // Reset the state for next round run.
    // It only be called when spill is enable.
    pub(crate) fn reset(&self) {
        self.row_space.reset();
        let build_state = unsafe { &mut *self.build_state.get() };
        build_state.generation_state.chunks.clear();
        build_state.generation_state.build_num_rows = 0;
        build_state.generation_state.build_columns.clear();
        build_state.generation_state.build_columns_data_type.clear();
        if self.need_outer_scan() {
            build_state.outer_scan_map.clear();
        }
        if self.need_mark_scan() {
            build_state.mark_scan_map.clear();
        }
        build_state.generation_state.is_build_projected = true;
    }

    // Generate runtime filters
    pub(crate) fn generate_runtime_filters(&self) -> Result<()> {
        // If build side rows < 10k, using inlist filter else using bloom filter
        let func_ctx = self.ctx.get_function_context()?;
        let build_state = unsafe { &mut *self.build_state.get() };
        let data_blocks = &mut build_state.build_chunks;

        let num_rows = build_state.generation_state.build_num_rows;
        if num_rows > BLOOM_RUNTIME_FILTER_THRESHOLD {
            data_blocks.clear();
            return Ok(());
        }
        let mut runtime_filter = RuntimeFilterInfo::new();
        for (build_key, probe_key) in self
            .hash_join_desc
            .build_keys
            .iter()
            .zip(self.hash_join_desc.probe_keys_rt.iter())
        {
            if let Some(distinct_build_column) = dedup_build_key_column(&func_ctx, data_blocks, build_key)? {
                if num_rows <= INLIST_RUNTIME_FILTER_THRESHOLD {
                    if let Some(filter) = inlist_filter(&probe_key, distinct_build_column.clone())? {
                        runtime_filter.add_inlist(filter);
                    }
                }
                if num_rows <= BLOOM_RUNTIME_FILTER_THRESHOLD {
                    if let Some(filter) = bloom_filter(build_key, probe_key, distinct_build_column, num_rows)? {
                        runtime_filter.add_bloom(Box::new(filter));
                    }
                }
            }
        }
        if !runtime_filter.is_empty() {
            self.ctx
                .set_runtime_filter((self.table_index, runtime_filter));
        }
        data_blocks.clear();
        Ok(())
    }
}
