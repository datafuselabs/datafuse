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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UpdateTableMetaReq;

#[derive(Debug, Clone)]
pub struct TxnManager {
    state: TxnState,
    txn_buffer: TxnBuffer,
}

pub type TxnManagerRef = Arc<Mutex<TxnManager>>;

#[derive(Clone, Debug)]
pub enum TxnState {
    AutoCommit,
    Active,
    Fail,
}

#[derive(Debug, Clone)]
struct TxnBuffer {
    mutated_tables: HashMap<String, (UpdateTableMetaReq, TableInfo)>,
}

impl TxnBuffer {
    fn new() -> Self {
        Self {
            mutated_tables: HashMap::new(),
        }
    }
    fn refresh(&mut self) {
        self.mutated_tables.clear();
    }
}

impl TxnManager {
    pub fn init() -> Self {
        TxnManager {
            state: TxnState::AutoCommit,
            txn_buffer: TxnBuffer::new(),
        }
    }

    pub fn begin(&mut self) {
        if let TxnState::AutoCommit = self.state {
            self.state = TxnState::Active
        }
    }

    pub fn refresh(&mut self) {
        self.state = TxnState::AutoCommit;
        self.txn_buffer.refresh();
    }

    pub fn set_fail(&mut self) {
        if let TxnState::Active = self.state {
            self.state = TxnState::Fail;
        }
    }

    pub fn is_fail(&self) -> bool {
        matches!(self.state, TxnState::Fail)
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, TxnState::Active)
    }

    pub fn state(&self) -> TxnState {
        self.state.clone()
    }

    pub fn add_mutated_table(&mut self, req: UpdateTableMetaReq, table_info: &TableInfo) {
        self.txn_buffer
            .mutated_tables
            .insert(table_info.desc.clone(), (req, table_info.clone()));
    }

    pub fn get_mutated_table(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Option<TableInfo> {
        let tenant_dbname_tbname = TableNameIdent::new(tenant, db_name, table_name).to_string();
        self.txn_buffer
            .mutated_tables
            .get(&tenant_dbname_tbname)
            .map(|(req, table_info)| TableInfo {
                meta: req.new_table_meta.clone(),
                ..table_info.clone()
            })
    }

    pub fn reqs(&self) -> Vec<UpdateTableMetaReq> {
        self.txn_buffer
            .mutated_tables
            .values()
            .map(|(req, _)| req.clone())
            .collect()
    }
}
