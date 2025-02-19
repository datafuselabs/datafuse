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

use databend_common_exception::Result;
use poem::web::Path;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct ListDatabasesResponse {
    pub databases: Vec<DatabaseInfo>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct DatabaseInfo {
    pub name: String,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_databases_handler() -> Result<ListDatabasesResponse> {
    todo!()
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_tenant_databases_handler(
    Path(tenant): Path<String>,
) -> Result<ListDatabasesResponse> {
    todo!()
}
