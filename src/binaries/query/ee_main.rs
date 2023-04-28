// Copyright 2023 Databend Cloud, Inc.
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::uninlined_format_args)]
#![feature(try_blocks)]

mod entry;
mod local;

use common_base::mem_allocator::GlobalAllocator;
use common_base::runtime::Runtime;
use common_config::InnerConfig;
use common_exception::Result;
use enterprise_query::enterprise_services::EnterpriseServices;

use crate::entry::init_services;
use crate::entry::start_services;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: GlobalAllocator = GlobalAllocator;

fn main() {
    match Runtime::with_default_worker_threads() {
        Err(cause) => {
            eprintln!("Databend Query start failure, cause: {:?}", cause);
            std::process::exit(cause.code() as i32);
        }
        Ok(rt) => {
            if let Err(cause) = rt.block_on(async_backtrace::location!().frame(main_entrypoint())) {
                eprintln!("Databend Query start failure, cause: {:?}", cause);
                std::process::exit(cause.code() as i32);
            }
        }
    }
}

pub async fn main_entrypoint() -> Result<()> {
    let conf: InnerConfig = InnerConfig::load()?;
    init_services(&conf).await?;
    EnterpriseServices::init(conf.clone()).await?;
    start_services(&conf).await
}
