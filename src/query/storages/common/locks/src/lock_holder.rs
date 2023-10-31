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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::tokio::time::sleep;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_catalog::catalog::Catalog;
use common_catalog::lock_api::LockApi;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::select;
use futures::future::Either;
use rand::thread_rng;
use rand::Rng;

pub struct LockHolder {
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    shutdown_handler: Option<JoinHandle<Result<()>>>,
}

impl LockHolder {
    pub fn create() -> LockHolder {
        LockHolder {
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            shutdown_handler: None,
        }
    }

    #[async_backtrace::framed]
    pub async fn start<T: LockApi + ?Sized>(
        &mut self,
        catalog: Arc<dyn Catalog>,
        lock: &T,
        revision: u64,
    ) -> Result<()> {
        let expire_secs = lock.get_expire_secs();
        let sleep_range = (expire_secs * 1000 / 3)..=((expire_secs * 1000 / 3) * 2);
        let delete_table_lock_req = lock.delete_table_lock_req(revision);
        let extend_table_lock_req = lock.extend_table_lock_req(revision, false);

        self.shutdown_handler = Some(GlobalIORuntime::instance().spawn({
            let shutdown_flag = self.shutdown_flag.clone();
            let shutdown_notify = self.shutdown_notify.clone();

            async move {
                let mut notified = Box::pin(shutdown_notify.notified());
                while !shutdown_flag.load(Ordering::Relaxed) {
                    let mills = {
                        let mut rng = thread_rng();
                        rng.gen_range(sleep_range.clone())
                    };
                    let sleep = Box::pin(sleep(Duration::from_millis(mills)));
                    match select(notified, sleep).await {
                        Either::Left((_, _)) => {
                            catalog.delete_lock_revision(delete_table_lock_req).await?;
                            break;
                        }
                        Either::Right((_, new_notified)) => {
                            notified = new_notified;
                            catalog
                                .extend_lock_revision(extend_table_lock_req.clone())
                                .await?;
                        }
                    }
                }
                Ok(())
            }
        }));
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown_handler) = self.shutdown_handler.take() {
            self.shutdown_flag.store(true, Ordering::Relaxed);
            self.shutdown_notify.notify_waiters();
            if let Err(shutdown_failure) = shutdown_handler.await {
                return Err(ErrorCode::TokioError(format!(
                    "Cannot shutdown table lock heartbeat, cause {:?}",
                    shutdown_failure
                )));
            }
        }
        Ok(())
    }
}
