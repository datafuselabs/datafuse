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

use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CatalogOption;
use common_meta_app::schema::CreateCatalogReq;
use common_meta_app::schema::DropCatalogReq;
use common_meta_app::schema::IcebergCatalogOption;
use common_meta_store::MetaStore;
use common_meta_store::MetaStoreProvider;
use common_storage::DataOperator;
use dashmap::DashMap;

use super::Catalog;

pub struct CatalogManager {
    pub meta: MetaStore,
    pub default_catalog: Arc<dyn Catalog>,
}

impl CatalogManager {
    pub fn instance() -> Arc<CatalogManager> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    async fn init(conf: &InnerConfig) -> Result<()> {
        GlobalInstance::set(Self::try_create(conf).await?);

        Ok(())
    }

    pub fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        self.catalogs
            .get(catalog_name)
            .as_deref()
            .cloned()
            .ok_or_else(|| ErrorCode::BadArguments(format!("no such catalog {}", catalog_name)))
    }

    #[async_backtrace::framed]
    async fn try_create(conf: &InnerConfig) -> Result<Arc<CatalogManager>> {
        // Initiate meta store
        let meta = {
            let provider = Arc::new(MetaStoreProvider::new(conf.meta.to_meta_grpc_client_conf()));

            provider.create_meta_store().await?
        };

        // Add built-in catalog: default
        let default_catalog: Arc<dyn Catalog> =
            Arc::new(DatabaseCatalog::try_create_with_config(conf.clone()).await?);
        catalog_manager
            .catalogs
            .insert(CATALOG_DEFAULT.to_owned(), default_catalog);

        let catalog_manager = CatalogManager {
            default_catalog,
            meta,
        };

        Ok(Arc::new(catalog_manager))
    }

    #[async_backtrace::framed]
    async fn create_user_defined_catalog(&self, req: CreateCatalogReq) -> Result<()> {
        let catalog_option = req.meta.catalog_option;

        // create catalog first
        match catalog_option {
            // NOTE:
            // when compiling without `hive` feature enabled
            // `address` will be seem as unused, which is not intentional
            #[allow(unused)]
            CatalogOption::Hive(cfg) => {
                #[cfg(not(feature = "hive"))]
                {
                    Err(ErrorCode::CatalogNotSupported(
                        "Hive catalog is not enabled, please recompile with --features hive",
                    ))
                }
                #[cfg(feature = "hive")]
                {
                    let catalog: Arc<dyn Catalog> = Arc::new(HiveCatalog::try_create(cfg.address)?);
                    let ctl_name = &req.name_ident.catalog_name;
                    let if_not_exists = req.if_not_exists;

                    self.insert_catalog(ctl_name, catalog, if_not_exists)
                }
            }
            CatalogOption::Iceberg(opt) => {
                let IcebergCatalogOption { storage_params: sp } = opt;

                let data_operator = DataOperator::try_create(&sp).await?;
                let ctl_name = &req.name_ident.catalog_name;
                let catalog: Arc<dyn Catalog> =
                    Arc::new(IcebergCatalog::try_create(ctl_name, data_operator)?);

                let if_not_exists = req.if_not_exists;
                self.insert_catalog(ctl_name, catalog, if_not_exists)
            }
        }
    }

    #[async_backtrace::framed]
    async fn drop_user_defined_catalog(&self, req: DropCatalogReq) -> Result<()> {
        let name = req.name_ident.catalog_name;
        if name == CATALOG_DEFAULT {
            return Err(ErrorCode::CatalogNotSupported(
                "Dropping the DEFAULT catalog is not allowed",
            ));
        }

        match self.catalogs.remove(&name) {
            Some(_) => Ok(()),

            None if req.if_exists => Ok(()),

            None => Err(ErrorCode::CatalogNotFound(format!(
                "Catalog {} has to be exists",
                name
            ))),
        }
    }
}
