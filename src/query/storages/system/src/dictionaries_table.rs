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

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::database;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::GetDictionaryReply;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct DictionariesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for DictionariesTable {
    const NAME: &'static str = "system.dictionaries";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();

        let mut db_names = vec![];
        let mut names = vec![];
        let mut dict_ids = vec![];

        let mut key_names = vec![];
        let mut key_types = vec![];
        let mut attribute_names = vec![];
        let mut attribute_types = vec![];
        let mut sources = vec![];
        let mut comments = vec![];
        let mut created_ons = vec![];
        let mut updated_ons = vec![];

        let catalog = ctx.get_default_catalog()?;
        let databases = catalog.list_databases(&tenant).await?;
        for database in databases {
            let db_id = database.get_db_info().database_id;
            let req = ListDictionaryReq {
                tenant: tenant.clone(),
                db_id,
            };
            let dictionaries = catalog.list_dictionaries(req).await?;
            for (dict_name, dict_meta) in dictionaries {
                db_names.push(database.get_db_name().to_string());

                names.push(dict_name.clone());

                let dict_name_ident = DictionaryNameIdent::new(
                    tenant.clone(), 
                    DictionaryIdentity::new(
                        db_id,
                        dict_name.clone()
                ));
                let reply = catalog.get_dictionary(dict_name_ident).await?.unwrap();
                let dict_id = reply.dictionary_id;
                dict_ids.push(dict_id);

                let comment = dict_meta.comment;
                comments.push(comment);

                let created_on = dict_meta.created_on.timestamp();
                created_ons.push(created_on);
                let updated_on = dict_meta.updated_on.unwrap().timestamp();
                updated_ons.push(updated_on);
                
                let schema = dict_meta.schema;
                let fields = schema.fields;
                let primary_column_ids = dict_meta.primary_column_ids;
                let key_name = vec![];
                let key_type = vec![];
                let attribute_name = vec![];
                let attribute_type = vec![];
                for field in fields {
                    if primary_column_ids.contains(&field.column_id) {
                        key_name.push(field.name);
                        key_type.push(field.data_type);
                    } else {
                        attribute_name.push(field.name);
                        attribute_type.push(field.data_type);
                    }
                }
                key_names.push(key_name);
                key_types.push(key_type);
                attribute_names.push(attribute_name);
                attribute_types.push(attribute_type);

                let dict_source = dict_meta.source;
                let options = dict_meta.options;
                if let Some(password) = options.get_mut("password") {
                    *password = "[hidden]".to_string();
                } 
                let options_str: Vec<String> = options.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect();
                let options_joined = options_str.join(" ");
                let source = format!("{}({})", source, options_joined);
                sources.push(source);
            }
        }
        return Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(db_names),
            StringType::from_data(names),
            UInt64Type::from_data(dict_ids),
            // TODO
            StringType::from_data(key_names),
            StringType::from_data(key_types),
            StringType::from_data(attribute_names),
            StringType::from_data(attribute_types),
            StringType::from_data(sources),
            StringType::from_data(comments),
            TimestampType::from_data(created_ons),
            TimestampType::from_data(updated_ons),
        ]));
    }
}

impl DictionariesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("database", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new(
                "dictionary_id",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("key_names", TableDataType::Array(Box::new(TableDataType::String))),
            TableField::new("key_types", TableDataType::Array(Box::new(TableDataType))),
            TableField::new("attribute_names", TableDataType::String),
            TableField::new("attribute_types", TableDataType::String),
            TableField::new("source", TableDataType::String),
            TableField::new("comment", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new("updated_on", TableDataType::Timestamp),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'dictionaries'".to_string(),
            name: "dictionaries".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemDictionaries".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(DictionariesTable { table_info })
    }
}
