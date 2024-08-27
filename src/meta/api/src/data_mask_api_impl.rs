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

use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::DatamaskAlreadyExists;
use databend_common_meta_app::data_mask::CreateDatamaskReply;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DataMaskIdIdent;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::DropDatamaskReply;
use databend_common_meta_app::data_mask::DropDatamaskReq;
use databend_common_meta_app::data_mask::GetDatamaskReply;
use databend_common_meta_app::data_mask::GetDatamaskReq;
use databend_common_meta_app::data_mask::MaskPolicyTableIdListIdent;
use databend_common_meta_app::data_mask::MaskpolicyTableIdList;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::data_mask_api::DatamaskApi;
use crate::fetch_id;
use crate::get_pb_value;
use crate::get_u64_value;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_eq_seq;
use crate::txn_op_del;
use crate::txn_op_put;

/// DatamaskApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls DatamaskApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> DatamaskApi for KV {
    async fn create_data_mask(
        &self,
        req: CreateDatamaskReq,
    ) -> Result<CreateDatamaskReply, KVAppError> {
        debug!(req :? =(&req); "DatamaskApi: {}", func_name!());

        let name_ident = &req.name;

        let mut trials = txn_backoff(None, func_name!());
        let id = loop {
            trials.next().unwrap()?.await;

            // Get db mask by name to ensure absence
            let (seq, id) = get_u64_value(self, name_ident).await?;
            debug!(seq = seq, id = id, name_key :? =(name_ident); "create_data_mask");

            let mut txn = TxnRequest::default();

            if seq > 0 {
                match req.create_option {
                    CreateOption::Create => {
                        return Err(KVAppError::AppError(AppError::DatamaskAlreadyExists(
                            DatamaskAlreadyExists::new(
                                name_ident.name(),
                                format!("create data mask: {}", req.name.display()),
                            ),
                        )));
                    }
                    CreateOption::CreateIfNotExists => return Ok(CreateDatamaskReply { id }),
                    CreateOption::CreateOrReplace => {
                        construct_drop_mask_policy_operations(
                            self,
                            name_ident,
                            false,
                            false,
                            func_name!(),
                            &mut txn,
                        )
                        .await?;
                    }
                };
            };

            // Create data mask by inserting these record:
            // name -> id
            // id -> policy
            // data mask name -> data mask table id list

            let id = fetch_id(self, IdGenerator::data_mask_id()).await?;

            let id_ident = DataMaskIdIdent::new(name_ident.tenant(), id);
            let id_list_key = MaskPolicyTableIdListIdent::new_from(name_ident.clone());

            debug!(
                id :? =(&id_ident),
                name_key :? =(name_ident);
                "new datamask id"
            );

            {
                let meta: DatamaskMeta = req.clone().into();
                let id_list = MaskpolicyTableIdList::default();
                txn.condition.push(txn_cond_eq_seq(name_ident, seq));
                txn.if_then.extend( vec![
                    txn_op_put(name_ident, serialize_u64(id)?), // name -> db_id
                    txn_op_put(&id_ident, serialize_struct(&meta)?), // id -> meta
                    txn_op_put(&id_list_key, serialize_struct(&id_list)?), /* data mask name -> id_list */
                ]);

                let (succ, _responses) = send_txn(self, txn).await?;

                debug!(
                    name :? =(name_ident),
                    id :? =(&id_ident),
                    succ = succ;
                    "create_data_mask"
                );

                if succ {
                    break id;
                }
            }
        };

        Ok(CreateDatamaskReply { id })
    }

    async fn drop_data_mask(&self, req: DropDatamaskReq) -> Result<DropDatamaskReply, KVAppError> {
        debug!(req :? =(&req); "DatamaskApi: {}", func_name!());

        let name_key = &req.name;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            construct_drop_mask_policy_operations(
                self,
                name_key,
                req.if_exists,
                true,
                func_name!(),
                &mut txn,
            )
            .await?;

            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                succ = succ;
                "drop_data_mask"
            );

            if succ {
                break;
            }
        }

        Ok(DropDatamaskReply {})
    }

    async fn get_data_mask(&self, req: GetDatamaskReq) -> Result<GetDatamaskReply, KVAppError> {
        debug!(req :? =(&req); "DatamaskApi: {}", func_name!());

        let name_key = &req.name;

        let (_seq_id, policy) = self
            .get_id_and_value(name_key)
            .await?
            .ok_or_else(|| AppError::from(name_key.unknown_error("get_data_mask")))?;

        Ok(GetDatamaskReply { policy })
    }
}

async fn clear_table_column_mask_policy(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_ident: &DataMaskNameIdent,
    txn: &mut TxnRequest,
) -> Result<(), KVAppError> {
    let id_list_key = MaskPolicyTableIdListIdent::new_from(name_ident.clone());

    let seq_id_list = kv_api.get_pb(&id_list_key).await?;

    let Some(seq_id_list) = seq_id_list else {
        return Ok(());
    };

    txn.condition
        .push(txn_cond_eq_seq(&id_list_key, seq_id_list.seq));
    txn.if_then.push(txn_op_del(&id_list_key));

    // remove mask policy from table meta
    for table_id in seq_id_list.data.id_list.into_iter() {
        let tbid = TableId { table_id };

        let (tb_meta_seq, table_meta_opt): (_, Option<TableMeta>) =
            get_pb_value(kv_api, &tbid).await?;
        if let Some(mut table_meta) = table_meta_opt {
            if let Some(column_mask_policy) = table_meta.column_mask_policy {
                let new_column_mask_policy = column_mask_policy
                    .into_iter()
                    .filter(|(_, name)| name != name_ident.name())
                    .collect();

                table_meta.column_mask_policy = Some(new_column_mask_policy);

                txn.condition.push(txn_cond_eq_seq(&tbid, tb_meta_seq));
                txn.if_then
                    .push(txn_op_put(&tbid, serialize_struct(&table_meta)?));
            }
        }
    }

    Ok(())
}

async fn construct_drop_mask_policy_operations(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &DataMaskNameIdent,
    drop_if_exists: bool,
    if_delete: bool,
    ctx: &str,
    txn: &mut TxnRequest,
) -> Result<(), KVAppError> {
    let res = kv_api.get_id_and_value(name_key).await?;

    let (seq_id, seq_meta) = match res {
        Some((seq_id, seq_meta)) => (seq_id, seq_meta),
        None => {
            return if drop_if_exists {
                Ok(())
            } else {
                let err = AppError::from(name_key.unknown_error("drop_data_mask"));
                Err(err.into())
            };
        }
    };

    let id_ident = seq_id.data.into_t_ident(name_key.tenant());

    txn.condition.push(txn_cond_eq_seq(&id_ident, seq_meta.seq));
    txn.if_then.push(txn_op_del(&id_ident));

    if if_delete {
        txn.condition.push(txn_cond_eq_seq(name_key, seq_id.seq));
        txn.if_then.push(txn_op_del(name_key));
        clear_table_column_mask_policy(kv_api, name_key, txn).await?;
    }

    debug!(
        name :? =(name_key),
        seq_id :? =seq_id,
        ctx = ctx;
        "construct_drop_mask_policy_operations"
    );

    Ok(())
}
