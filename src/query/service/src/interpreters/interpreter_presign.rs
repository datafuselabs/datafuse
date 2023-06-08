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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Scalar;
use common_expression::Value;
use common_storages_stage::StageTable;
use jsonb::Value as JsonbValue;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::PresignAction;
use crate::sql::plans::PresignPlan;

pub struct PresignInterpreter {
    ctx: Arc<dyn TableContext>,
    plan: PresignPlan,
}

impl PresignInterpreter {
    /// Create a PresignInterpreter with context and [`PresignPlan`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: PresignPlan) -> Result<Self> {
        Ok(PresignInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for PresignInterpreter {
    fn name(&self) -> &str {
        "PresignInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[tracing::instrument(level = "debug", name = "presign_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let op = StageTable::get_op(&self.plan.stage)?;
        if !op.info().can_presign() {
            return Err(ErrorCode::StorageUnsupported(
                "storage doesn't support presign operation",
            ));
        }

        let presigned_req = match self.plan.action {
            PresignAction::Download => op.presign_read(&self.plan.path, self.plan.expire).await?,
            PresignAction::Upload => {
                let mut fut = op.presign_write_with(&self.plan.path, self.plan.expire);
                if let Some(content_type) = &self.plan.content_type {
                    fut = fut.content_type(content_type);
                }
                fut.await?
            }
        };

        let header = JsonbValue::Object(
            presigned_req
                .header()
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        JsonbValue::String(
                            v.to_str()
                                .expect("header value generated by opendal must be valid")
                                .to_string()
                                .into(),
                        ),
                    )
                })
                .collect(),
        );

        let block = DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(
                        presigned_req.method().as_str().as_bytes().to_vec(),
                    )),
                ),
                BlockEntry::new(
                    DataType::Variant,
                    Value::Scalar(Scalar::Variant(header.to_vec())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(
                        presigned_req.uri().to_string().as_bytes().to_vec(),
                    )),
                ),
            ],
            1,
        );

        PipelineBuildResult::from_blocks(vec![block])
    }
}
