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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Expr;
use common_expression::Scalar;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::parse_exprs;
use common_storages_factory::Table;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::transform::Transform;
use crate::pipelines::processors::transforms::transform::Transformer;
use crate::sessions::QueryContext;

pub struct TransformResortAddOnWithoutSourceSchema {
    output_schema: DataSchemaRef,
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
}

impl TransformResortAddOnWithoutSourceSchema {
    fn build_expression_transform(
        &self,
        input_schema: DataSchemaRef,
    ) -> Result<CompoundBlockOperator> {
        let mut exprs = Vec::with_capacity(self.output_schema.fields().len());
        for f in self.output_schema.fields().iter() {
            let expr = if !input_schema.has_field(f.name()) {
                if let Some(default_expr) = f.default_expr() {
                    let mut expr = parse_exprs(self.ctx.clone(), self.table.clone(), default_expr)?;
                    let mut expr = expr.remove(0);
                    if expr.data_type() != f.data_type() {
                        expr = Expr::Cast {
                            span: None,
                            is_try: f.data_type().is_nullable(),
                            expr: Box::new(expr),
                            dest_type: f.data_type().clone(),
                        };
                    }
                    expr
                } else {
                    let default_value = Scalar::default_value(f.data_type());
                    Expr::Constant {
                        span: None,
                        scalar: default_value,
                        data_type: f.data_type().clone(),
                    }
                }
            } else {
                let field = input_schema.field_with_name(f.name()).unwrap();
                let id = input_schema.index_of(f.name()).unwrap();
                Expr::ColumnRef {
                    span: None,
                    id,
                    data_type: field.data_type().clone(),
                    display_name: field.name().clone(),
                }
            };
            exprs.push(expr);
        }

        let func_ctx = self.ctx.get_function_context()?;
        Ok(CompoundBlockOperator {
            ctx: func_ctx,
            operators: vec![BlockOperator::Map {
                exprs,
                projections: None,
            }],
        })
    }
}

impl TransformResortAddOnWithoutSourceSchema
where Self: Transform
{
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        table: Arc<dyn Table>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            Self {
                output_schema,
                ctx,
                table,
            },
        )))
    }
}

impl Transform for TransformResortAddOnWithoutSourceSchema {
    const NAME: &'static str = "AddOnWithoutSourceSchemaTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        todo!();
        // block = self
        //     .build_expression_transform(input_schema)?
        //     .transform(block)?;
        // let columns = block.columns()[self.input_schema..].to_owned();
        // Ok(DataBlock::new(columns, block.num_rows()))
    }
}
