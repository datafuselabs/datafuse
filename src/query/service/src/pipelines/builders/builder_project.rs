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

use common_exception::Result;
use common_expression::DataSchemaRef;
use common_expression::FunctionContext;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sinks::EmptySink;
use common_pipeline_transforms::processors::profile_wrapper::TransformProfileWrapper;
use common_pipeline_transforms::processors::transforms::Transformer;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::executor::Project;
use common_sql::executor::ProjectSet;
use common_sql::ColumnBinding;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_project(&mut self, project: &Project) -> Result<()> {
        self.build_pipeline(&project.input)?;
        let num_input_columns = project.input.output_schema()?.num_fields();
        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                input,
                output,
                num_input_columns,
                self.func_ctx.clone(),
                vec![BlockOperator::Project {
                    projection: project.projections.clone(),
                }],
            )))
        })
    }

    pub fn build_result_projection(
        func_ctx: &FunctionContext,
        input_schema: DataSchemaRef,
        result_columns: &[ColumnBinding],
        pipeline: &mut Pipeline,
        ignore_result: bool,
    ) -> Result<()> {
        if ignore_result {
            return pipeline.add_sink(|input| Ok(ProcessorPtr::create(EmptySink::create(input))));
        }

        let mut projections = Vec::with_capacity(result_columns.len());

        for column_binding in result_columns {
            let index = column_binding.index;
            projections.push(input_schema.index_of(index.to_string().as_str())?);
        }
        let num_input_columns = input_schema.num_fields();
        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                input,
                output,
                num_input_columns,
                func_ctx.clone(),
                vec![BlockOperator::Project {
                    projection: projections.clone(),
                }],
            )))
        })?;

        Ok(())
    }

    pub(crate) fn build_project_set(&mut self, project_set: &ProjectSet) -> Result<()> {
        self.build_pipeline(&project_set.input)?;

        let op = BlockOperator::FlatMap {
            projections: project_set.projections.clone(),
            srf_exprs: project_set
                .srf_exprs
                .iter()
                .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS))
                .collect(),
        };

        let num_input_columns = project_set.input.output_schema()?.num_fields();

        self.main_pipeline.add_transform(|input, output| {
            let transform = CompoundBlockOperator::new(
                vec![op.clone()],
                self.func_ctx.clone(),
                num_input_columns,
            );

            if self.enable_profiling {
                Ok(ProcessorPtr::create(TransformProfileWrapper::create(
                    transform,
                    input,
                    output,
                    project_set.plan_id,
                    self.proc_profs.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(Transformer::create(
                    input, output, transform,
                )))
            }
        })
    }
}
