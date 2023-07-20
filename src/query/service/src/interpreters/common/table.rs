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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ComputedExpr;
use common_expression::DataSchemaRef;
use common_sql::parse_computed_expr;

pub fn check_referenced_computed_columns(
    ctx: Arc<dyn TableContext>,
    schema: DataSchemaRef,
    column: &str,
) -> Result<()> {
    for f in schema.fields() {
        if let Some(computed_expr) = f.computed_expr() {
            let expr = match computed_expr {
                ComputedExpr::Stored(ref expr) => expr,
                ComputedExpr::Virtual(ref expr) => expr,
            };
            if parse_computed_expr(ctx.clone(), schema.clone(), expr).is_err() {
                return Err(ErrorCode::ColumnReferencedByComputedColumn(format!(
                    "column `{}` is referenced by computed column `{}`",
                    column,
                    &f.name()
                )));
            }
        }
    }
    Ok(())
}
