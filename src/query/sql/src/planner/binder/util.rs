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
use databend_common_expression::types::DataType;

use crate::optimizer::SExpr;
use crate::plans::RelOperator;
use crate::Binder;

/// Ident name can not contain ' or "
/// Forbidden ' or " in UserName and RoleName, to prevent Meta injection problem
pub fn illegal_ident_name(ident_name: &str) -> bool {
    ident_name.chars().any(|c| c == '\'' || c == '\"')
}

impl Binder {
    // Find all recursive cte scans and update the data type of field in cte scan
    pub fn count_r_cte_scan(
        &mut self,
        expr: &SExpr,
        count: &mut usize,
        cte_types: &mut Vec<DataType>,
    ) -> Result<()> {
        match expr.plan() {
            RelOperator::Join(_) | RelOperator::UnionAll(_) | RelOperator::MaterializedCte(_) => {
                self.count_r_cte_scan(expr.child(0)?, count, cte_types)?;
                self.count_r_cte_scan(expr.child(1)?, count, cte_types)?;
            }
            RelOperator::Sort(_)
            | RelOperator::Limit(_)
            | RelOperator::Window(_)
            | RelOperator::ProjectSet(_)
            | RelOperator::AsyncFunction(_)
            | RelOperator::Udf(_)
            | RelOperator::EvalScalar(_)
            | RelOperator::Filter(_)
            | RelOperator::Aggregate(_) => {
                self.count_r_cte_scan(expr.child(0)?, count, cte_types)?;
            }
            RelOperator::RecursiveCteScan(plan) => {
                *count += 1_usize;
                if cte_types.is_empty() {
                    cte_types.extend(
                        plan.fields
                            .iter()
                            .map(|f| f.data_type().clone())
                            .collect::<Vec<DataType>>(),
                    );
                }
            }

            RelOperator::Exchange(_)
            | RelOperator::AddRowNumber(_)
            | RelOperator::Scan(_)
            | RelOperator::CteScan(_)
            | RelOperator::DummyTableScan(_)
            | RelOperator::ConstantTableScan(_)
            | RelOperator::ExpressionScan(_)
            | RelOperator::CacheScan(_) => {}
        }
        Ok(())
    }
}
