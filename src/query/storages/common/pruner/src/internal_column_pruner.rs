//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;

use common_expression::types::string::StringDomain;
use common_expression::ConstantFolder;
use common_expression::Domain;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_functions::BUILTIN_FUNCTIONS;

/// Only support `_segment_name` and `_block_name` now.
pub struct InternalColumnPruner {
    func_ctx: FunctionContext,
    input_domains: HashMap<String, Domain>,
    expr: Option<Expr<String>>,
}

impl InternalColumnPruner {
    pub fn create(func_ctx: FunctionContext, expr: Option<Expr<String>>) -> Self {
        let input_domains = if let Some(expr) = &expr {
            expr.column_refs()
                .into_iter()
                .map(|(name, ty)| (name, Domain::full(&ty)))
                .collect()
        } else {
            HashMap::new()
        };

        InternalColumnPruner {
            func_ctx,
            input_domains,
            expr,
        }
    }

    pub fn should_keep(&self, col_name: &str, value: &str) -> bool {
        if let Some(expr) = &self.expr {
            let mut input_domains = self.input_domains.clone();
            input_domains
                .entry(col_name.to_string())
                .and_modify(|domain| {
                    let bytes = value.as_bytes().to_vec();
                    *domain = Domain::String(StringDomain {
                        min: bytes.clone(),
                        max: Some(bytes),
                    });
                });
            let (folded_expr, _) = ConstantFolder::fold_with_domain(
                expr,
                input_domains,
                &self.func_ctx,
                &BUILTIN_FUNCTIONS,
            );

            !matches!(folded_expr, Expr::Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        } else {
            true
        }
    }
}
