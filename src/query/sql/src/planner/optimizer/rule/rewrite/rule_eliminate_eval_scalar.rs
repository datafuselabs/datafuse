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

use std::collections::HashSet;
use std::sync::Arc;
use databend_common_exception::Result;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;
use crate::plans::EvalScalar;
use crate::plans::RelOp;

pub struct RuleEliminateEvalScalar {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleEliminateEvalScalar {
    pub fn new() -> Self {
        Self {
            id: RuleID::EliminateEvalScalar,
            // EvalScalar
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RuleEliminateEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let eval_scalar: EvalScalar = s_expr.plan().clone().try_into()?;

        // Eliminate duplicated item in eval scalar
        let mut unique_items = HashSet::new();
        let mut items_to_keep = Vec::new();

        for item in eval_scalar.items.iter() {
            if unique_items.insert(item.scalar.clone()) {
                items_to_keep.push(item.clone());
            }
        }

        if items_to_keep.is_empty() {
            state.add_result(s_expr.child(0)?.clone());
            return Ok(());
        }

        // Create a new EvalScalar with unique items
        let new_eval_scalar = EvalScalar {
            items: items_to_keep,
        };

        state.add_result(SExpr::create_unary(
            Arc::new(new_eval_scalar.into()),
            Arc::new(s_expr.child(0)?.clone()),
        ));

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
