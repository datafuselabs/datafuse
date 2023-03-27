// Copyright 2023 Datafuse Labs.
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

use std::collections::HashMap;
use std::collections::HashSet;

use common_expression::types::number::NumberDataType;
use common_expression::types::DataType;
use common_functions::aggregates::AggregateFunctionFactory;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::ColumnSet;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::ColumnBinding;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Visibility;

/// Rule to push aggregation past a join to reduces the number of input rows to the join.
/// Read the paper "Eager aggregation and lazy aggregation" for more details.
///
/// (1) Eager Group-By:
/// Input:
///                 expression
///                     |   
///           aggregate(final): SUM(x)
///                     |
///          aggregate(partial): SUM(x)
///                     |
///                    join
///                   /    \
///                  *      *
///
/// Output:
///                 expression
///                     |
///     final aggregate(final): SUM(EAGER SUM(x))
///                     |
///    final aggregate(partial): SUM(EAGER SUM(x))
///                     |
///                    join
///                   /    \
///                  *      eager aggregate(final): EAGER SUM(x)
///                          \
///                           eager aggregate(partial): EAGER SUM(x)
///
/// (2) Eager Count:
/// Input:
///                 expression
///                     |   
///              aggregate(final)
///                     |
///             aggregate(partial)
///                     |
///                    join
///                   /    \
///                  *      *
///
/// Output:
///                 expression
///                     |
///     final aggregate(final) SUM(x) * cnt
///                     |
///    final aggregate(partial) SUM(x) * cnt
///                     |
///                    join
///                   /    \
///                  *(x)   eager count(final) as cnt
///                          \
///                           eager count(partial) as cnt
///
/// (3) Double Eager:
/// Input:
///                 expression
///                     |   
///           aggregate(final): SUM(x)
///                     |
///          aggregate(partial): SUM(x)
///                     |
///                    join
///                   /    \
///                  *      *
///
/// Output:
///                 expression
///                     |
///     final aggregate(final): SUM(EAGER SUM(x)) * cnt
///                     |
///    final aggregate(partial): SUM(EAGER SUM(x)) * cnt
///                     |
///                    join
///                     |  \
///                     |    eager count(final) as cnt
///                     |     \
///                     |      eager count(partial) as cnt
///                     |
///                     eager aggregate(final): EAGER SUM(x)
///                     |
///                     eager aggregate(partial): EAGER SUM(x)

pub struct RuleEagerAggregation {
    id: RuleID,
    pattern: SExpr,
    metadata: MetadataRef,
}

impl RuleEagerAggregation {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::EagerAggregation,

            //     Expression
            //         |
            //  Aggregate(final)
            //         |
            // Aggregate(partial)
            //         |
            //        Join
            //       /    \
            //      *      *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::EvalScalar,
                }
                .into(),
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::Aggregate,
                    }
                    .into(),
                    SExpr::create_unary(
                        PatternPlan {
                            plan_type: RelOp::Aggregate,
                        }
                        .into(),
                        SExpr::create_binary(
                            PatternPlan {
                                plan_type: RelOp::Join,
                            }
                            .into(),
                            SExpr::create_leaf(
                                PatternPlan {
                                    plan_type: RelOp::Pattern,
                                }
                                .into(),
                            ),
                            SExpr::create_leaf(
                                PatternPlan {
                                    plan_type: RelOp::Pattern,
                                }
                                .into(),
                            ),
                        ),
                    ),
                ),
            ),
            metadata,
        }
    }
}

impl Rule for RuleEagerAggregation {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, a_expr: &SExpr, state: &mut TransformResult) -> common_exception::Result<()> {
        let eval_scalar_expr = a_expr;
        let final_agg_final_expr = eval_scalar_expr.child(0)?;
        let final_agg_partial_expr = final_agg_final_expr.child(0)?;
        let join_expr = final_agg_partial_expr.child(0)?;

        let join: Join = join_expr.plan().clone().try_into()?;
        // Only supports inner join and cross join.
        if !matches!(join.join_type, JoinType::Inner | JoinType::Cross) {
            return Ok(());
        }

        let mut eval_scalar: EvalScalar = eval_scalar_expr.plan().clone().try_into()?;
        let mut final_agg_final: Aggregate = final_agg_final_expr.plan().clone().try_into()?;
        let mut final_agg_partial: Aggregate = final_agg_partial_expr.plan().clone().try_into()?;

        // Get the original column set from the left child and right child of join.
        let mut columns_sets = [ColumnSet::new(), ColumnSet::new()];
        for (idx, columns_set) in columns_sets.iter_mut().enumerate() {
            get_columns_set(&join_expr.children[idx], columns_set);
        }

        // Find all `BoundColumnRef`, and then create a mapping from the column source
        // of `BoundColumnRef` to the item's index.
        let eval_scalar_items: HashMap<usize, usize> = eval_scalar
            .items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| match &item.scalar {
                ScalarExpr::BoundColumnRef(column) => Some((column.column.index, index)),
                _ => None,
            })
            .collect();

        // Get all eager aggregate functions of the left child and right child.
        let function_factory = AggregateFunctionFactory::instance();
        let mut eager_aggregations = Vec::new();
        for columns_set in columns_sets.iter() {
            eager_aggregations.push(get_eager_aggregation_functions(
                &final_agg_final,
                columns_set,
                &eval_scalar_items,
                function_factory,
            ))
        }

        // There are some aggregate functions that cannot be pushed down, if a func(x)
        // cannot be pushed down, it means that we need to add x to the group by items
        // of eager aggregation, it will change the semantics of the sql, So we should
        // stop eager aggregation.
        if eager_aggregations[0].len() + eager_aggregations[1].len()
            != final_agg_final.aggregate_functions.len()
        {
            return Ok(());
        }

        // Divide group by columns into two parts, where group_columns_set[0] comes from
        // the left child and group_columns_set[1] comes from the right child.
        let mut group_columns_set = [ColumnSet::new(), ColumnSet::new()];
        for item in final_agg_final.group_columns()?.iter() {
            for idx in 0..2 {
                if columns_sets[idx].contains(item) {
                    group_columns_set[idx].insert(*item);
                }
            }
        }

        let mut can_eager = [true, true];
        // If a child's `can_push_down` is true, its group_columns_set should include all
        // join conditions related to the child.
        let conditions = [&join.left_conditions, &join.right_conditions];
        for idx in 0..2 {
            for cond in conditions[idx].iter() {
                for c in cond.used_columns().iter() {
                    if !group_columns_set[idx].contains(c) {
                        can_eager[idx] = false;
                    }
                }
            }
        }

        let can_push_down = [
            !eager_aggregations[0].is_empty() && can_eager[0],
            !eager_aggregations[1].is_empty() && can_eager[1],
        ];

        let mut success = false;
        let d = if can_push_down[0] { 0 } else { 1 };

        if can_push_down[d] && can_push_down[d ^ 1] {
            // Apply eager split on both.
            // Apply eager groupby-count on d.
        } else if can_push_down[d] && !can_push_down[d ^ 1] {
            // Apply eager group-by on d and try to apply eager count on d ^ 1

            // Find all AVG aggregate functions, convert AVG to SUM and then add a COUNT aggregate function.
            // AVG(C) => SUM(C) / COUNT(C)
            let mut avg_components = HashMap::new();
            let mut new_eager_aggregations = Vec::new();
            for (index, _, func_name) in eager_aggregations[d].iter_mut() {
                if func_name.as_str() != "avg" {
                    continue;
                }
                *func_name = "sum".to_string();

                // Add COUNT aggregate functions.
                final_agg_final
                    .aggregate_functions
                    .push(final_agg_final.aggregate_functions[*index].clone());
                final_agg_partial
                    .aggregate_functions
                    .push(final_agg_partial.aggregate_functions[*index].clone());

                // Get SUM(currently still AVG) aggregate functions.
                let sum_aggregation_functions = vec![
                    &mut final_agg_partial.aggregate_functions[*index],
                    &mut final_agg_final.aggregate_functions[*index],
                ];

                // AVG => SUM
                let sum_index = sum_aggregation_functions[0].index;
                for aggregate_function in sum_aggregation_functions {
                    if let ScalarExpr::AggregateFunction(agg) = &mut aggregate_function.scalar {
                        if let ScalarExpr::BoundColumnRef(column) = &agg.args[0] {
                            agg.func_name = "sum".to_string();
                            self.metadata.write().change_derived_column_alias(
                                aggregate_function.index,
                                format!(
                                    "{}({}.{})",
                                    agg.func_name.clone(),
                                    &column.column.table_name.clone().unwrap_or(String::new()),
                                    &column.column.column_name.clone(),
                                ),
                            );
                            let func =
                                function_factory.get(&agg.func_name, agg.params.clone(), vec![
                                    *column.column.data_type.clone(),
                                ])?;
                            agg.return_type = Box::new(func.return_type()?);
                        }
                    }
                }

                // Get COUNT(currently still AVG) aggregate functions.
                let last_index = final_agg_partial.aggregate_functions.len() - 1;
                let count_aggregation_functions = vec![
                    &mut final_agg_partial.aggregate_functions[last_index],
                    &mut final_agg_final.aggregate_functions[last_index],
                ];

                // Generate a new column for COUNT.
                let mut table_name = String::new();
                let mut column_name = String::new();
                if let ScalarExpr::AggregateFunction(agg) = &count_aggregation_functions[0].scalar {
                    if let ScalarExpr::BoundColumnRef(column) = &agg.args[0] {
                        table_name = column.column.table_name.clone().unwrap_or(String::new());
                        column_name = column.column.column_name.clone();
                    }
                }
                let new_index = self.metadata.write().add_derived_column(
                    format!("{}({}.{})", &func_name, table_name, column_name),
                    count_aggregation_functions[0].scalar.data_type()?,
                );

                // AVG => COUNT
                for aggregate_function in count_aggregation_functions {
                    if let ScalarExpr::AggregateFunction(agg) = &mut aggregate_function.scalar {
                        agg.func_name = "count".to_string();
                        agg.return_type = Box::new(DataType::Number(NumberDataType::UInt64));
                    }
                    aggregate_function.index = new_index;
                }

                // AVG = SUM / COUNT, save AVG components to HashMap: (key=SUM, value=COUNT).
                avg_components.insert(sum_index, new_index);
                // Add COUNT to new_eager_aggregations.
                new_eager_aggregations.push((last_index, new_index, "count".to_string()));
            }
            eager_aggregations[d].extend(new_eager_aggregations);

            // Add eager COUNT aggregate functions.
            let mut eager_count_old_index = -1;
            if can_eager[d ^ 1] {
                final_agg_final
                    .aggregate_functions
                    .push(final_agg_final.aggregate_functions[0].clone());
                final_agg_partial
                    .aggregate_functions
                    .push(final_agg_partial.aggregate_functions[0].clone());

                let last_index = final_agg_partial.aggregate_functions.len() - 1;

                let eager_count_aggregation_functions = vec![
                    &mut final_agg_partial.aggregate_functions[last_index],
                    &mut final_agg_final.aggregate_functions[last_index],
                ];

                let new_index = self.metadata.write().add_derived_column(
                    "count(*)".to_string(),
                    DataType::Number(NumberDataType::UInt64),
                );
                for aggregate_function in eager_count_aggregation_functions {
                    if let ScalarExpr::AggregateFunction(agg) = &mut aggregate_function.scalar {
                        agg.func_name = "count".to_string();
                        agg.distinct = false;
                        agg.return_type = Box::new(DataType::Number(NumberDataType::UInt64));
                        agg.args = vec![];
                        agg.display_name = "count(*)".to_string();
                    }
                    aggregate_function.index = new_index;
                }
                eager_aggregations[d ^ 1].push((last_index, new_index, "count".to_string()));
                eager_count_old_index = new_index as i32;
            }

            let mut eager_agg_final = final_agg_final.clone();
            let mut eager_agg_partial = final_agg_partial.clone();
            let mut eager_count_agg_final = final_agg_final.clone();
            let mut eager_count_agg_partial = final_agg_partial.clone();

            // Map AVG components's old-index to new-index
            let mut old_to_new = HashMap::new();

            // The Vec index of eval_scalar.items for each aggregate function.
            let mut eval_scalar_index = Vec::new();
            // The eager_aggregations here only only contains MIN, MAX, SUM and COUNT,
            // and the AVG has been transformed into SUM and COUNT.
            for eager_aggregation in eager_aggregations.iter() {
                for (index, _, func_name) in eager_aggregation.iter() {
                    let final_aggregate_functions = vec![
                        &mut final_agg_partial.aggregate_functions[*index],
                        &mut final_agg_final.aggregate_functions[*index],
                    ];

                    let old_index = final_aggregate_functions[0].index;
                    let new_index = self.metadata.write().add_derived_column(
                        format!("_eager_final_{}", &func_name),
                        final_aggregate_functions[0].scalar.data_type()?,
                    );
                    old_to_new.insert(old_index, new_index);

                    // Modify final aggregate functions.
                    for aggregate_function in final_aggregate_functions {
                        if let ScalarExpr::AggregateFunction(agg) = &mut aggregate_function.scalar {
                            // final_aggregate_functions is currently a clone of eager aggregation.
                            modify_final_aggregate_function(agg, old_index);
                            // final_aggregate_functions is a final aggregation now.
                            aggregate_function.index = new_index;
                        }
                    }

                    // Modify the eval scalars of all aggregate functions that are not AVG components.
                    if let Some(idx) = eval_scalar_items.get(&old_index) && !avg_components.contains_key(&old_index) {
                    // if let Some(idx) = eval_scalar_items.get(&old_index) && !avg_components.contains_key(&old_index) && (eager_count_old_index == -1 || (&eager_count_old_index != &(old_index as i32))) {
                        let eval_scalar_item = &mut eval_scalar.items[*idx];
                        if let ScalarExpr::BoundColumnRef(column) = &mut eval_scalar_item.scalar {
                            let column_binding = &mut column.column;
                            column_binding.index = new_index;
                            if func_name == "count" {
                                column_binding.data_type = Box::new(DataType::Nullable(Box::new(
                                    DataType::Number(NumberDataType::UInt64),
                                )));
                                eval_scalar_item.scalar = cast_expr_if_needed(eval_scalar_item.scalar.clone(), DataType::Number(NumberDataType::UInt64));
                            }
                            success = true;
                            if eager_count_old_index != -1 {
                                eval_scalar_index.push(*idx);
                            }
                        }
                    }
                }
            }

            // AVG(C) = SUM(C) / COUNT(C)
            // Use AVG components(SUM and COUNT) to build AVG eval scalar.
            for (sum_index, count_index) in avg_components.iter() {
                if let Some(idx) = eval_scalar_items.get(sum_index) {
                    let eval_scalar_item = &mut eval_scalar.items[*idx];
                    eval_scalar_item.scalar =
                        create_avg_scalar_item(old_to_new[sum_index], old_to_new[count_index]);
                    success = true;
                    if eager_count_old_index != -1 {
                        eval_scalar_index.push(*idx);
                    }
                }
            }

            if !success {
                return Ok(());
            }

            // Try to multiply eager count for each aggregate result.
            if eager_count_old_index != -1 {
                for idx in eval_scalar_index {
                    let eval_scalar_item = &mut eval_scalar.items[idx];
                    eval_scalar_item.scalar = create_eager_count_multiply_scalar_item(
                        eval_scalar_item.scalar.clone(),
                        old_to_new[&(eager_count_old_index as usize)],
                    );
                }
            }

            // Remove group by items and aggregate functions that do not belong to d.
            remove_group_by_items_and_aggregate_functions(
                &mut eager_agg_final,
                &mut eager_agg_partial,
                &columns_sets[d],
                &eager_aggregations[d ^ 1],
            );
            // let left_child =
            let join_expr = match can_eager[d ^ 1] {
                true => {
                    // Remove group by items and aggregate functions that do not belong to d ^ 1.
                    remove_group_by_items_and_aggregate_functions(
                        &mut eager_count_agg_final,
                        &mut eager_count_agg_partial,
                        &columns_sets[d ^ 1],
                        &eager_aggregations[d],
                    );
                    if d == 0 {
                        join_expr.replace_children(vec![
                            SExpr::create_unary(
                                RelOperator::Aggregate(eager_agg_final),
                                SExpr::create_unary(
                                    RelOperator::Aggregate(eager_agg_partial),
                                    join_expr.child(0)?.clone(),
                                ),
                            ),
                            SExpr::create_unary(
                                RelOperator::Aggregate(eager_count_agg_final),
                                SExpr::create_unary(
                                    RelOperator::Aggregate(eager_count_agg_partial),
                                    join_expr.child(1)?.clone(),
                                ),
                            ),
                        ])
                    } else {
                        join_expr.replace_children(vec![
                            SExpr::create_unary(
                                RelOperator::Aggregate(eager_count_agg_final),
                                SExpr::create_unary(
                                    RelOperator::Aggregate(eager_count_agg_partial),
                                    join_expr.child(0)?.clone(),
                                ),
                            ),
                            SExpr::create_unary(
                                RelOperator::Aggregate(eager_agg_final),
                                SExpr::create_unary(
                                    RelOperator::Aggregate(eager_agg_partial),
                                    join_expr.child(1)?.clone(),
                                ),
                            ),
                        ])
                    }
                }
                false => {
                    if d == 0 {
                        join_expr.replace_children(vec![
                            SExpr::create_unary(
                                RelOperator::Aggregate(eager_agg_final),
                                SExpr::create_unary(
                                    RelOperator::Aggregate(eager_agg_partial),
                                    join_expr.child(0)?.clone(),
                                ),
                            ),
                            join_expr.child(1)?.clone(),
                        ])
                    } else {
                        join_expr.replace_children(vec![
                            join_expr.child(0)?.clone(),
                            SExpr::create_unary(
                                RelOperator::Aggregate(eager_agg_final),
                                SExpr::create_unary(
                                    RelOperator::Aggregate(eager_agg_partial),
                                    join_expr.child(1)?.clone(),
                                ),
                            ),
                        ])
                    }
                }
            };

            let mut result = eval_scalar_expr
                .replace_children(vec![
                    final_agg_final_expr
                        .replace_children(vec![
                            final_agg_partial_expr
                                .replace_children(vec![join_expr])
                                .replace_plan(final_agg_partial.try_into()?),
                        ])
                        .replace_plan(final_agg_final.try_into()?),
                ])
                .replace_plan(eval_scalar.try_into()?);

            result.set_applied_rule(&self.id);
            state.add_result(result);
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}

fn get_columns_set(s_expr: &SExpr, columns_set: &mut ColumnSet) {
    match &s_expr.plan {
        RelOperator::Scan(scan) => {
            columns_set.extend(scan.columns.clone());
        }
        RelOperator::Join(_) => {
            get_columns_set(&s_expr.children[0], columns_set);
            get_columns_set(&s_expr.children[1], columns_set);
        }
        RelOperator::UnionAll(union_all) => {
            columns_set.extend(union_all.used_columns().unwrap());
        }
        _ => {
            if !s_expr.children().is_empty() {
                get_columns_set(&s_expr.children[0], columns_set);
            }
        }
    }
}

// In the current implementation, if an aggregation function can be eager,
// then it needs to satisfy the following constraints:
// (1) The args.len() is equal to 1.
// (2) The aggregate function can be decomposed.
// (3) The index of the aggregate function can be found in the column source of the eval scalar.
// (4) The data type of the aggregate column is either Number or Nullable(Number).
// Return the (Vec index, func index, func_name) for each eager aggregation function.
fn get_eager_aggregation_functions(
    agg_final: &Aggregate,
    columns_set: &ColumnSet,
    eval_scalar_items: &HashMap<usize, usize>,
    function_factory: &AggregateFunctionFactory,
) -> Vec<(usize, IndexType, String)> {
    agg_final
        .aggregate_functions
        .iter()
        .enumerate()
        .filter_map(|(index, aggregate_item)| match &aggregate_item.scalar {
            ScalarExpr::AggregateFunction(aggregate_function)
                if aggregate_function.args.len() == 1
                    && function_factory.is_decomposable(&aggregate_function.func_name)
                    && eval_scalar_items.contains_key(&aggregate_item.index) =>
            {
                match &aggregate_function.args[0] {
                    ScalarExpr::BoundColumnRef(column) => match &*column.column.data_type {
                        DataType::Number(_) if columns_set.contains(&column.column.index) => {
                            Some((
                                index,
                                aggregate_item.index,
                                aggregate_function.func_name.clone(),
                            ))
                        }
                        DataType::Nullable(ty) => match **ty {
                            DataType::Number(_) if columns_set.contains(&column.column.index) => {
                                Some((
                                    index,
                                    aggregate_item.index,
                                    aggregate_function.func_name.clone(),
                                ))
                            }
                            _ => None,
                        },
                        _ => None,
                    },
                    _ => None,
                }
            }
            _ => None,
        })
        .collect::<Vec<_>>()
}

// Final aggregate functions's data type = eager aggregate functions's return_type
// For COUNT, func_name: count => sum, return_type: Nullable(UInt64)
fn modify_final_aggregate_function(agg: &mut AggregateFunction, args_index: usize) {
    if agg.func_name.as_str() == "count" {
        agg.func_name = "sum".to_string();
        agg.return_type = Box::new(DataType::Nullable(Box::new(DataType::Number(
            NumberDataType::UInt64,
        ))));
    }
    if agg.args.is_empty() {
        agg.args.push(ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBinding {
                database_name: None,
                table_name: None,
                column_name: "_eager".to_string(),
                index: args_index,
                data_type: agg.return_type.clone(),
                visibility: Visibility::Visible,
            },
        }));
        return;
    }
    agg.args[0] = ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBinding {
            database_name: None,
            table_name: None,
            column_name: "_eager".to_string(),
            index: args_index,
            data_type: agg.return_type.clone(),
            visibility: Visibility::Visible,
        },
    });
}

fn cast_expr_if_needed(expr: ScalarExpr, target_data_type: DataType) -> ScalarExpr {
    match expr.data_type() {
        Ok(data_type) if data_type != target_data_type => ScalarExpr::CastExpr(CastExpr {
            span: None,
            is_try: false,
            argument: Box::new(expr),
            target_type: Box::new(target_data_type),
        }),
        _ => expr,
    }
}

fn remove_group_by_items_and_aggregate_functions(
    agg_final: &mut Aggregate,
    agg_partial: &mut Aggregate,
    columns_set: &ColumnSet,
    eager_aggregations: &[(usize, usize, String)],
) {
    // Remove group by items.
    let origin_group_by_index = agg_final
        .group_items
        .iter()
        .map(|x| x.index)
        .collect::<Vec<_>>();
    for group_by_index in origin_group_by_index {
        if !columns_set.contains(&group_by_index) {
            for (idx, item) in agg_final.group_items.iter().enumerate() {
                if item.index == group_by_index {
                    agg_final.group_items.remove(idx);
                    agg_partial.group_items.remove(idx);
                    break;
                }
            }
        }
    }
    // Remove aggregate functions.
    let funcs_not_owned_by_eager: HashSet<usize> = eager_aggregations
        .iter()
        .map(|(_, index, _)| *index)
        .collect();
    for func_index in funcs_not_owned_by_eager.iter() {
        for (idx, item) in agg_final.aggregate_functions.iter().enumerate() {
            if &item.index == func_index {
                agg_final.aggregate_functions.remove(idx);
                agg_partial.aggregate_functions.remove(idx);
                break;
            }
        }
    }
}

fn create_avg_scalar_item(left_index: usize, right_index: usize) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "divide".to_string(),
        params: vec![],
        arguments: vec![
            ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBinding {
                    database_name: None,
                    table_name: None,
                    column_name: "_eager_final_sum".to_string(),
                    index: left_index,
                    data_type: Box::new(DataType::Number(NumberDataType::Float64)),
                    visibility: Visibility::Visible,
                },
            }),
            cast_expr_if_needed(
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: ColumnBinding {
                        database_name: None,
                        table_name: None,
                        column_name: "_eager_final_count".to_string(),
                        index: right_index,
                        data_type: Box::new(DataType::Nullable(Box::new(DataType::Number(
                            NumberDataType::UInt64,
                        )))),
                        visibility: Visibility::Visible,
                    },
                }),
                DataType::Number(NumberDataType::UInt64),
            ),
        ],
    })
}

fn create_eager_count_multiply_scalar_item(
    left_scalar_item: ScalarExpr,
    right_index: usize,
) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "multiply".to_string(),
        params: vec![],
        arguments: vec![
            left_scalar_item,
            cast_expr_if_needed(
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: ColumnBinding {
                        database_name: None,
                        table_name: None,
                        column_name: "_eager_final_count".to_string(),
                        index: right_index,
                        data_type: Box::new(DataType::Nullable(Box::new(DataType::Number(
                            NumberDataType::UInt64,
                        )))),
                        visibility: Visibility::Visible,
                    },
                }),
                DataType::Number(NumberDataType::UInt64),
            ),
        ],
    })
}
