/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.operators;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.SequencedSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;

import io.crate.analyze.AnalyzedInsertStatement;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.JoinRelation;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.RelationNames;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.CrateException;
import io.crate.execution.MultiPhaseExecutor;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.builder.SplitPoints;
import io.crate.execution.dsl.projection.builder.SplitPointsBuilder;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.SelectSymbol.ResultType;
import io.crate.expression.symbol.Symbol;
import io.crate.fdw.ForeignDataWrapper;
import io.crate.fdw.ForeignDataWrappers;
import io.crate.fdw.ForeignTableRelation;
import io.crate.fdw.ServersMetadata;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.CorrelatedSubQueries;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.SubqueryPlanner.SubQueries;
import io.crate.planner.consumer.InsertFromSubQueryPlanner;
import io.crate.planner.optimizer.Optimizer;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.iterative.IterativeOptimizer;
import io.crate.planner.optimizer.rule.DeduplicateOrder;
import io.crate.planner.optimizer.rule.EliminateCrossJoin;
import io.crate.planner.optimizer.rule.EquiJoinToLookupJoin;
import io.crate.planner.optimizer.rule.MergeAggregateAndCollectToCount;
import io.crate.planner.optimizer.rule.MergeAggregateRenameAndCollectToCount;
import io.crate.planner.optimizer.rule.MergeFilterAndCollect;
import io.crate.planner.optimizer.rule.MergeFilterAndForeignCollect;
import io.crate.planner.optimizer.rule.MergeFilters;
import io.crate.planner.optimizer.rule.MoveConstantJoinConditionsBeneathJoin;
import io.crate.planner.optimizer.rule.MoveEquiJoinFilterIntoInnerJoin;
import io.crate.planner.optimizer.rule.MoveFilterBeneathCorrelatedJoin;
import io.crate.planner.optimizer.rule.MoveFilterBeneathEval;
import io.crate.planner.optimizer.rule.MoveFilterBeneathGroupBy;
import io.crate.planner.optimizer.rule.MoveFilterBeneathJoin;
import io.crate.planner.optimizer.rule.MoveFilterBeneathOrder;
import io.crate.planner.optimizer.rule.MoveFilterBeneathProjectSet;
import io.crate.planner.optimizer.rule.MoveFilterBeneathRename;
import io.crate.planner.optimizer.rule.MoveFilterBeneathUnion;
import io.crate.planner.optimizer.rule.MoveFilterBeneathWindowAgg;
import io.crate.planner.optimizer.rule.MoveLimitBeneathEval;
import io.crate.planner.optimizer.rule.MoveLimitBeneathRename;
import io.crate.planner.optimizer.rule.MoveOrderBeneathEval;
import io.crate.planner.optimizer.rule.MoveOrderBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveOrderBeneathRename;
import io.crate.planner.optimizer.rule.MoveOrderBeneathUnion;
import io.crate.planner.optimizer.rule.OptimizeCollectWhereClauseAccess;
import io.crate.planner.optimizer.rule.RemoveOrderBeneathInsert;
import io.crate.planner.optimizer.rule.RemoveRedundantEval;
import io.crate.planner.optimizer.rule.ReorderHashJoin;
import io.crate.planner.optimizer.rule.ReorderNestedLoopJoin;
import io.crate.planner.optimizer.rule.RewriteFilterOnCrossJoinToInnerJoin;
import io.crate.planner.optimizer.rule.RewriteFilterOnOuterJoinToInnerJoin;
import io.crate.planner.optimizer.rule.RewriteGroupByKeysLimitToLimitDistinct;
import io.crate.planner.optimizer.rule.RewriteJoinPlan;
import io.crate.planner.optimizer.rule.RewriteLeftOuterJoinToHashJoin;
import io.crate.planner.optimizer.rule.RewriteRightOuterJoinToHashJoin;
import io.crate.planner.optimizer.rule.RewriteToQueryThenFetch;
import io.crate.planner.optimizer.tracer.OptimizerTracer;
import io.crate.role.Role;
import io.crate.sql.tree.JoinType;
import io.crate.types.DataTypes;

/**
 * Planner which can create a {@link ExecutionPlan} using intermediate {@link LogicalPlan} nodes.
 */
public class LogicalPlanner {
    private final IterativeOptimizer optimizer;
    // Join implementations optimization rules have their own optimizer, because these rules have
    // little interaction with the other rules and we want to avoid unnecessary pattern matches on them.
    private final IterativeOptimizer joinImplementationOptimizer;
    private final Visitor statementVisitor = new Visitor();
    private final Optimizer writeOptimizer;
    private final Optimizer fetchOptimizer;
    private final ForeignDataWrappers foreignDataWrappers;

    // Be careful, the order of the rules matter
    public static final List<Rule<?>> ITERATIVE_OPTIMIZER_RULES = List.of(
        new RemoveRedundantEval(),
        new MergeAggregateAndCollectToCount(),
        new MergeAggregateRenameAndCollectToCount(),
        new MergeFilters(),
        new RewriteFilterOnCrossJoinToInnerJoin(),
        new MoveFilterBeneathRename(),
        new MoveFilterBeneathEval(),
        new MoveFilterBeneathOrder(),
        new MoveFilterBeneathProjectSet(),
        new MoveFilterBeneathJoin(),
        new MoveFilterBeneathCorrelatedJoin(),
        new MoveFilterBeneathUnion(),
        new MoveFilterBeneathGroupBy(),
        new MoveFilterBeneathWindowAgg(),
        new MoveLimitBeneathRename(),
        new MoveLimitBeneathEval(),
        new MoveEquiJoinFilterIntoInnerJoin(),
        new MergeFilterAndCollect(),
        new MergeFilterAndForeignCollect(),
        new RewriteFilterOnOuterJoinToInnerJoin(),
        new MoveOrderBeneathUnion(),
        new MoveOrderBeneathEval(),
        new MoveOrderBeneathRename(),
        new DeduplicateOrder(),
        new OptimizeCollectWhereClauseAccess(),
        new RewriteGroupByKeysLimitToLimitDistinct(),
        new MoveConstantJoinConditionsBeneathJoin(),
        new EliminateCrossJoin(),
        new EquiJoinToLookupJoin(),
        new RewriteLeftOuterJoinToHashJoin(),
        new RewriteRightOuterJoinToHashJoin(),
        new RewriteJoinPlan()
    );

    public static final List<Rule<?>> JOIN_IMPLEMENTATION_OPTIMIZER_RULES = List.of(
        new MoveOrderBeneathNestedLoop(),
        new ReorderHashJoin(),
        new ReorderNestedLoopJoin()
    );

    public static final List<Rule<?>> FETCH_OPTIMIZER_RULES = List.of(
        new RemoveRedundantEval(),
        new MergeFilterAndCollect(),
        new RewriteToQueryThenFetch()
    );

    public static final List<Rule<?>> WRITE_OPTIMIZER_RULES = List.of(
        new RewriteInsertFromSubQueryToInsertFromValues(),
        new RemoveOrderBeneathInsert()
    );

    public LogicalPlanner(NodeContext nodeCtx,
                          ForeignDataWrappers foreignDataWrappers,
                          Supplier<Version> minNodeVersionInCluster) {
        this.foreignDataWrappers = foreignDataWrappers;
        this.optimizer = new IterativeOptimizer(
            nodeCtx,
            minNodeVersionInCluster,
            ITERATIVE_OPTIMIZER_RULES
        );
        this.joinImplementationOptimizer = new IterativeOptimizer(
            nodeCtx,
            minNodeVersionInCluster,
            JOIN_IMPLEMENTATION_OPTIMIZER_RULES
        );
        this.fetchOptimizer = new Optimizer(
            nodeCtx,
            minNodeVersionInCluster,
            FETCH_OPTIMIZER_RULES
        );
        this.writeOptimizer = new Optimizer(
            nodeCtx,
            minNodeVersionInCluster,
            WRITE_OPTIMIZER_RULES
        );
    }

    public LogicalPlan plan(AnalyzedStatement statement, PlannerContext plannerContext) {
        return statement.accept(statementVisitor, plannerContext);
    }

    public LogicalPlan planSubSelect(SelectSymbol selectSymbol, PlannerContext plannerContext) {
        AnalyzedRelation relation = selectSymbol.relation();
        final int fetchSize;
        final UnaryOperator<LogicalPlan> maybeApplySoftLimit;
        ResultType resultType = selectSymbol.getResultType();
        switch (resultType) {
            case SINGLE_COLUMN_EXISTS:
                // Exists only needs to know if there are any rows
                fetchSize = 1;
                maybeApplySoftLimit = plan -> new Limit(plan, Literal.of(1), Literal.of(0));
                break;
            case SINGLE_COLUMN_SINGLE_VALUE:
                // SELECT (SELECT foo FROM t)
                //         ^^^^^^^^^^^^^^^^^
                // The subquery must return at most 1 row, if more than 1 row is returned semantics require us to throw an error.
                // So we limit the query to 2 if there is no limit to avoid retrieval of many rows while being able to validate max1row
                fetchSize = 2;
                maybeApplySoftLimit = plan -> new Limit(plan, Literal.of(2L), Literal.of(0L));
                break;
            case SINGLE_COLUMN_MULTIPLE_VALUES:
            default:
                fetchSize = 0;
                maybeApplySoftLimit = plan -> plan;
                break;
        }
        PlannerContext subSelectPlannerContext = PlannerContext.forSubPlan(plannerContext, fetchSize);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(s -> planSubSelect(s, subSelectPlannerContext));
        var planBuilder = new PlanBuilder(
            subqueryPlanner,
            foreignDataWrappers,
            plannerContext.clusterState(),
            plannerContext.transactionContext()
        );
        LogicalPlan plan = relation.accept(planBuilder, relation.outputs());
        plan = tryOptimizeForInSubquery(selectSymbol, relation, plan);
        return new RootRelationBoundary(optimize(maybeApplySoftLimit.apply(plan), relation, plannerContext, false));
    }

    private LogicalPlan optimize(LogicalPlan plan,
                                 AnalyzedRelation relation,
                                 PlannerContext plannerContext,
                                 boolean avoidTopLevelFetch) {
        CoordinatorTxnCtx txnCtx = plannerContext.transactionContext();
        OptimizerTracer tracer = plannerContext.optimizerTracer();
        var timeoutToken = plannerContext.timeoutToken();
        LogicalPlan optimizedPlan = optimizer.optimize(plan, plannerContext.planStats(), txnCtx, tracer, timeoutToken);
        optimizedPlan = joinImplementationOptimizer.optimize(optimizedPlan, plannerContext.planStats(), txnCtx, tracer, timeoutToken);
        LogicalPlan prunedPlan = optimizedPlan.pruneOutputsExcept(relation.outputs());
        assert prunedPlan.outputs().equals(optimizedPlan.outputs())
            : "Pruned plan must have the same outputs as original plan";
        LogicalPlan fetchOptimized = fetchOptimizer.optimize(
            prunedPlan,
            plannerContext.planStats(),
            txnCtx,
            tracer,
            timeoutToken
        );
        if (fetchOptimized != prunedPlan || avoidTopLevelFetch) {
            return fetchOptimized;
        }
        // Doing a second pass here to also rewrite additional plan patterns to "Fetch"
        // The `fetchOptimizer` operators on `Limit - X` fragments of a tree.
        // This here instead operators on a narrow selection of top-level patterns
        //
        // The reason for this is that some plans are cheaper to execute as fetch
        // even if there is no operator that reduces the number of records
        return RewriteToQueryThenFetch.tryRewrite(relation, fetchOptimized);
    }

    public LogicalPlan optimize(LogicalPlan plan, PlannerContext plannerContext) {
        var timeoutToken = plannerContext.timeoutToken();
        LogicalPlan optimizedPlan = optimizer.optimize(
            plan,
            plannerContext.planStats(),
            plannerContext.transactionContext(),
            plannerContext.optimizerTracer(),
            timeoutToken
        );
        optimizedPlan = joinImplementationOptimizer.optimize(
            optimizedPlan,
            plannerContext.planStats(),
            plannerContext.transactionContext(),
            plannerContext.optimizerTracer(),
            timeoutToken
        );
        return optimizedPlan;
    }

    // In case the subselect is inside an IN() or = ANY() apply a "natural" OrderBy to optimize
    // the building of TermInSetQuery which does a sort on the collection of values.
    // See issue https://github.com/crate/crate/issues/6755
    // If the output values are already sorted (even in desc order) no optimization is needed
    private LogicalPlan tryOptimizeForInSubquery(SelectSymbol selectSymbol, AnalyzedRelation relation, LogicalPlan planBuilder) {
        if (selectSymbol.isCorrelated()) {
            return planBuilder;
        }

        if (selectSymbol.parentIsOrderSensitive() == false
            && selectSymbol.getResultType() == SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES
            && relation instanceof QueriedSelectRelation queriedRelation) {

            OrderBy relationOrderBy = queriedRelation.orderBy();
            Symbol firstOutput = queriedRelation.outputs().get(0);
            if ((relationOrderBy == null || relationOrderBy.orderBySymbols().get(0).equals(firstOutput) == false)
                && DataTypes.isPrimitive(firstOutput.valueType())) {

                return Order.create(planBuilder, new OrderBy(Collections.singletonList(firstOutput)));
            }
        }
        return planBuilder;
    }


    public LogicalPlan plan(AnalyzedRelation relation,
                            PlannerContext plannerContext,
                            SubqueryPlanner subqueryPlanner,
                            boolean avoidTopLevelFetch) {
        var planBuilder = new PlanBuilder(
            subqueryPlanner,
            foreignDataWrappers,
            plannerContext.clusterState(),
            plannerContext.transactionContext()
        );
        LogicalPlan logicalPlan = relation.accept(planBuilder, relation.outputs());
        return optimize(logicalPlan, relation, plannerContext, avoidTopLevelFetch);
    }

    static class PlanBuilder extends AnalyzedRelationVisitor<List<Symbol>, LogicalPlan> {

        private final SubqueryPlanner subqueryPlanner;
        private final ForeignDataWrappers foreignDataWrappers;
        private final ClusterState clusterState;
        private final CoordinatorTxnCtx coordinatorTxnCtx;

        private PlanBuilder(SubqueryPlanner subqueryPlanner,
                            ForeignDataWrappers foreignDataWrappers,
                            ClusterState clusterState,
                            CoordinatorTxnCtx coordinatorTxnCtx) {
            this.subqueryPlanner = subqueryPlanner;
            this.foreignDataWrappers = foreignDataWrappers;
            this.clusterState = clusterState;
            this.coordinatorTxnCtx = coordinatorTxnCtx;
        }

        @Override
        public LogicalPlan visitJoinRelation(JoinRelation joinRelation, List<Symbol> outputs) {
            Symbol joinCondition = joinRelation.joinCondition();
            List<Symbol> allOutputs = joinCondition == null ? outputs : Lists.concat(outputs, joinCondition);
            List<Symbol> lhsOutputs = getOutputsForRelation(joinRelation.left(), allOutputs);
            List<Symbol> rhsOutputs = getOutputsForRelation(joinRelation.right(), allOutputs);

            var correlatedSubQueries = CorrelatedSubQueries.of(joinCondition);
            if (correlatedSubQueries.correlatedSubQueries().isEmpty()) {
                return new JoinPlan(
                    joinRelation.left().accept(this, lhsOutputs),
                    joinRelation.right().accept(this, rhsOutputs),
                    joinRelation.joinType(),
                    joinCondition
                );
            } else {
                LogicalPlan source = new JoinPlan(
                    joinRelation.left().accept(this, lhsOutputs),
                    joinRelation.right().accept(this, rhsOutputs),
                    joinRelation.joinType(),
                    AndOperator.join(correlatedSubQueries.remainder())
                );
                for (Symbol symbol : correlatedSubQueries.correlatedSubQueries()) {
                    source = subqueryPlanner.planSubQueries(symbol).applyCorrelatedJoin(source);
                }
                return Filter.create(source, AndOperator.join(correlatedSubQueries.correlatedSubQueries()));
            }
        }

        @Override
        public LogicalPlan visitAnalyzedRelation(AnalyzedRelation relation, List<Symbol> outputs) {
            throw new UnsupportedOperationException(relation.getClass().getSimpleName() + " NYI");
        }

        @Override
        public LogicalPlan visitTableFunctionRelation(TableFunctionRelation relation, List<Symbol> outputs) {
            // MultiPhase is needed here but not in `DocTableRelation` or `TableRelation` because
            // `TableFunctionRelation` is also used for top-level `VALUES`
            //    -> so there wouldn't be a `QueriedSelectRelation` that can do the MultiPhase handling

            SubQueries subQueries = subqueryPlanner.planSubQueries(relation);
            return MultiPhase.createIfNeeded(
                subQueries.uncorrelated(),
                TableFunction.create(relation, relation.outputs(), WhereClause.MATCH_ALL)
            );
        }

        @Override
        public LogicalPlan visitDocTableRelation(DocTableRelation relation, List<Symbol> outputs) {
            return new Collect(relation, outputs, WhereClause.MATCH_ALL);
        }

        @Override
        public LogicalPlan visitTableRelation(TableRelation relation, List<Symbol> outputs) {
            return new Collect(relation, outputs, WhereClause.MATCH_ALL);
        }

        @Override
        public LogicalPlan visitForeignTable(ForeignTableRelation relation, List<Symbol> outputs) {
            Metadata metadata = clusterState.metadata();
            ServersMetadata servers = metadata.custom(ServersMetadata.TYPE, ServersMetadata.EMPTY);
            Server server = servers.get(relation.tableInfo().server());
            ForeignDataWrapper foreignDataWrapper = foreignDataWrappers.get(server.fdw());
            return new ForeignCollect(
                foreignDataWrapper,
                relation,
                outputs,
                WhereClause.MATCH_ALL,
                coordinatorTxnCtx.sessionSettings().userName()
            );
        }

        @Override
        public LogicalPlan visitAliasedAnalyzedRelation(AliasedAnalyzedRelation relation, List<Symbol> outputs) {
            var child = relation.relation();
            if (child instanceof AbstractTableRelation<?>) {
                List<Symbol> mappedOutputs = Lists.map(outputs, FieldReplacer.bind(relation::resolveField));
                var source = child.accept(this, mappedOutputs);
                return new Rename(outputs, relation.relationName(), relation, source);
            } else {
                // Can't do outputs propagation because field reverse resolving could be ambiguous
                //  `SELECT * FROM (select * from t as t1, t as t2)` -> x can refer to t1.x or t2.x
                var source = child.accept(this, child.outputs());
                return new Rename(relation.outputs(), relation.relationName(), relation, source);
            }
        }

        @Override
        public LogicalPlan visitView(AnalyzedView view, List<Symbol> outputs) {
            CoordinatorSessionSettings sessionSettings = coordinatorTxnCtx.sessionSettings();
            Role sessionUser = sessionSettings.sessionUser();
            sessionSettings.setSessionUser(view.owner());
            var child = view.relation();
            var source = child.accept(this, child.outputs());
            sessionSettings.setSessionUser(sessionUser);
            return new Rename(view.outputs(), view.relationName(), view, source);
        }

        @Override
        public LogicalPlan visitUnionSelect(UnionSelect union, List<Symbol> outputs) {
            var lhsRel = union.left();
            var rhsRel = union.right();
            return Distinct.create(
                new Union(
                    lhsRel.accept(this, lhsRel.outputs()),
                    rhsRel.accept(this, rhsRel.outputs()),
                    union.outputs()),
                union.isDistinct(),
                union.outputs()
            );
        }

        private static List<Symbol> getOutputsForRelation(AnalyzedRelation relation, List<Symbol> outputs) {
            LinkedHashSet<Symbol> result = new LinkedHashSet<>();
            SequencedSet<RelationName> relationNamesFromRelation = RelationNames.getShallow(relation);
            Predicate<Symbol> collectFiltered = node -> {
                SequencedSet<RelationName> relationNamesFromSymbol = RelationNames.getShallow(node);
                for (RelationName relationName : relationNamesFromSymbol) {
                    if (relationNamesFromRelation.contains(relationName)) {
                        if (node instanceof ScopedSymbol || node instanceof Reference) {
                            result.add(node);
                            break;
                        }
                    }
                }
                return false;
            };
            for (Symbol output : outputs) {
                output.any(collectFiltered);
            }
            return List.copyOf(result);
        }



        @Override
        public LogicalPlan visitQueriedSelectRelation(QueriedSelectRelation relation, List<Symbol> outputs) {
            SplitPoints splitPoints = SplitPointsBuilder.create(relation);
            SubQueries subQueries = subqueryPlanner.planSubQueries(relation);
            LogicalPlan source = buildImplicitJoins(
                relation.from(),
                relation.where(),
                subQueries,
                rel -> {
                    // Need to pass along the `splitPoints.toCollect` symbols to the relation the symbols belong to
                    // We could get rid of `SplitPoints` and the logic here if we
                    // a) introduce a column pruning
                    // b) Make sure tableRelations contain all columns (incl. sys-columns) in `outputs`
                    List<Symbol> toCollect = splitPoints.toCollect();
                    if (relation.from().size() == 1) {
                        return rel.accept(this, toCollect);
                    } else {
                        return rel.accept(this, getOutputsForRelation(rel, toCollect));
                    }
                }
            );
            Symbol having = relation.having();
            if (having != null && having.any(Symbol.IS_CORRELATED_SUBQUERY)) {
                throw new UnsupportedOperationException("Cannot use correlated subquery in HAVING clause");
            }
            return MultiPhase.createIfNeeded(
                subQueries.uncorrelated(),
                Eval.create(
                    Limit.create(
                        Order.create(
                            Distinct.create(
                                ProjectSet.create(
                                    WindowAgg.create(
                                        Filter.create(
                                            groupByOrAggregate(
                                                ProjectSet.create(
                                                    source,
                                                    splitPoints.tableFunctionsBelowGroupBy()
                                                ),
                                                relation.groupBy(),
                                                splitPoints.aggregates()
                                            ),
                                            having
                                        ),
                                        splitPoints.windowFunctions()
                                    ),
                                    splitPoints.tableFunctions()
                                ),
                                relation.isDistinct(),
                                relation.outputs()
                            ),
                            relation.orderBy()
                        ),
                        relation.limit(),
                        relation.offset()
                    ),
                    outputs
                )
            );
        }
    }

    static LogicalPlan buildImplicitJoins(List<AnalyzedRelation> from,
                                          Symbol whereClause,
                                          SubQueries subQueries,
                                          java.util.function.Function<AnalyzedRelation, LogicalPlan> toLogicalPlan) {
        final LogicalPlan logicalPlan;
        if (from.size() == 1) {
            // We have only one relation, this means we have no implicit joins
            logicalPlan = toLogicalPlan.apply(from.getFirst());
        } else {
            // We have more than one relation, this means we have implicit joins in the query
            // Therefore, convert all relations from the from-clause to logical plans and join them
            Iterator<AnalyzedRelation> relations = from.iterator();

            LogicalPlan joinPlan = new JoinPlan(
                toLogicalPlan.apply(relations.next()),
                toLogicalPlan.apply(relations.next()),
                JoinType.CROSS,
                null);

            while (relations.hasNext()) {
                joinPlan = new JoinPlan(joinPlan, toLogicalPlan.apply(relations.next()), JoinType.CROSS, null);
            }
            logicalPlan = joinPlan;
        }
        LogicalPlan correlatedJoin = subQueries.applyCorrelatedJoin(logicalPlan);
        return Filter.create(correlatedJoin, whereClause);
    }

    private static LogicalPlan groupByOrAggregate(LogicalPlan source,
                                                  List<Symbol> groupKeys,
                                                  List<Function> aggregates) {
        if (!groupKeys.isEmpty()) {
            return new GroupHashAggregate(source, groupKeys, aggregates);
        }
        if (!aggregates.isEmpty()) {
            return new HashAggregate(source, aggregates);
        }
        return source;
    }

    public static Set<Symbol> extractColumns(Symbol symbol) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        symbol.visit(Symbol.IS_COLUMN, columns::add);
        return columns;
    }

    public static Set<Symbol> extractColumns(Collection<? extends Symbol> symbols) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        for (Symbol symbol : symbols) {
            symbol.visit(Symbol.IS_COLUMN, columns::add);
        }
        return columns;
    }

    public static void execute(LogicalPlan logicalPlan,
                               DependencyCarrier executor,
                               PlannerContext plannerContext,
                               RowConsumer consumer,
                               Row params,
                               SubQueryResults subQueryResults,
                               boolean enableProfiling) {
        if (logicalPlan.dependencies().isEmpty()) {
            doExecute(logicalPlan, executor, plannerContext, consumer, params, subQueryResults, enableProfiling);
        } else {
            MultiPhaseExecutor.execute(logicalPlan.dependencies(), executor, plannerContext, params)
                .whenComplete((valueBySubQuery, failure) -> {
                    if (failure == null) {
                        doExecute(
                            logicalPlan,
                            executor,
                            plannerContext,
                            consumer,
                            params,
                            SubQueryResults.merge(subQueryResults, valueBySubQuery),
                            false
                        );
                    } else {
                        consumer.accept(null, failure);
                    }
                });
        }
    }

    private static void doExecute(LogicalPlan logicalPlan,
                                  DependencyCarrier executor,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  SubQueryResults subQueryResults,
                                  boolean enableProfiling) {
        NodeOperationTree nodeOpTree;
        try {
            nodeOpTree = getNodeOperationTree(logicalPlan, executor, plannerContext, params, subQueryResults);
        } catch (Throwable t) {
            consumer.accept(null, t);
            return;
        }
        executeNodeOpTree(
            executor,
            plannerContext.transactionContext(),
            plannerContext.jobId(),
            consumer,
            enableProfiling,
            nodeOpTree
        );
    }

    public static NodeOperationTree getNodeOperationTree(LogicalPlan logicalPlan,
                                                         DependencyCarrier executor,
                                                         PlannerContext plannerContext,
                                                         Row params,
                                                         SubQueryResults subQueryResults) {
        try {
            var executionPlan = logicalPlan.build(
                executor,
                plannerContext,
                Set.of(),
                executor.projectionBuilder(),
                -1,
                0,
                null,
                null,
                params,
                subQueryResults);
            return NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId());
        } catch (ConversionException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof CrateException || e instanceof ElasticsearchException) {
                // Don't hide errors like MissingShardOperationsException, UnavailableShardsException
                throw e;
            }

            // This should really only happen if there are planner bugs,
            // so the additional costs of creating a more informative exception shouldn't matter.
            PrintContext printContext = new PrintContext(plannerContext.planStats());
            logicalPlan.print(printContext);
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException(
                String.format(
                    Locale.ENGLISH,
                    "Couldn't create execution plan from logical plan because of: %s:%n%s",
                    e.getMessage(),
                    printContext
                ),
                e
            );
            illegalArgumentException.setStackTrace(e.getStackTrace());
            throw illegalArgumentException;
        }
    }

    public static void executeNodeOpTree(DependencyCarrier dependencies,
                                         TransactionContext txnCtx,
                                         UUID jobId,
                                         RowConsumer consumer,
                                         boolean enableProfiling,
                                         NodeOperationTree nodeOpTree) {
        dependencies.phasesTaskFactory()
            .create(jobId, Collections.singletonList(nodeOpTree), enableProfiling)
            .execute(consumer, txnCtx);
    }

    private class Visitor extends AnalyzedStatementVisitor<PlannerContext, LogicalPlan> {

        @Override
        protected LogicalPlan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, PlannerContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "Cannot create LogicalPlan from AnalyzedStatement \"%s\"  - not supported.", analyzedStatement));
        }

        @Override
        public LogicalPlan visitSelectStatement(AnalyzedRelation relation, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(s -> planSubSelect(s, context));
            LogicalPlan logicalPlan = plan(relation, context, subqueryPlanner, false);
            return new RootRelationBoundary(logicalPlan);
        }

        @Override
        protected LogicalPlan visitAnalyzedInsertStatement(AnalyzedInsertStatement statement, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(s -> planSubSelect(s, context));
            return writeOptimizer.optimize(
                InsertFromSubQueryPlanner.plan(
                    statement,
                    context,
                    LogicalPlanner.this,
                    subqueryPlanner),
                context.planStats(),
                context.transactionContext(),
                context.optimizerTracer(),
                context.timeoutToken()
            );
        }
    }
}
