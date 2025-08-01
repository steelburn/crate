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

package io.crate.planner.statement;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.AnalyzedDeleteStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.ddl.tables.DropPartitionsRequest;
import io.crate.execution.ddl.tables.TransportDropPartitionsAction;
import io.crate.execution.dml.BulkResponse;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.DeleteProjection;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.IndexName;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.WhereClauseOptimizer;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ddl.DeleteAllPartitions;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.dml.DeleteById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.optimizer.symbol.Optimizer;
import io.crate.types.DataTypes;

public final class DeletePlanner {

    private DeletePlanner() {}

    public static Plan planDelete(AnalyzedDeleteStatement delete,
                                  SubqueryPlanner subqueryPlanner,
                                  PlannerContext context) {
        Plan plan = planDelete(delete, context);
        return MultiPhasePlan.createIfNeeded(plan, subqueryPlanner.planSubQueries(delete).uncorrelated());
    }

    private static Plan planDelete(AnalyzedDeleteStatement delete, PlannerContext context) {
        DocTableRelation tableRel = delete.relation();
        DocTableInfo table = tableRel.tableInfo();
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(context.nodeContext());
        WhereClauseOptimizer.DetailedQuery detailedQuery = WhereClauseOptimizer.optimize(
            normalizer, delete.query(), table, context.transactionContext(), context.nodeContext(), context.timeoutToken());
        Symbol query = detailedQuery.query();
        if (!detailedQuery.partitions().isEmpty()) {
            // deleting whole partitions is only valid if the query only contains filters based on partition-by cols
            var hasNonPartitionReferences = query.any(s -> s instanceof Reference && table.partitionedByColumns().contains(s) == false);
            if (hasNonPartitionReferences == false) {
                return new DeletePartitions(table.ident(), detailedQuery.partitions());
            }
        }

        if (detailedQuery.docKeys().isPresent() && detailedQuery.queryHasPkSymbolsOnly()) {
            return new DeleteById(tableRel.tableInfo(), detailedQuery.docKeys().get());
        }
        if (table.isPartitioned() && query instanceof Input<?> input && DataTypes.BOOLEAN.sanitizeValue(input.value())) {
            return new DeleteAllPartitions(table.ident());
        }

        return new Delete(tableRel, detailedQuery);
    }

    @VisibleForTesting
    public static class Delete implements Plan {

        private final DocTableRelation table;
        private final WhereClauseOptimizer.DetailedQuery detailedQuery;

        public Delete(DocTableRelation table, WhereClauseOptimizer.DetailedQuery detailedQuery) {
            this.table = table;
            this.detailedQuery = detailedQuery;
        }

        @Override
        public StatementType type() {
            return StatementType.DELETE;
        }

        @Override
        public void executeOrFail(DependencyCarrier dependencies,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  SubQueryResults subQueryResults) {

            WhereClause where = detailedQuery.toBoundWhereClause(
                table.tableInfo(),
                params,
                subQueryResults,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                plannerContext.clusterState().metadata());
            if (!where.partitions().isEmpty()
                && (!where.hasQuery() || Literal.BOOLEAN_TRUE.equals(where.query()))) {
                List<PartitionName> partitionValues = new ArrayList<>(where.partitions().size());
                for (String partition : where.partitions()) {
                    partitionValues.add(new PartitionName(
                        table.relationName(), IndexName.decode(partition).partitionIdent()));
                }
                dependencies.client().execute(
                    TransportDropPartitionsAction.ACTION,
                    new DropPartitionsRequest(table.relationName(), partitionValues)
                ).whenComplete(new OneRowActionListener<>(consumer, ignoredResponse -> Row1.ROW_COUNT_UNKNOWN));
                return;
            }

            ExecutionPlan executionPlan = deleteByQuery(table, plannerContext, where);
            NodeOperationTree nodeOpTree = NodeOperationTreeGenerator.fromPlan(executionPlan, dependencies.localNodeId());
            dependencies.phasesTaskFactory()
                .create(plannerContext.jobId(), Collections.singletonList(nodeOpTree))
                .execute(consumer, plannerContext.transactionContext());
        }

        @Override
        public CompletableFuture<BulkResponse> executeBulk(DependencyCarrier executor,
                                                           PlannerContext plannerContext,
                                                           List<Row> bulkParams,
                                                           SubQueryResults subQueryResults) {
            ArrayList<NodeOperationTree> nodeOperationTreeList = new ArrayList<>(bulkParams.size());
            for (Row params : bulkParams) {
                WhereClause where = detailedQuery.toBoundWhereClause(
                    table.tableInfo(),
                    params,
                    subQueryResults,
                    plannerContext.transactionContext(),
                    executor.nodeContext(),
                    plannerContext.clusterState().metadata());
                ExecutionPlan executionPlan = deleteByQuery(table, plannerContext, where);
                nodeOperationTreeList.add(NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId()));
            }
            return executor.phasesTaskFactory()
                .create(plannerContext.jobId(), nodeOperationTreeList)
                .executeBulk(plannerContext.transactionContext());
        }

        private static ExecutionPlan deleteByQuery(DocTableRelation table, PlannerContext context, WhereClause where) {
            DocTableInfo tableInfo = table.tableInfo();
            Reference idReference = requireNonNull(tableInfo.getReference(SysColumns.ID.COLUMN), "Table has to have a _id reference");
            DeleteProjection deleteProjection = new DeleteProjection(new InputColumn(0, idReference.valueType()));
            var sessionSettings = context.transactionContext().sessionSettings();
            Routing routing = context.allocateRouting(
                tableInfo, where, RoutingProvider.ShardSelection.PRIMARIES, sessionSettings);
            RoutedCollectPhase collectPhase = new RoutedCollectPhase(
                context.jobId(),
                context.nextExecutionPhaseId(),
                "collect",
                routing,
                tableInfo.rowGranularity(),
                tableInfo.isPartitioned(),
                List.of(idReference),
                List.of(deleteProjection),
                Optimizer.optimizeCasts(where.queryOrFallback(), context),
                DistributionInfo.DEFAULT_BROADCAST
            );
            Collect collect = new Collect(collectPhase, LimitAndOffset.NO_LIMIT, 0, 1, 1, null);
            return Merge.ensureOnHandler(collect, context, Collections.singletonList(MergeCountProjection.INSTANCE));
        }
    }
}
