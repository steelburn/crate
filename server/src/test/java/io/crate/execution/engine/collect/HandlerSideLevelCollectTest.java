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

package io.crate.execution.engine.collect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;

@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class HandlerSideLevelCollectTest extends IntegTestCase {

    private MapSideDataCollectOperation operation;
    private Functions functions;
    private final RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList());
    private final TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Before
    public void prepare() {
        operation = cluster().getDataNodeInstance(MapSideDataCollectOperation.class);
        functions = cluster().getInstance(Functions.class);
    }

    private RoutedCollectPhase collectNode(Routing routing,
                                           List<Symbol> toCollect,
                                           RowGranularity rowGranularity,
                                           WhereClause whereClause) {
        return new RoutedCollectPhase(
            UUID.randomUUID(),
            0,
            "dummy",
            routing,
            rowGranularity,
            false,
            toCollect,
            List.of(),
            whereClause.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST
        );
    }

    private RoutedCollectPhase collectNode(Routing routing, List<Symbol> toCollect, RowGranularity rowGranularity) {
        return collectNode(routing, toCollect, rowGranularity, WhereClause.MATCH_ALL);
    }

    @Test
    public void testClusterLevel() throws Exception {
        Schemas schemas = cluster().getInstance(NodeContext.class).schemas();
        TableInfo tableInfo = schemas.getTableInfo(new RelationName("sys", "cluster"));
        Routing routing = tableInfo.getRouting(
            clusterService().state(),
            routingProvider,
            WhereClause.MATCH_ALL, RoutingProvider.ShardSelection.ANY, CoordinatorSessionSettings.systemDefaults());
        SimpleReference clusterNameRef = new SimpleReference(
            new ReferenceIdent(SysClusterTableInfo.IDENT, ColumnIdent.of("name")),
            RowGranularity.CLUSTER,
            DataTypes.STRING,
            1,
            null
        );
        RoutedCollectPhase collectNode = collectNode(routing, List.of(clusterNameRef), RowGranularity.CLUSTER);
        Bucket result = collect(collectNode);
        assertThat(result).hasSize(1);
        assertThat(((String) result.iterator().next().get(0))).startsWith("SUITE-");
    }

    private Bucket collect(RoutedCollectPhase collectPhase) throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        CollectTask collectTask = mock(CollectTask.class);
        when(collectTask.txnCtx()).thenReturn(txnCtx);
        BatchIterator<Row> bi = operation.createIterator(txnCtx, collectPhase, consumer.requiresScroll(), collectTask).get(5, TimeUnit.SECONDS);
        consumer.accept(bi, null);
        return new CollectionBucket(consumer.getResult());
    }

    @Test
    public void testInformationSchemaTables() throws Exception {
        InformationSchemaInfo schemaInfo = cluster().getInstance(InformationSchemaInfo.class);
        TableInfo tablesTableInfo = schemaInfo.getTableInfo("tables");
        Routing routing = tablesTableInfo.getRouting(
            clusterService().state(),
            routingProvider,
            WhereClause.MATCH_ALL, RoutingProvider.ShardSelection.ANY, CoordinatorSessionSettings.systemDefaults());
        List<Symbol> toCollect = new ArrayList<>();
        for (Reference reference : tablesTableInfo.rootColumns()) {
            toCollect.add(reference);
        }
        Symbol tableNameRef = toCollect.get(12);

        List<Symbol> arguments = Arrays.asList(tableNameRef, Literal.of("shards"));
        FunctionImplementation eqImpl
            = functions.get(null, EqOperator.NAME, arguments, SearchPath.pathWithPGCatalogAndDoc());
        Function whereClause = new Function(eqImpl.signature(), arguments, EqOperator.RETURN_TYPE);

        RoutedCollectPhase collectNode = collectNode(routing, toCollect, RowGranularity.DOC, new WhereClause(whereClause));
        Bucket result = collect(collectNode);
        assertThat(TestingHelpers.printedTable(result)).isEqualTo("NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| shards| sys| BASE TABLE| NULL\n");
    }

    @Test
    public void testInformationSchemaColumns() throws Exception {
        InformationSchemaInfo schemaInfo = cluster().getInstance(InformationSchemaInfo.class);
        TableInfo tableInfo = schemaInfo.getTableInfo("columns");
        assert tableInfo != null;
        Routing routing = tableInfo.getRouting(
            clusterService().state(),
            routingProvider,
            WhereClause.MATCH_ALL, RoutingProvider.ShardSelection.ANY, CoordinatorSessionSettings.systemDefaults());
        List<Symbol> toCollect = new ArrayList<>();
        for (Reference ref : tableInfo.rootColumns()) {
            if (Set.of("column_name", "data_type", "table_name").contains(ref.column().name())) {
                toCollect.add(ref);
            }
        }
        RoutedCollectPhase collectNode = collectNode(routing, toCollect, RowGranularity.DOC);
        List<Object[]> result = StreamSupport
            .stream(collect(collectNode).spliterator(), false)
            .limit(10)
            .map(Row::materialize)
            .toList();

        String expected = """
            grantee| text| administrable_role_authorizations
            is_grantable| boolean| administrable_role_authorizations
            role_name| text| administrable_role_authorizations
            grantee| text| applicable_roles
            is_grantable| boolean| applicable_roles
            role_name| text| applicable_roles
            character_repertoire| text| character_sets
            character_set_catalog| text| character_sets
            character_set_name| text| character_sets
            character_set_schema| text| character_sets
            """;


        assertThat(TestingHelpers.printedTable(result.toArray(new Object[0][]))).contains(expected);

        // second time - to check if the internal iterator resets
        result = StreamSupport
            .stream(collect(collectNode).spliterator(), false)
            .limit(10)
            .map(Row::materialize)
            .toList();
        assertThat(TestingHelpers.printedTable(result.toArray(new Object[0][]))).contains(expected);
    }
}
