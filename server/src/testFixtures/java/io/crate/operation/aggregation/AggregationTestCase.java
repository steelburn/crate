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

package io.crate.operation.aggregation;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;
import static org.elasticsearch.index.shard.IndexShardTestCase.EMPTY_EVENT_LISTENER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.index.Term;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.IndexResult;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.WhereClause;
import io.crate.common.collections.Lists;
import io.crate.common.io.IOUtils;
import io.crate.concurrent.FutureActionListener;
import io.crate.data.ArrayBucket;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.execution.ddl.tables.MappingUtil.AllocPosition;
import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.Indexer;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.DocValuesAggregates;
import io.crate.execution.engine.collect.MapSideDataCollectOperation;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.memory.MemoryManager;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.StorageSupport;

public abstract class AggregationTestCase extends ESTestCase {

    protected static final RamAccounting RAM_ACCOUNTING = RamAccounting.NO_ACCOUNTING;
    public static final PartitionName PARTITION_NAME = new PartitionName(new RelationName(Schemas.DOC_SCHEMA_NAME, "index"), List.of());

    protected NodeContext nodeCtx;
    protected MemoryManager memoryManager;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        nodeCtx = createNodeContext();
        threadPool = new TestThreadPool(getClass().getName(), Settings.EMPTY);
        memoryManager = new OnHeapMemoryManager(RAM_ACCOUNTING::addBytes);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        } finally {
            super.tearDown();
        }
    }

    public Object executeAggregation(Signature boundSignature,
                                     Object[][] data,
                                     List<Literal<?>> optionalParams) throws Exception {
        return executeAggregation(
            boundSignature,
            boundSignature.getArgumentDataTypes(),
            boundSignature.getReturnType().createType(),
            data,
            true,
            optionalParams
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object executeAggregation(Signature maybeUnboundSignature,
                                     List<DataType<?>> actualArgumentTypes,
                                     DataType<?> actualReturnType,
                                     Object[][] data,
                                     boolean randomExtraStates,
                                     List<Literal<?>> optionalParams) throws Exception {
        var aggregationFunction = (AggregationFunction) nodeCtx.functions().get(
            null,
            maybeUnboundSignature.getName().name(),
            Lists.map(actualArgumentTypes, t -> new InputColumn(0, t)),
            SearchPath.pathWithPGCatalogAndDoc()
        );

        // Lookup the flavor of the aggregation function which operates on partial states and executes the final merge.
        // Especially useful for aggregations like TopkAggregation where the final result is different from the partial
        // states.
        AggregationFunction terminatePartialAggFunction = (AggregationFunction) nodeCtx.functions().getQualified(
            maybeUnboundSignature,
            List.of(aggregationFunction.partialType()),
            actualReturnType);

        // For ArbitraryAggregationTest, where the signature with partial type is not defined
        if (terminatePartialAggFunction == null) {
            terminatePartialAggFunction = aggregationFunction;
        }

        Object partialResultWithoutDocValues = execPartialAggregationWithoutDocValues(
            aggregationFunction,
            data,
            randomExtraStates,
            Version.CURRENT
        );
        for (var argType : actualArgumentTypes) {
            if (argType.storageSupport() == null) {
                return assertAndGetMergedIterAndPartial(
                    aggregationFunction, terminatePartialAggFunction, partialResultWithoutDocValues);
            }
        }
        List<Reference> targetColumns = toReference(actualArgumentTypes);
        var shard = newStartedPrimaryShard(nodeCtx, targetColumns, threadPool);
        var refResolver = new LuceneReferenceResolver(
            PARTITION_NAME.values(),
            List.of(),
            List.of(),
            Version.CURRENT,
            _ -> false
        );

        try {
            insertDataIntoShard(shard, data, targetColumns);
            shard.refresh("test");

            List<Row> partialResultWithDocValues = execPartialAggregationWithDocValues(
                refResolver,
                aggregationFunction.signature(),
                actualArgumentTypes,
                actualReturnType,
                shard,
                optionalParams
            );
            var resultWithoutDocValues = assertAndGetMergedIterAndPartial(
                aggregationFunction, terminatePartialAggFunction, partialResultWithoutDocValues);

            // assert that aggregations with/-out doc values yield the
            // same result, if a doc value aggregator exists.
            if (partialResultWithDocValues != null) {
                assertThat(partialResultWithDocValues).hasSize(1);
                var resultWithDocValues = assertAndGetMergedIterAndPartial(
                    aggregationFunction, terminatePartialAggFunction, partialResultWithDocValues.get(0).get(0));

                assertThat(resultWithoutDocValues).isEqualTo(resultWithDocValues);
            } else {
                var docValueAggregator = aggregationFunction.getDocValueAggregator(
                    refResolver,
                    targetColumns,
                    mock(DocTableInfo.class),
                    Version.CURRENT,
                    List.of()
                );
                if (docValueAggregator != null) {
                    throw new IllegalStateException("DocValueAggregator is implemented but partialResultWithDocValues is null.");
                }
            }
            return resultWithoutDocValues;
        } finally {
            closeShard(shard);
        }
    }

    private static <T> Object assertAndGetMergedIterAndPartial(AggregationFunction<T, ?> aggregationFunction,
                                                               AggregationFunction<T ,?> terminatePartialAggFunction,
                                                               T partialResultWithoutDocValues) {
        Object result1 = terminatePartialAggFunction.terminatePartial(
            RAM_ACCOUNTING,
            partialResultWithoutDocValues
        );
        Object result2 = aggregationFunction.terminatePartial(
            RAM_ACCOUNTING,
            partialResultWithoutDocValues
        );
        assertThat(result2).as("iter->final should have the same result as partial->final")
            .isEqualTo(result1);
        return result1;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Object execPartialAggregationWithoutDocValues(AggregationFunction function,
                                                            Object[][] data,
                                                            boolean randomExtraStates,
                                                            Version minNodeVersion) {
        var argumentsSize = function.signature().getArgumentTypes().size();
        RowCollectExpression[] inputs = new RowCollectExpression[argumentsSize];
        for (int i = 0; i < argumentsSize; i++) {
            inputs[i] = new RowCollectExpression(i);
        }

        ArrayList<Object> states = new ArrayList<>();
        states.add(function.newState(RAM_ACCOUNTING, minNodeVersion, memoryManager));
        for (Row row : new ArrayBucket(data)) {
            for (RowCollectExpression input : inputs) {
                input.setNextRow(row);
            }
            if (randomExtraStates && randomIntBetween(1, 4) == 1) {
                states.add(function.newState(RAM_ACCOUNTING, minNodeVersion, memoryManager));
            }
            int idx = states.size() - 1;
            states.set(idx, function.iterate(RAM_ACCOUNTING, memoryManager, states.get(idx), inputs));
        }
        Object state = states.get(0);
        for (int i = 1; i < states.size(); i++) {
            state = function.reduce(RAM_ACCOUNTING, state, states.get(i));
        }
        return state;
    }

    @Nullable
    private List<Row> execPartialAggregationWithDocValues(LuceneReferenceResolver refResolver,
                                                          Signature signature,
                                                          List<DataType<?>> argumentTypes,
                                                          DataType<?> actualReturnType,
                                                          IndexShard shard,
                                                          List<Literal<?>> optionalParams) throws Exception {
        // Make sure optional parameters do not become references
        List<Symbol> inputs = InputColumn.mapToInputColumns(argumentTypes.subList(0, argumentTypes.size() - optionalParams.size()));
        inputs.addAll(optionalParams);
        var aggregation = new Aggregation(
            signature,
            actualReturnType,
            inputs
        );
        var toCollectRefs = new ArrayList<Symbol>(argumentTypes.size());
        for (int i = 0; i < argumentTypes.size(); i++) {
            ReferenceIdent ident = new ReferenceIdent(PARTITION_NAME.relationName(), Integer.toString(i));
            toCollectRefs.add(
                new SimpleReference(
                    ident,
                    RowGranularity.DOC,
                    argumentTypes.get(i),
                    IndexType.PLAIN,
                    true,
                    true,
                    i + 1,
                    COLUMN_OID_UNASSIGNED,
                    false,
                    null
                )
            );
        }
        var projections = List.of(
            new AggregationProjection(
                List.of(aggregation),
                RowGranularity.SHARD,
                AggregateMode.ITER_PARTIAL
            )
        );
        var collectPhase = createCollectPhase(toCollectRefs, projections);
        var collectTask = createCollectTask(shard, collectPhase, Version.CURRENT);
        var batchIterator = DocValuesAggregates.tryOptimize(
            nodeCtx.functions(),
            refResolver,
            shard,
            mock(DocTableInfo.class),
            PARTITION_NAME.values(),
            new LuceneQueryBuilder(nodeCtx),
            collectPhase,
            collectTask
        );
        List<Row> result;
        if (batchIterator != null) {
            result = batchIterator.toList().get();
        } else {
            result = null;
        }
        // required to close searchers
        collectTask.kill(new InterruptedException("kill"));
        return result;

    }

    private void insertDataIntoShard(IndexShard shard,
                                     Object[][] data,
                                     List<Reference> targetColumns) throws IOException {
        DocTableInfo table = new DocTableInfo(
            PARTITION_NAME.relationName(),
            targetColumns.stream().collect(Collectors.toMap(Reference::column, r -> r)),
            Map.of(),
            Set.of(),
            null,
            List.of(),
            List.of(),
            null,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .build(),
            List.of(),
            ColumnPolicy.STRICT,
            Version.CURRENT,
            null,
            false,
            Set.of(),
            0
        );
        Indexer indexer = new Indexer(
            PARTITION_NAME.values(),
            table,
            Version.CURRENT,
            CoordinatorTxnCtx.systemTransactionContext(),
            nodeCtx,
            targetColumns,
            null
        );

        final long startTime = System.nanoTime();
        for (Object[] row : data) {
            String id = UUIDs.randomBase64UUID();
            for (int i = 0; i < row.length; i++) {
                row[i] = targetColumns.get(i).valueType().implicitCast(row[i]);
            }
            IndexItem.StaticItem item = new IndexItem.StaticItem(id, List.of(id), row, 1, 1);
            ParsedDocument parsedDoc = indexer.index(item);
            Term uid = new Term(SysColumns.Names.ID, Uid.encodeId(item.id()));
            Engine.Index index = new Engine.Index(
                uid,
                parsedDoc,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                shard.getOperationPrimaryTerm(),
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                startTime,
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM
            );
            IndexResult result = shard.index(index);
            assertThat(result.getResultType()).isIn(
                Engine.Result.Type.MAPPING_UPDATE_REQUIRED,
                Engine.Result.Type.SUCCESS
            );
        }
    }

    private static XContentBuilder buildMapping(List<Reference> targetColumns) throws IOException {
        Map<String, Map<String, Object>> properties = MappingUtil.toProperties(
            AllocPosition.forNewTable(),
            Reference.buildTree(targetColumns)
        );
        return JsonXContent.builder()
            .startObject()
            .field("properties", properties == null ? Map.of() : properties)
            .endObject();
    }

    /**
     * Creates a new empty primary shard and starts it.
     */
    public static IndexShard newStartedPrimaryShard(NodeContext nodeCtx,
                                                    List<Reference> targetColumns,
                                                    ThreadPool threadPool) throws Exception {
        var mapping = buildMapping(targetColumns);
        IndexShard shard = newPrimaryShard(nodeCtx, mapping, threadPool);
        shard.markAsRecovering(
            "store",
            new RecoveryState(
                shard.routingEntry(),
                new DiscoveryNode(
                    shard.routingEntry().currentNodeId(),
                    shard.routingEntry().currentNodeId(),
                    buildNewFakeTransportAddress(),
                    Map.of(),
                    DiscoveryNodeRole.BUILT_IN_ROLES,
                    Version.CURRENT),
                null)
        );

        FutureActionListener<Boolean> future = new FutureActionListener<>();
        shard.recoverFromStore(future);
        future.get(5, TimeUnit.SECONDS);

        var newRouting = ShardRoutingHelper.moveToStarted(shard.routingEntry());
        var newRoutingTable = new IndexShardRoutingTable.Builder(newRouting.shardId())
            .addShard(newRouting)
            .build();
        assert newRouting.allocationId() != null;
        shard.updateShardState(
            newRouting,
            shard.getPendingPrimaryTerm(),
            null,
            0,
            Set.of(newRouting.allocationId().getId()),
            newRoutingTable
        );
        return shard;
    }

    /**
     * Creates a new initializing primary shard.
     * The shard will have its own unique data path.
     */
    private static IndexShard newPrimaryShard(NodeContext nodeCtx, XContentBuilder mapping, ThreadPool threadPool) throws IOException {
        ShardRouting routing = TestShardRouting.newShardRouting(
            new ShardId(PARTITION_NAME.relationName().indexNameOrAlias(), UUIDs.base64UUID(), 0),
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(Settings.EMPTY)
            .build();
        var indexMetadata = IndexMetadata.builder(routing.getIndexUUID())
            .indexName(routing.index().getName())
            .settings(indexSettings)
            .primaryTerm(0, 1)
            .putMapping(Strings.toString(mapping))
            .build();
        var shardId = routing.shardId();
        var nodePath = new NodeEnvironment.NodePath(createTempDir());
        var shardPath = new ShardPath(
            false,
            nodePath.resolve(shardId),
            nodePath.resolve(shardId),
            shardId
        );
        return newPrimaryShard(nodeCtx, routing, shardPath, indexMetadata, threadPool);
    }

    private static IndexShard newPrimaryShard(NodeContext nodeCtx,
                                              ShardRouting routing,
                                              ShardPath shardPath,
                                              IndexMetadata indexMetadata,
                                              ThreadPool threadPool) throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();
        var indexSettings = new IndexSettings(indexMetadata, settings);
        var queryCache = DisabledQueryCache.instance();
        var store = new Store(
            shardPath.getShardId(),
            indexSettings,
            newFSDirectory(shardPath.resolveIndex()),
            new DummyShardLock(shardPath.getShardId())
        );
        TestAnalysis testAnalysis = createTestAnalysis(indexSettings, indexSettings.getSettings());
        IndexShard shard = null;
        try {
            shard = new IndexShard(
                nodeCtx,
                routing,
                indexSettings,
                shardPath,
                store,
                queryCache,
                testAnalysis.indexAnalyzers.getDefaultIndexAnalyzer(),
                () -> null,
                List.of(),
                EMPTY_EVENT_LISTENER,
                threadPool,
                BigArrays.NON_RECYCLING_INSTANCE,
                List.of(),
                () -> { },
                RetentionLeaseSyncer.EMPTY, new NoneCircuitBreakerService()
            );
        } catch (IOException e) {
            IOUtils.close(store);
            fail("cannot create a new shard", e);
        }
        return shard;
    }

    public static void closeShard(IndexShard shard) throws IOException {
        if (shard != null) {
            IOUtils.close(() -> shard.close("test", false), shard.store());
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Symbol normalize(String functionName, Object value, DataType type) {
        return normalize(functionName, Literal.of(type, value));
    }

    protected Symbol normalize(String functionName, Symbol... args) {
        List<Symbol> arguments = Arrays.asList(args);
        AggregationFunction<?, ?> function = (AggregationFunction<?, ?>) nodeCtx.functions().get(
            null,
            functionName,
            arguments,
            SearchPath.pathWithPGCatalogAndDoc()
        );
        return function.normalizeSymbol(
            new Function(function.signature(), arguments, function.partialType()),
            new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults()),
            nodeCtx
        );
    }

    public void assertHasDocValueAggregator(String functionName, List<DataType<?>> argumentTypes) {
        var aggregationFunction = (AggregationFunction<?, ?>) nodeCtx.functions().get(
            null,
            functionName,
            InputColumn.mapToInputColumns(argumentTypes),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        var docValueAggregator = aggregationFunction.getDocValueAggregator(
            mock(LuceneReferenceResolver.class),
            toReference(argumentTypes),
            mock(DocTableInfo.class),
            Version.CURRENT,
            List.of()
        );
        assertThat(docValueAggregator)
                .as("DocValueAggregator is not implemented for "
                    + functionName + "(" + argumentTypes.stream()
                        .map(DataType::toString)
                        .collect(Collectors.joining(", ")) + ")")
            .isNotNull();
    }

    public static List<Reference> toReference(List<DataType<?>> dataTypes) {
        var references = new ArrayList<Reference>(dataTypes.size());
        for (int i = 0; i < dataTypes.size(); i++) {
            DataType<?> type = dataTypes.get(i);
            StorageSupport<?> storageSupport = type.storageSupportSafe();
            references.add(
                new SimpleReference(
                    new ReferenceIdent(PARTITION_NAME.relationName(), Integer.toString(i)),
                    RowGranularity.DOC,
                    type,
                    IndexType.PLAIN,
                    true,
                    storageSupport.docValuesDefault(),
                    i + 1,
                    COLUMN_OID_UNASSIGNED,
                    false,
                    null)
            );
        }
        return references;
    }

    public static CollectTask createCollectTask(IndexShard shard, CollectPhase collectPhase, Version minNodeVersion) {
        var indexServices = mock(IndicesService.class);
        var indexService = mock(IndexService.class);
        when(indexService.getShard(shard.shardId().id()))
            .thenReturn(shard);
        when(indexServices.indexServiceSafe(shard.routingEntry().index()))
            .thenReturn(indexService);
        return new CollectTask(
            collectPhase,
            CoordinatorTxnCtx.systemTransactionContext(),
            mock(MapSideDataCollectOperation.class),
            RAM_ACCOUNTING,
            ramAccounting -> new OnHeapMemoryManager(ramAccounting::addBytes),
            new TestingRowConsumer(),
            new SharedShardContexts(indexServices, (ignored, searcher) -> searcher),
            minNodeVersion,
            4096
        );
    }

    public static RoutedCollectPhase createCollectPhase(List<Symbol> toCollectRefs,
                                                        Collection<? extends Projection> projections) {
        return new RoutedCollectPhase(
            UUID.randomUUID(),
            1,
            "collect",
            new Routing(Map.of()),
            RowGranularity.SHARD,
            false,
            toCollectRefs,
            projections,
            WhereClause.MATCH_ALL.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST
        );
    }
}
