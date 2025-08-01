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

package io.crate.testing;

import static io.crate.blob.v2.BlobIndex.fullIndexName;
import static io.crate.testing.DiscoveryNodes.newFakeAddress;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_CLOSED_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.Metadata.Builder.NO_OID_COLUMN_OID_SUPPLIER;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;
import org.jetbrains.annotations.Nullable;
import org.mockito.Answers;

import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedCreateBlobTable;
import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.Analyzer;
import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.CreateBlobTableAnalyzer;
import io.crate.analyze.CreateTableStatementAnalyzer;
import io.crate.analyze.NumberOfShards;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.TableParameters;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.auth.Protocol;
import io.crate.common.collections.Lists;
import io.crate.common.collections.MapBuilder;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.ddl.RepositoryService;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.udf.UDFLanguage;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.fdw.AddForeignTableTask;
import io.crate.fdw.AddServerTask;
import io.crate.fdw.CreateServerRequest;
import io.crate.fdw.FdwAnalyzer;
import io.crate.fdw.ForeignDataWrappers;
import io.crate.lucene.CrateLuceneTestCase;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.planner.CreateForeignTablePlan;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.CreateBlobTablePlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.optimizer.LoadedRules;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.protocols.postgres.TransactionState;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.LogicalReplicationSettings;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.role.Role;
import io.crate.role.RoleManager;
import io.crate.role.StubRoleManager;
import io.crate.session.Cursors;
import io.crate.session.Session;
import io.crate.session.Sessions;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CreateBlobTable;
import io.crate.sql.tree.CreateForeignTable;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;

/**
 * Lightweight alternative to {@link SQLTransportExecutor}.
 *
 * Can be used for unit-tests tests which don't require the full execution-layer/nodes to be started.
 */
public class SQLExecutor {

    public final Sessions sqlOperations;
    public final Analyzer analyzer;
    public Planner planner;
    private final RelationAnalyzer relAnalyzer;
    private final CoordinatorSessionSettings sessionSettings;
    private final CoordinatorTxnCtx coordinatorTxnCtx;
    public final NodeContext nodeCtx;
    private final Random random;
    private final Schemas schemas;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final UserDefinedFunctionService udfService;
    public final Cursors cursors = new Cursors();
    public final JobsLogs jobsLogs;
    public final DependencyCarrier dependencyMock;
    private final PlanStats planStats;
    private final TableStats tableStats;
    public final ForeignDataWrappers foreignDataWrappers;
    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final CreateBlobTableAnalyzer createBlobTableAnalyzer;
    private final CreateTableStatementAnalyzer createTableAnalyzer;

    public TransactionState transactionState = TransactionState.IDLE;
    public boolean jobsLogsEnabled;

    @Nullable
    private LongSupplier columnOidSupplier;
    private Role user = Role.CRATE_USER;
    private String[] searchPath = new String[] { Schemas.DOC_SCHEMA_NAME };


    /**
     * Shortcut for {@link #getPlannerContext(Random)}
     * This can only be used if {@link com.carrotsearch.randomizedtesting.RandomizedContext} is available
     * (e.g. TestCase using {@link com.carrotsearch.randomizedtesting.RandomizedRunner}
     */
    public PlannerContext getPlannerContext() {
        return getPlannerContext(Randomness.get());
    }

    public PlannerContext getPlannerContext(Random random) {
        return planner.createContext(
            new RoutingProvider(random.nextInt(), emptyList()),
            UUID.randomUUID(),
            new CoordinatorTxnCtx(sessionSettings),
            -1,
            null,
            cursors,
            transactionState,
            Session.TimeoutToken.noopToken()
        );
    }

    private static void addNodesToClusterState(ClusterService clusterService, int numNodes) {
        ClusterState prevState = clusterService.state();
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder(prevState.nodes());
        for (int i = 1; i <= numNodes; i++) {
            if (builder.get("n" + i) == null) {
                builder.add(new DiscoveryNode("n" + i, newFakeAddress(), Version.CURRENT));
            }
        }
        builder.localNodeId("n1");
        builder.masterNodeId("n1");
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(prevState).nodes(builder).build());
    }

    /**
     * Publish the current {@link ClusterState} so new instances of
     * {@link org.elasticsearch.cluster.ClusterStateListener} like e.g. {@link Schemas} will consume it and
     * build current schema/table infos.
     *
     * The {@link Metadata#version()} must be increased to trigger a {@link ClusterChangedEvent#metadataChanged()}.
     */
    private static void publishInitialClusterState(ClusterService clusterService) {
        ClusterState currentState = clusterService.state();
        ClusterState newState = ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata())
                            .version(currentState.metadata().version() + 1)
                            .build())
            .build();
        ClusterServiceUtils.setState(clusterService, newState);
    }

    public static class Builder {

        private final ClusterService clusterService;

        private int numNodes = 1;
        private Planner planner;
        private RoleManager roleManager = new StubRoleManager();
        private List<AnalysisPlugin> analysisPlugins = List.of();

        private Builder(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        public Builder setNumNodes(int numNodes) {
            this.numNodes = numNodes;
            return this;
        }

        public Builder setPlanner(Planner planner) {
            this.planner = planner;
            return this;
        }

        public Builder setRoleManager(RoleManager roleManager) {
            this.roleManager = roleManager;
            return this;
        }

        public Builder setAnalysisPlugins(List<AnalysisPlugin> analysisPlugins) {
            this.analysisPlugins = analysisPlugins;
            return this;
        }

        public SQLExecutor build() {
            if (numNodes < 1) {
                throw new IllegalArgumentException("Must have at least 1 node");
            }
            addNodesToClusterState(clusterService, numNodes);
            final Settings settings = Settings.EMPTY;
            var tableStats = new TableStats();
            var sessionSettingRegistry = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE));
            AtomicReference<RelationAnalyzer> relationAnalyzerRef = new AtomicReference<>(null);
            Path homeDir = CrateLuceneTestCase.createTempDir();
            Environment environment = new Environment(
                Settings.builder().put(PATH_HOME_SETTING.getKey(), homeDir.toAbsolutePath()).build(),
                homeDir.resolve("config")
            );
            var nodeCtx = NodeContext.of(
                environment,
                clusterService,
                Functions.load(settings, sessionSettingRegistry),
                roleManager,
                tableStats
            );
            var udfService = new UserDefinedFunctionService(clusterService, nodeCtx);

            var relationAnalyzer = new RelationAnalyzer(nodeCtx);
            relationAnalyzerRef.set(relationAnalyzer);
            AnalysisRegistry analysisRegistry;
            try {
                analysisRegistry = new AnalysisModule(environment, analysisPlugins).getAnalysisRegistry();
            } catch (IOException e) {
                udfService.close();
                throw new UncheckedIOException(e);
            }
            var fulltextAnalyzerResolver = new FulltextAnalyzerResolver(clusterService, analysisRegistry);
            var allocationService = new AllocationService(
                new AllocationDeciders(
                    Arrays.asList(
                        new SameShardAllocationDecider(settings, clusterService.getClusterSettings()),
                        new ReplicaAfterPrimaryActiveAllocationDecider()
                    )
                ),
                new TestGatewayAllocator(),
                new BalancedShardsAllocator(settings),
                EmptyClusterInfoService.INSTANCE,
                EmptySnapshotsInfoService.INSTANCE
            );
            var threadPool = mock(ThreadPool.class);
            var logicalReplicationSettings = new LogicalReplicationSettings(settings, clusterService);
            NodeClient client = new NodeClient();
            MetadataUpgradeService metadataUpgradeService = new MetadataUpgradeService(nodeCtx, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, udfService);
            var logicalReplicationService = new LogicalReplicationService(
                settings,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                clusterService,
                mock(RemoteClusters.class),
                threadPool,
                client,
                allocationService,
                metadataUpgradeService,
                logicalReplicationSettings
            );
            logicalReplicationService.repositoriesService(mock(RepositoriesService.class));
            var foreignDataWrappers = new ForeignDataWrappers(settings, clusterService, nodeCtx);

            publishInitialClusterState(clusterService);
            return new SQLExecutor(
                clusterService,
                allocationService,
                nodeCtx,
                new Analyzer(
                    nodeCtx,
                    relationAnalyzer,
                    clusterService,
                    analysisRegistry,
                    new RepositoryService(clusterService, client),
                    roleManager,
                    sessionSettingRegistry,
                    logicalReplicationService
                ),
                planner != null
                    ? planner
                    : new Planner(
                        settings,
                        clusterService,
                        nodeCtx,
                        null,
                        null,
                        roleManager,
                        foreignDataWrappers,
                        sessionSettingRegistry
                    ),
                relationAnalyzer,
                sessionSettingRegistry,
                new CoordinatorSessionSettings(Role.CRATE_USER, Role.CRATE_USER, LoadedRules.INSTANCE.disabledRules()),
                nodeCtx.schemas(),
                Randomness.get(),
                fulltextAnalyzerResolver,
                udfService,
                tableStats,
                foreignDataWrappers
            );
        }
    }

    public static SQLExecutor.Builder builder(ClusterService clusterService) {
        return new Builder(clusterService);
    }

    public static SQLExecutor of(ClusterService clusterService) {
        return new Builder(clusterService).build();
    }

    /**
     * This will build a cluster state containing the given table definition before returning a fully resolved
     * {@link DocTableInfo}.
     *
     * <p>Building a cluster state is a rather expensive operation thus this method should NOT be used when multiple
     * tables are needed (e.g. inside a loop) but instead building the cluster state once containing all tables
     * using {@link #builder(ClusterService)} and afterwards resolve all table infos manually.</p>
     */
    public static DocTableInfo tableInfo(RelationName name, String stmt, ClusterService clusterService) {
        try {
            SQLExecutor executor = builder(clusterService)
                .build()
                .addTable(stmt);
            return executor.schemas().getTableInfo(name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Variant of {@link #tableInfo(RelationName, String, ClusterService)} for partitioned tables.
     */
    public static DocTableInfo partitionedTableInfo(RelationName name, String stmt, ClusterService clusterService) {
        try {
            SQLExecutor executor = builder(clusterService)
                .build()
                .addTable(stmt);
            return executor.schemas().getTableInfo(name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SQLExecutor(ClusterService clusterService,
                        AllocationService allocationService,
                        NodeContext nodeCtx,
                        Analyzer analyzer,
                        Planner planner,
                        RelationAnalyzer relAnalyzer,
                        SessionSettingRegistry sessionSettingRegistry,
                        CoordinatorSessionSettings sessionSettings,
                        Schemas schemas,
                        Random random,
                        FulltextAnalyzerResolver fulltextAnalyzerResolver,
                        UserDefinedFunctionService udfService,
                        TableStats tableStats,
                        ForeignDataWrappers foreignDataWrappers) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.jobsLogsEnabled = false;
        this.jobsLogs = new JobsLogs(() -> SQLExecutor.this.jobsLogsEnabled);
        this.dependencyMock = mock(DependencyCarrier.class, Answers.RETURNS_MOCKS);
        when(dependencyMock.clusterService()).thenReturn(clusterService);
        when(dependencyMock.schemas()).thenReturn(schemas);
        this.sqlOperations = new Sessions(
            nodeCtx,
            analyzer,
            planner,
            () -> dependencyMock,
            jobsLogs,
            clusterService.getSettings(),
            clusterService,
            sessionSettingRegistry
        );
        this.analyzer = analyzer;
        this.planner = planner;
        this.relAnalyzer = relAnalyzer;
        this.sessionSettings = sessionSettings;
        this.coordinatorTxnCtx = new CoordinatorTxnCtx(sessionSettings);
        this.nodeCtx = nodeCtx;
        this.schemas = schemas;
        this.random = random;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        this.udfService = udfService;
        this.tableStats = tableStats;
        this.planStats = new PlanStats(nodeCtx, coordinatorTxnCtx, tableStats);
        this.foreignDataWrappers = foreignDataWrappers;
        this.createTableAnalyzer = new CreateTableStatementAnalyzer(nodeCtx);
        this.createBlobTableAnalyzer = new CreateBlobTableAnalyzer(schemas, nodeCtx);
    }

    public FulltextAnalyzerResolver fulltextAnalyzerResolver() {
        return fulltextAnalyzerResolver;
    }

    public UserDefinedFunctionService udfService() {
        return udfService;
    }

    @SuppressWarnings("unchecked")
    public <T extends AnalyzedStatement> T analyze(String stmt, ParamTypeHints typeHints) {
        return (T) analyzer.analyze(
            SqlParser.createStatement(stmt),
            coordinatorTxnCtx.sessionSettings(),
            typeHints,
            cursors
        );
    }

    public <T extends AnalyzedStatement> T analyze(String statement) {
        return analyze(statement, ParamTypeHints.EMPTY);
    }

    /**
     * Convert a expression to a symbol
     * If tables are used here they must also be registered in the SQLExecutor having used {@link Builder#addTable(String, String...)}
     */
    public Symbol asSymbol(String expression) {
        MapBuilder<RelationName, AnalyzedRelation> sources = MapBuilder.newMapBuilder();
        for (SchemaInfo schemaInfo : schemas) {
            for (TableInfo tableInfo : schemaInfo.getTables()) {
                if (tableInfo instanceof DocTableInfo) {
                    RelationName relationName = tableInfo.ident();
                    sources.put(relationName, new DocTableRelation(schemas.getTableInfo(relationName)));
                }
            }
        }
        CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(sessionSettings);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            coordinatorTxnCtx,
            nodeCtx,
            ParamTypeHints.EMPTY,
            new FullQualifiedNameFieldProvider(
                sources.immutableMap(),
                ParentRelations.NO_PARENTS,
                sessionSettings.searchPath().currentSchema()
            ),
            new SubqueryAnalyzer(
                relAnalyzer,
                new StatementAnalysisContext(ParamTypeHints.EMPTY, Operation.READ, coordinatorTxnCtx)
            )
        );
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext(coordinatorTxnCtx.sessionSettings());
        return expressionAnalyzer.convert(
            SqlParser.createExpression(expression), expressionAnalysisContext);
    }

    public <T> T plan(String statement, UUID jobId, int fetchSize) {
        return plan(statement, jobId, fetchSize, Row.EMPTY);
    }

    public <T> T plan(String statement, UUID jobId, int fetchSize, Row params) {
        AnalyzedStatement analyzedStatement = analyze(statement, ParamTypeHints.EMPTY);
        return planInternal(analyzedStatement, jobId, fetchSize, params);
    }

    @SuppressWarnings("unchecked")
    private <T> T planInternal(AnalyzedStatement analyzedStatement, UUID jobId, int fetchSize, Row params) {
        RoutingProvider routingProvider = new RoutingProvider(random.nextInt(), emptyList());
        PlannerContext plannerContext = planner.createContext(
            routingProvider,
            jobId,
            coordinatorTxnCtx,
            fetchSize,
            null,
            cursors,
            transactionState,
            Session.TimeoutToken.noopToken()
        );
        Plan plan = planner.plan(analyzedStatement, plannerContext);
        if (plan instanceof LogicalPlan logicalPlan) {
            return (T) logicalPlan.build(
                dependencyMock,
                plannerContext,
                Set.of(),
                new ProjectionBuilder(nodeCtx),
                LimitAndOffset.NO_LIMIT,
                0,
                null,
                null,
                params,
                new SubQueryResults(emptyMap()) {
                    @Override
                    public Object getSafe(SelectSymbol key) {
                        return null;
                    }
                }
            );
        }
        return (T) plan;
    }

    public <T> T buildPlan(LogicalPlan logicalPlan) {
        return (T) logicalPlan.build(
            dependencyMock,
            getPlannerContext(),
            Set.of(),
            new ProjectionBuilder(nodeCtx),
            LimitAndOffset.NO_LIMIT,
            0,
            null,
            null,
            Row.EMPTY,
            new SubQueryResults(emptyMap()) {
                @Override
                public Object getSafe(SelectSymbol key) {
                    return null;
                }
            }
        );
    }

    @SuppressWarnings("unchecked")
    public <T extends LogicalPlan> T logicalPlan(String statement) {
        AnalyzedStatement stmt = analyze(statement, ParamTypeHints.EMPTY);
        return (T) planner.plan(stmt, getPlannerContext());
    }

    public <T> T plan(String statement) {
        return plan(statement, UUID.randomUUID(), 0);
    }

    public TestingRowConsumer execute(String statement, Object ... params) throws Exception {
        return execute((Plan) plan(statement), params);
    }

    public TestingRowConsumer execute(Plan plan, Object ... params) throws Exception {
        var consumer = new TestingRowConsumer();
        plan.execute(
            dependencyMock,
            getPlannerContext(),
            consumer,
            new RowN(params),
            SubQueryResults.EMPTY
        );
        return consumer;
    }

    public void updateTableStats(Map<RelationName, Stats> stats) {
        tableStats.updateTableStats(stats::get);
    }

    public PlanStats planStats() {
        return planStats;
    }

    public Stats getStats(LogicalPlan logicalPlan) {
        return planStats.get(logicalPlan);
    }

    public Schemas schemas() {
        return schemas;
    }

    public CoordinatorSessionSettings getSessionSettings() {
        return sessionSettings;
    }

    public <T extends RelationInfo> T resolveTableInfo(String tableName) {
        IndexParts indexParts = IndexName.decode(tableName);
        QualifiedName qualifiedName = QualifiedName.of(indexParts.schema(), indexParts.table());
        return schemas.findRelation(
            qualifiedName,
            Operation.READ,
            sessionSettings.sessionUser(),
            sessionSettings.searchPath()
        );
    }

    public Session createSession() {
        return sqlOperations.newSession(
            new ConnectionProperties(null, null, Protocol.HTTP, null),
            sessionSettings.currentSchema(),
            sessionSettings.authenticatedUser()
        );
    }

    private static IndexMetadata.Builder getIndexMetadata(String indexName,
                                                            Settings settings,
                                                            @Nullable Map<String, Object> mapping,
                                                            Version smallestNodeVersion) throws IOException {
        Settings indexSettings = buildSettings(settings, smallestNodeVersion);
        IndexMetadata.Builder metaBuilder = IndexMetadata.builder(UUIDs.randomBase64UUID())
            .settings(indexSettings)
            .indexName(indexName);
        if (mapping != null) {
            metaBuilder.putMapping(new MappingMetadata(mapping));
        }

        return metaBuilder;
    }

    private static Settings buildSettings(Settings settings, Version smallestNodeVersion) {
        Settings.Builder builder = Settings.builder()
            .put(settings);
        if (settings.get(SETTING_VERSION_CREATED) == null) {
            builder.put(SETTING_VERSION_CREATED, smallestNodeVersion);
        }
        if (settings.get(SETTING_CREATION_DATE) == null) {
            builder.put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());
        }
        if (settings.get(SETTING_INDEX_UUID) == null) {
            builder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
        }

        return builder.build();
    }

    public SQLExecutor addTable(String createTableStmt, String... partitions) throws IOException {
        return addTable(createTableStmt, Settings.EMPTY, partitions);
    }

    @SuppressWarnings("unchecked")
    public SQLExecutor addTable(String createTableStmt, Settings customSettings, String... partitions) throws IOException {
        CreateTable<Expression> stmt = (CreateTable<Expression>) SqlParser.createStatement(createTableStmt);
        CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
        AnalyzedCreateTable analyzedCreateTable = createTableAnalyzer.analyze(
            stmt, ParamTypeHints.EMPTY, txnCtx);

        BoundCreateTable boundCreateTable = analyzedCreateTable.bind(
            new NumberOfShards(clusterService),
            fulltextAnalyzerResolver,
            nodeCtx,
            txnCtx,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        boolean partitioned = boundCreateTable.isPartitioned();
        if (!partitioned) {
            if (partitions.length > 0) {
                throw new IllegalArgumentException("Cannot set partitions manually for unpartitioned tables");
            }
            partitions = new String[] { boundCreateTable.tableName().indexNameOrAlias() };
        }
        ClusterState prevState = clusterService.state();
        var combinedSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(boundCreateTable.settings())
            .put(customSettings)
            .build();

        // addPartitionedTable can be called multiple times, create supplier based on existing state.
        Metadata.Builder mdBuilder = Metadata.builder(prevState.metadata());
        RoutingTable.Builder routingBuilder = RoutingTable.builder(prevState.routingTable());
        LongSupplier columnOidSupplier =
                this.columnOidSupplier != null ? this.columnOidSupplier : mdBuilder.columnOidSupplier();

        Version smallestVersion = prevState.nodes().getSmallestNonClientNodeVersion();
        Version versionCreated = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(combinedSettings);

        // skip mapping generation if using RelationMetadata
        // This is a bit different than production code but is done to avoids issues with duplicate oid generation
        // (which would happen once here, and once again further done via mdBuilder.setTable)
        Map<String, Object> mapping = (partitioned ? Map.of() : null);
        List<String> indexUUIDs = new ArrayList<>();
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(prevState.blocks());
        for (String partition : partitions) {
            IndexMetadata.Builder imdBuilder = getIndexMetadata(
                partition,
                combinedSettings,
                mapping, // Each partition has the same mapping.
                smallestVersion
            );
            if (partitioned) {
                imdBuilder.partitionValues(PartitionName.fromIndexOrTemplate(partition).values());
            }
            IndexMetadata indexMetadata = imdBuilder.build();
            mdBuilder.put(indexMetadata, true);
            routingBuilder.addAsNew(indexMetadata);
            indexUUIDs.add(indexMetadata.getIndexUUID());
            blocksBuilder.addBlocks(indexMetadata);
        }
        RelationMetadata relation = prevState.metadata().getRelation(boundCreateTable.tableName());
        if (partitioned && relation instanceof RelationMetadata.Table table) {
            indexUUIDs = Lists.concatUnique(table.indexUUIDs(), indexUUIDs);
        }
        mdBuilder.setTable(
            versionCreated.before(DocTableInfo.COLUMN_OID_VERSION)
                ? NO_OID_COLUMN_OID_SUPPLIER
                : columnOidSupplier,
            boundCreateTable.tableName(),
            List.copyOf(boundCreateTable.columns().values()),
            combinedSettings,
            boundCreateTable.routingColumn(),
            TableParameters.COLUMN_POLICY.get(boundCreateTable.settings()),
            boundCreateTable.pkConstraintName(),
            boundCreateTable.getCheckConstraints(),
            Lists.map(boundCreateTable.primaryKeys(), Reference::column),
            Lists.map(boundCreateTable.partitionedBy(), Reference::toColumn),
            State.OPEN,
            indexUUIDs,
            0
        );
        ClusterState newState = ClusterState.builder(prevState)
            .metadata(mdBuilder.build())
            .routingTable(routingBuilder.build())
            .blocks(blocksBuilder)
            .build();

        ClusterServiceUtils.setState(clusterService, allocationService.reroute(newState, "assign shards"));
        return this;
    }

    public SQLExecutor startShards(String... indices) {
        var clusterState = clusterService.state();
        for (var index : indices) {
            List<String> indexUUIDs = clusterState.metadata().getIndices(RelationName.fromIndexName(index), List.of(), true, IndexMetadata::getIndexUUID);
            for (var indexUUID : indexUUIDs) {
                final List<ShardRouting> startedShards = clusterState.getRoutingNodes().shardsWithState(indexUUID, INITIALIZING);
                clusterState = allocationService.applyStartedShards(clusterState, startedShards);
            }
        }
        clusterState = allocationService.reroute(clusterState, "reroute after starting");
        ClusterServiceUtils.setState(clusterService, clusterState);
        return this;
    }

    public SQLExecutor failShards(String... indices) {
        var clusterState = clusterService.state();
        for (var index : indices) {
            List<String> indexUUIDs = clusterState.metadata().getIndices(RelationName.fromIndexName(index), List.of(), true, IndexMetadata::getIndexUUID);
            for (var indexUUID : indexUUIDs) {
                final List<ShardRouting> startedShards = clusterState.getRoutingNodes().shardsWithState(indexUUID, STARTED);
                for (ShardRouting shard : startedShards) {
                    clusterState = allocationService.applyFailedShard(clusterState, shard, false);
                }
            }
        }
        clusterState = allocationService.reroute(clusterState, "reroute after failing shards");
        ClusterServiceUtils.setState(clusterService, clusterState);
        return this;
    }

    public SQLExecutor closeTable(String tableName) throws IOException {
        RelationName relationName = RelationName.of(QualifiedName.of(tableName), Schemas.DOC_SCHEMA_NAME);
        ClusterState prevState = clusterService.state();
        var metadata = prevState.metadata();
        List<IndexMetadata> indices = metadata.getIndices(
            relationName,
            List.of(),
            false,
            imd -> imd.getState() == State.OPEN ? imd : null);
        Metadata.Builder mdBuilder = Metadata.builder(clusterService.state().metadata());
        RelationMetadata relation = metadata.getRelation(relationName);
        if (relation instanceof RelationMetadata.Table table) {
            mdBuilder.setTable(
                table.name(),
                table.columns(),
                table.settings(),
                table.routingColumn(),
                table.columnPolicy(),
                table.pkConstraintName(),
                table.checkConstraints(),
                table.primaryKeys(),
                table.partitionedBy(),
                State.CLOSE,
                table.indexUUIDs(),
                table.tableVersion()
            );
        }
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(clusterService.state().blocks());

        for (IndexMetadata imd: indices) {
            mdBuilder.put(IndexMetadata.builder(imd).state(IndexMetadata.State.CLOSE));
            blocksBuilder.addIndexBlock(imd.getIndex().getName(), INDEX_CLOSED_BLOCK);
        }

        ClusterState updatedState = ClusterState.builder(prevState).metadata(mdBuilder).blocks(blocksBuilder).build();

        ClusterServiceUtils.setState(clusterService, allocationService.reroute(updatedState, "assign shards"));
        return this;
    }


    /**
     * Add a view definition to the metadata.
     * Note that this by-passes the analyzer step and directly operates on the clusterState. So there is no
     * resolve logic for columns (`*` is not resolved to the column names)
     */
    public SQLExecutor addView(RelationName name, String query) {
        ClusterState prevState = clusterService.state();
        ViewsMetadata newViews = ViewsMetadata.addOrReplace(
            prevState.metadata().custom(ViewsMetadata.TYPE),
            name,
            query,
            user == null ? null : user.name(),
            SearchPath.createSearchPathFrom(searchPath),
            coordinatorTxnCtx.sessionSettings().errorOnUnknownObjectKey()
        );

        Metadata newMetadata = Metadata.builder(prevState.metadata()).putCustom(ViewsMetadata.TYPE, newViews).build();
        ClusterState newState = ClusterState.builder(prevState)
            .metadata(newMetadata)
            .build();

        ClusterServiceUtils.setState(clusterService, newState);
        return this;
    }

    public SQLExecutor addPublication(String name, boolean forAllTables, RelationName... tables) {
        ClusterState prevState = clusterService.state();
        var publication = new Publication(user.name(), forAllTables, Arrays.asList(tables));
        var publicationsMetadata = new PublicationsMetadata(Map.of(name, publication));

        Metadata newMetadata = Metadata.builder(prevState.metadata())
            .putCustom(PublicationsMetadata.TYPE, publicationsMetadata)
            .build();
        ClusterState newState = ClusterState.builder(prevState)
            .metadata(newMetadata)
            .build();

        ClusterServiceUtils.setState(clusterService, newState);
        return this;
    }

    public SQLExecutor addSubscription(String name, String publication) {
        ClusterState prevState = clusterService.state();
        var subscription = new Subscription(
            user.name(),
            ConnectionInfo.fromURL("crate://localhost"),
            List.of(publication),
            Settings.EMPTY,
            Collections.emptyMap()
        );
        var subsMetadata = new SubscriptionsMetadata(Map.of(name, subscription));

        Metadata newMetadata = Metadata.builder(prevState.metadata())
            .putCustom(SubscriptionsMetadata.TYPE, subsMetadata)
            .build();
        ClusterState newState = ClusterState.builder(prevState)
            .metadata(newMetadata)
            .build();

        ClusterServiceUtils.setState(clusterService, newState);
        return this;
    }

    @SuppressWarnings("unchecked")
    public SQLExecutor addBlobTable(String createBlobTableStmt) throws IOException {
        CreateBlobTable<Expression> stmt = (CreateBlobTable<Expression>) SqlParser.createStatement(createBlobTableStmt);
        CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
        AnalyzedCreateBlobTable analyzedStmt = createBlobTableAnalyzer.analyze(
            stmt, ParamTypeHints.EMPTY, txnCtx);
        Settings settings = CreateBlobTablePlan.buildSettings(
            analyzedStmt.createBlobTable(),
            txnCtx,
            nodeCtx,
            Row.EMPTY,
            SubQueryResults.EMPTY,
            new NumberOfShards(clusterService));

        ClusterState prevState = clusterService.state();
        RelationName relationName = analyzedStmt.relationName();
        IndexMetadata indexMetadata = getIndexMetadata(
            fullIndexName(relationName.name()),
            settings,
            Collections.emptyMap(),
            prevState.nodes().getSmallestNonClientNodeVersion()
        ).build();

        Metadata.Builder mdBuilder = Metadata.builder(prevState.metadata())
            .setBlobTable(relationName, indexMetadata.getIndexUUID(), settings, State.OPEN)
            .put(indexMetadata, true);
        ClusterState state = ClusterState.builder(prevState)
            .metadata(mdBuilder)
            .routingTable(RoutingTable.builder(prevState.routingTable()).addAsNew(indexMetadata).build())
            .build();

        ClusterServiceUtils.setState(clusterService, allocationService.reroute(state, "assign shards"));
        return this;
    }

    public SQLExecutor addServer(String serverName,
                                 String fdw,
                                 String owner,
                                 Settings options) throws Exception {
        CreateServerRequest request = new CreateServerRequest(
            serverName,
            fdw,
            owner,
            true,
            options
        );
        AddServerTask addServerTask = new AddServerTask(foreignDataWrappers, request);
        ClusterServiceUtils.setState(clusterService, addServerTask.execute(clusterService.state()));
        return this;
    }

    public SQLExecutor addForeignTable(String stmt) throws Exception {
        CreateForeignTable createTable = (CreateForeignTable) SqlParser.createStatement(stmt);
        var txnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
        Cursors cursors = new Cursors();
        var analysis = new Analysis(txnCtx, ParamTypeHints.EMPTY, cursors);
        var analyzedCreateForeignTable = FdwAnalyzer.analyze(analysis, nodeCtx, createTable);
        RoutingProvider routingProvider = new RoutingProvider(random.nextInt(), emptyList());
        ClusterState currentState = clusterService.state();
        PlannerContext plannerContext = planner.createContext(
            routingProvider,
            UUIDs.dirtyUUID(),
            txnCtx,
            0,
            null,
            cursors,
            TransactionState.IDLE,
            Session.TimeoutToken.noopToken()
        );
        var request = CreateForeignTablePlan.toRequest(
            foreignDataWrappers,
            analyzedCreateForeignTable,
            plannerContext,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        AddForeignTableTask addForeignTableTask = new AddForeignTableTask(request);
        ClusterState newState = addForeignTableTask.execute(currentState);
        ClusterServiceUtils.setState(clusterService, newState);
        return this;
    }

    public SQLExecutor setSearchPath(String ... schemas) {
        searchPath = schemas;
        sessionSettings.setSearchPath(schemas);
        return this;
    }

    public SQLExecutor setUser(Role user) {
        this.sessionSettings.setSessionUser(user);
        this.user = user;
        return this;
    }

    public SQLExecutor setColumnOidSupplier(LongSupplier oidSupplier) {
        this.columnOidSupplier = oidSupplier;
        return this;
    }

    public SQLExecutor addUDFLanguage(UDFLanguage lang) {
        udfService.registerLanguage(lang);
        return this;
    }

    public SQLExecutor addUDF(UserDefinedFunctionMetadata udf) {
        UserDefinedFunctionsMetadata udfs = clusterService.state().metadata().custom(UserDefinedFunctionsMetadata.TYPE);
        udfService.updateImplementations(Lists.concat(udfs == null ? List.of() : udfs.functionsMetadata(), udf));
        return this;
    }

    public Rule.Context ruleContext() {
      return new Rule.Context(planStats, CoordinatorTxnCtx.systemTransactionContext(), nodeCtx, UnaryOperator.identity(), null);
    }
}
