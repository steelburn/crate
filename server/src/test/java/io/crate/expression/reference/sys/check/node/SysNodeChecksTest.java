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

package io.crate.expression.reference.sys.check.node;

import static io.crate.testing.Asserts.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.node.NodeService;
import org.junit.Test;
import org.mockito.Answers;

import io.crate.common.unit.TimeValue;
import io.crate.expression.reference.sys.check.SysCheck;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class SysNodeChecksTest extends CrateDummyClusterServiceUnitTest {

    /**
     * We currently test deprecated settings for BWC. Enable warnings once the deprecated gateway settings are removed.
     */
    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithDefaultSetting() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        var dataNodes = ImmutableOpenMap.<String, DiscoveryNode>builder().build();
        when(clusterService.state().nodes().getDataNodes()).thenReturn(dataNodes);
        when(clusterService.state().nodes().getSize()).thenReturn(1);

        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(clusterService, Settings.EMPTY);


        assertThat(recoveryExpectedNodesCheck.id()).isEqualTo(1);
        assertThat(recoveryExpectedNodesCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(recoveryExpectedNodesCheck.isValid()).isTrue();
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithLessThanQuorum() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        var dataNodes = ImmutableOpenMap.<String, DiscoveryNode>builder()
            .fPut("data_node1", mock(DiscoveryNode.class))
            .fPut("data_node2", mock(DiscoveryNode.class))
            .build();
        when(clusterService.state().nodes().getDataNodes()).thenReturn(dataNodes);

        var settings = Settings.builder()
            .put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 1)
            .build();

        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(clusterService, settings);

        assertThat(recoveryExpectedNodesCheck.id()).isEqualTo(1);
        assertThat(recoveryExpectedNodesCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(recoveryExpectedNodesCheck.isValid()).isFalse();
    }

    @Test
    public void test_recovery_expected_nodes_check_BWC_with_deprecated_settings() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        var dataNodes = ImmutableOpenMap.<String, DiscoveryNode>builder().build();
        when(clusterService.state().nodes().getDataNodes()).thenReturn(dataNodes);
        when(clusterService.state().nodes().getSize()).thenReturn(2);

        var settings = Settings.builder()
            .put(GatewayService.EXPECTED_NODES_SETTING.getKey(), 1)
            .build();

        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(clusterService, settings);

        assertThat(recoveryExpectedNodesCheck.id()).isEqualTo(1);
        assertThat(recoveryExpectedNodesCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(recoveryExpectedNodesCheck.isValid()).isFalse();
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithCorrectSetting() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        var dataNodes = ImmutableOpenMap.<String, DiscoveryNode>builder()
            .fPut("data_node1", mock(DiscoveryNode.class))
            .fPut("data_node2", mock(DiscoveryNode.class))
            .fPut("data_node3", mock(DiscoveryNode.class))
            .build();
        when(clusterService.state().nodes().getDataNodes()).thenReturn(dataNodes);

        var settings = Settings.builder()
            .put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 3)
            .build();

        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(clusterService, settings);

        assertThat(recoveryExpectedNodesCheck.id()).isEqualTo(1);
        assertThat(recoveryExpectedNodesCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(recoveryExpectedNodesCheck.isValid()).isTrue();
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithBiggerThanNumberOfNodes() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        var dataNodes = ImmutableOpenMap.<String, DiscoveryNode>builder()
            .fPut("data_node1", mock(DiscoveryNode.class))
            .fPut("data_node2", mock(DiscoveryNode.class))
            .fPut("data_node3", mock(DiscoveryNode.class))
            .build();
        when(clusterService.state().nodes().getDataNodes()).thenReturn(dataNodes);

        var settings = Settings.builder()
            .put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 4)
            .build();

        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(clusterService, settings);

        assertThat(recoveryExpectedNodesCheck.id()).isEqualTo(1);
        assertThat(recoveryExpectedNodesCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(recoveryExpectedNodesCheck.isValid()).isFalse();
    }

    @Test
    public void testRecoveryAfterNodesCheckWithDefaultSetting() {
        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterNodesSysCheck(clusterService, Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id()).isEqualTo(2);
        assertThat(recoveryAfterNodesCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(recoveryAfterNodesCheck.isValid()).isTrue();
    }

    @Test
    public void testRecoveryAfterNodesCheckWithLessThanQuorum() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        var dataNodes = ImmutableOpenMap.<String, DiscoveryNode>builder()
            .fPut("data_node1", mock(DiscoveryNode.class))
            .fPut("data_node2", mock(DiscoveryNode.class))
            .build();
        when(clusterService.state().nodes().getDataNodes()).thenReturn(dataNodes);

        var settings = Settings.builder()
            .put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 1)
            .put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 2)
            .build();

        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterNodesSysCheck(clusterService, settings);

        assertThat(recoveryAfterNodesCheck.id()).isEqualTo(2);
        assertThat(recoveryAfterNodesCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(recoveryAfterNodesCheck.isValid()).isFalse();
    }

    @Test
    public void test_recovery_after_nodes_check_BWC_with_deprecated_setting() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        var dataNodes = ImmutableOpenMap.<String, DiscoveryNode>builder().build();
        when(clusterService.state().nodes().getDataNodes()).thenReturn(dataNodes);
        when(clusterService.state().nodes().getSize()).thenReturn(8);

        var settings = Settings.builder()
            .put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), 4)
            .put(GatewayService.EXPECTED_NODES_SETTING.getKey(), 8)
            .build();

        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterNodesSysCheck(clusterService, settings);

        assertThat(recoveryAfterNodesCheck.id()).isEqualTo(2);
        assertThat(recoveryAfterNodesCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(recoveryAfterNodesCheck.isValid()).isFalse();
    }

    @Test
    public void testRecoveryAfterNodesCheckWithCorrectSetting() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        var dataNodes = ImmutableOpenMap.<String, DiscoveryNode>builder()
            .fPut("data_node1", mock(DiscoveryNode.class))
            .fPut("data_node2", mock(DiscoveryNode.class))
            .build();
        when(clusterService.state().nodes().getDataNodes()).thenReturn(dataNodes);

        var settings = Settings.builder()
            .put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 2)
            .put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 3)
            .build();

        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterNodesSysCheck(clusterService, settings);

        assertThat(recoveryAfterNodesCheck.id()).isEqualTo(2);
        assertThat(recoveryAfterNodesCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(recoveryAfterNodesCheck.isValid()).isTrue();
    }

    @Test
    public void testRecoveryAfterTimeCheckWithCorrectSetting() {
        Settings settings = Settings.builder()
            .put(GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMillis(4).toString())
            .put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 3)
            .put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 3)
            .build();

        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck = new RecoveryAfterTimeSysCheck(settings);
        assertThat(recoveryAfterNodesCheck.isValid()).isTrue();
    }

    @Test
    public void test_recovery_after_time_check_BWC_with_deprecated_correct_setting() {
        Settings settings = Settings.builder()
            .put(GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMillis(4).toString())
            .put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), 3)
            .put(GatewayService.EXPECTED_NODES_SETTING.getKey(), 3)
            .build();

        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck = new RecoveryAfterTimeSysCheck(settings);
        assertThat(recoveryAfterNodesCheck.isValid()).isTrue();
    }

    @Test
    public void testRecoveryAfterTimeCheckWithDefaultSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck = new RecoveryAfterTimeSysCheck(Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id()).isEqualTo(3);
        assertThat(recoveryAfterNodesCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(recoveryAfterNodesCheck.isValid()).isTrue();
    }

    @Test
    public void testRecoveryAfterTimeCheckWithWrongSetting() {
        Settings settings = Settings.builder()
            .put(GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMillis(0).toString())
            .put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 3)
            .put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 3)
            .build();

        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck = new RecoveryAfterTimeSysCheck(settings);

        assertThat(recoveryAfterNodesCheck.isValid()).isFalse();
    }

    @Test
    public void testValidationLowDiskWatermarkCheck() {
        DiskWatermarkNodesSysCheck low = new LowDiskWatermarkNodesSysCheck(
            clusterService,
            Settings.EMPTY,
            mock(NodeService.class, Answers.RETURNS_MOCKS)
        );

        assertThat(low.id()).isEqualTo(6);
        assertThat(low.severity()).isEqualTo(SysCheck.Severity.HIGH);

        // default threshold is: 85% used
        assertThat(low.isValid(15, 100)).isTrue();
        assertThat(low.isValid(14, 100)).isFalse();
    }

    @Test
    public void testLowDiskWatermarkSucceedsIfThresholdCheckIsDisabled() {
        LowDiskWatermarkNodesSysCheck check = new LowDiskWatermarkNodesSysCheck(
            clusterService,
            Settings.builder().put("cluster.routing.allocation.disk.threshold_enabled", false).build(),
            mock(NodeService.class, Answers.RETURNS_MOCKS)
        );
        assertThat(check.isValid()).isTrue();
    }

    @Test
    public void testValidationHighDiskWatermarkCheck() {
        DiskWatermarkNodesSysCheck high = new HighDiskWatermarkNodesSysCheck(
            clusterService,
            Settings.EMPTY,
            mock(NodeService.class, Answers.RETURNS_MOCKS)
        );

        assertThat(high.id()).isEqualTo(5);
        assertThat(high.severity()).isEqualTo(SysCheck.Severity.HIGH);

        // default threshold is: 90% used
        assertThat(high.isValid(10, 100)).isTrue();
        assertThat(high.isValid(9, 100)).isFalse();
    }

    @Test
    public void testValidationFloodStageDiskWatermarkCheck() {
        DiskWatermarkNodesSysCheck floodStage = new FloodStageDiskWatermarkNodesSysCheck(
            clusterService,
            Settings.EMPTY,
            mock(NodeService.class, Answers.RETURNS_MOCKS)
        );

        assertThat(floodStage.id()).isEqualTo(7);
        assertThat(floodStage.severity()).isEqualTo(SysCheck.Severity.HIGH);

        // default threshold is: 95% used
        assertThat(floodStage.isValid(5, 100)).isTrue();
        assertThat(floodStage.isValid(4, 100)).isFalse();
    }

    @Test
    public void test_max_shard_per_node_check() {
        var nodeId = "node_1";
        var node = new DiscoveryNode(
            nodeId,
            nodeId,
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        var discoveryNodes = DiscoveryNodes
            .builder()
            .add(node)
            .masterNodeId(nodeId)
            .localNodeId(nodeId)
            .build();

        var numberOfShards = 85;
        Index index = new Index("test", UUIDs.randomBase64UUID());
        var indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        // Create a routing table for 85 shards on the same node
        for (int i = 1; i <= numberOfShards; i++) {
            indexRoutingTableBuilder.addShard(
                TestShardRouting.newShardRouting(
                    index.getUUID(),
                    i,
                    nodeId,
                    true,
                    ShardRoutingState.STARTED
                )
            );
        }

        var routingTable = RoutingTable.builder().add(indexRoutingTableBuilder).build();
        var meta = IndexMetadata.builder(index.getUUID()).settings(settings(Version.CURRENT)).numberOfShards(numberOfShards).numberOfReplicas(0);
        var clusterState = ClusterState.builder(new ClusterName("crate")).version(1L)
            .metadata(Metadata.builder().put(meta)).routingTable(routingTable).nodes(discoveryNodes).build();

        // Validate that with `cluster.max_shards_per_node = 100` and 85 shards the check passes
        var setting = Settings.builder().put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 100).build();
        var clusterSettings = new ClusterSettings(setting, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        var clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        var maxShardsPerNodeSysCheck = new MaxShardsPerNodeSysCheck(clusterService);

        assertThat(maxShardsPerNodeSysCheck.id()).isEqualTo(8);
        assertThat(maxShardsPerNodeSysCheck.severity()).isEqualTo(SysCheck.Severity.MEDIUM);
        assertThat(maxShardsPerNodeSysCheck.isValid()).isTrue();

        // Validate that with `cluster.max_shards_per_node = 90` and 85 shards the check fails
        setting = Settings.builder().put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 90).build();
        clusterSettings = new ClusterSettings(setting, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        maxShardsPerNodeSysCheck = new MaxShardsPerNodeSysCheck(clusterService);
        assertThat(maxShardsPerNodeSysCheck.isValid()).isFalse();
    }
}
