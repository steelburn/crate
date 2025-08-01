/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.cluster.shards.ShardCounts.forDataNodeCount;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.shards.ShardCounts;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class ShardLimitValidatorTests extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testOverShardLimit() {
        int nodesInCluster = randomIntBetween(1, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);

        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas()
        );

        int shardsToAdd = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        Optional<String> errorMessage = ShardLimitValidator.checkShardLimit(shardsToAdd, state, counts.getShardsPerNode());

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        assertThat(errorMessage.isPresent()).isTrue();
        assertThat(errorMessage.get()).isEqualTo("this action would add [" + totalShards + "] total shards, but this cluster currently has [" + currentShards
            + "]/[" + maxShards + "] maximum shards open");
    }

    @Test
    public void testUnderShardLimit() {
        int nodesInCluster = randomIntBetween(2, 90);
        // Calculate the counts for a cluster 1 node smaller than we have to ensure we have headroom
        ShardCounts counts = forDataNodeCount(nodesInCluster - 1);


        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas()
        );

        int existingShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int shardsToAdd = randomIntBetween(1, (counts.getShardsPerNode() * nodesInCluster) - existingShards);
        Optional<String> errorMessage = ShardLimitValidator.checkShardLimit(shardsToAdd, state, counts.getShardsPerNode());

        assertThat(errorMessage.isPresent()).isFalse();
    }


    private static ClusterState createClusterForShardLimitTest(int nodesInCluster, int shardsInIndex, int replicas) {
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodes = ImmutableOpenMap.builder();
        for (int i = 0; i < nodesInCluster; i++) {
            dataNodes.put(randomAlphaOfLengthBetween(5, 15), mock(DiscoveryNode.class));
        }
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getDataNodes()).thenReturn(dataNodes.build());

        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 15))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .creationDate(randomLong())
            .numberOfShards(shardsInIndex)
            .numberOfReplicas(replicas);
        Metadata.Builder metadata = Metadata.builder().put(indexMetadata);
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(nodes)
            .build();
    }

    @Test
    public void test_supported_total_shards_cannot_exceed_integer_max_value() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1925152226)
            .build();
        var validator = new ShardLimitValidator(settings, clusterService);
        assertThatThrownBy(() -> validator.validateShardLimit(settings, clusterService.state()))
            .isExactlyInstanceOf(ValidationException.class)
            .hasMessage("Validation Failed: 1: this action would add more than the supported [2147483647] total shards;");
    }

    @Test
    public void test_supported_replica_shards_cannot_exceed_integer_max_value_minus_one() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2147483647)
            .build();
        var validator = new ShardLimitValidator(settings, clusterService);
        assertThatThrownBy(() -> validator.validateShardLimit(settings, clusterService.state()))
            .isExactlyInstanceOf(ValidationException.class)
            .hasMessage("Validation Failed: 1: this action would add more than the supported [2147483647] total shards;");
    }

    @Test
    public void test_max_shards_in_cluster_cannot_exceed_integer_max_value() {
        int nodesInCluster = 2;
        ClusterState state = createClusterForShardLimitTest(nodesInCluster, 1, 0);

        Optional<String> errorMessage = ShardLimitValidator.checkShardLimit(1, state, Integer.MAX_VALUE);
        assertThat(errorMessage.isPresent()).isTrue();
        assertThat(errorMessage.get()).isEqualTo("this action would add more than the supported [2147483647] total shards");
    }
}
