/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway;

import static java.util.Collections.unmodifiableMap;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import io.crate.common.unit.TimeValue;

public class ReplicaShardAllocatorTests extends ESAllocationTestCase {
    private static final org.apache.lucene.util.Version MIN_SUPPORTED_LUCENE_VERSION = org.elasticsearch.Version.CURRENT
        .minimumIndexCompatibilityVersion().luceneVersion;
    private final ShardId shardId = new ShardId( "test", "test", 0);
    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final DiscoveryNode node3 = newNode("node3");

    private TestAllocator testAllocator;

    @Before
    public void buildTestAllocator() {
        this.testAllocator = new TestAllocator();
    }

    private void allocateAllUnassigned(final RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            testAllocator.allocateUnassigned(iterator.next(), allocation, iterator);
        }
    }

    /**
     * Verifies that when we are still fetching data in an async manner, the replica shard moves to ignore unassigned.
     */
    @Test
    public void testNoAsyncFetchData() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.clean();
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored()).hasSize(1);
        assertThat(allocation.routingNodes().unassigned().ignored().getFirst().shardId()).isEqualTo(shardId);
    }

    /**
     * Verifies that on index creation, we don't go and fetch data, but keep the replica shard unassigned to let
     * the shard allocator to allocate it. There isn't a copy around to find anyhow.
     */
    @Test
    public void testNoAsyncFetchOnIndexCreation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders(), Settings.EMPTY,
            UnassignedInfo.Reason.INDEX_CREATED);
        testAllocator.clean();
        allocateAllUnassigned(allocation);
        assertThat(testAllocator.getFetchDataCalledAndClean()).isFalse();
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).getFirst().shardId()).isEqualTo(shardId);
    }

    /**
     * Verifies that for anything but index creation, fetch data ends up being called, since we need to go and try
     * and find a better copy for the shard.
     */
    @Test
    public void testAsyncFetchOnAnythingButIndexCreation() {
        UnassignedInfo.Reason reason = RandomPicks.randomFrom(random(),
            EnumSet.complementOf(EnumSet.of(UnassignedInfo.Reason.INDEX_CREATED)));
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders(), Settings.EMPTY, reason);
        testAllocator.clean();
        allocateAllUnassigned(allocation);
        assertThat(testAllocator.getFetchDataCalledAndClean()).as("failed with reason " + reason).isTrue();
    }

    /**
     * Verifies that when there is a full match (syncId and files) we allocate it to matching node.
     */
    @Test
    public void testSimpleFullMatchAllocation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
                .addData(nodeToMatch, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).getFirst().currentNodeId()).isEqualTo(nodeToMatch.getId());
    }

    /**
     * Verifies that when there is a sync id match but no files match, we allocate it to matching node.
     */
    @Test
    public void testSyncIdMatch() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
                .addData(nodeToMatch, "MATCH", new StoreFileMetadata("file1", 10, "NO_MATCH_CHECKSUM" ,MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).getFirst().currentNodeId()).isEqualTo(nodeToMatch.getId());
    }

    /**
     * Verifies that when there is no sync id match but files match, we allocate it to matching node.
     */
    @Test
    public void testFileChecksumMatch() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
                .addData(nodeToMatch, "NO_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).getFirst().currentNodeId()).isEqualTo(nodeToMatch.getId());
    }

    @Test
    public void testPreferCopyWithHighestMatchingOperations() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        long retainingSeqNoOnPrimary = randomLongBetween(1, Integer.MAX_VALUE);
        long retainingSeqNoForNode2 = randomLongBetween(0, retainingSeqNoOnPrimary - 1);
        // Rarely use a seqNo above retainingSeqNoOnPrimary, which could in theory happen when primary fails and comes back quickly.
        long retainingSeqNoForNode3 = randomLongBetween(retainingSeqNoForNode2 + 1, retainingSeqNoOnPrimary + 100);
        List<RetentionLease> retentionLeases = Arrays.asList(newRetentionLease(node1, retainingSeqNoOnPrimary),
            newRetentionLease(node2, retainingSeqNoForNode2), newRetentionLease(node3, retainingSeqNoForNode3));
        testAllocator.addData(node1, retentionLeases, "MATCH",
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node2, "NOT_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).getFirst().currentNodeId()).isEqualTo(node3.getId());
    }

    @Test
    public void testCancelRecoveryIfFoundCopyWithNoopRetentionLease() {
        final UnassignedInfo unassignedInfo;
        final Set<String> failedNodeIds;
        if (randomBoolean()) {
            failedNodeIds = Collections.emptySet();
            unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null);
        } else {
            failedNodeIds = new HashSet<>(randomSubsetOf(Set.of("node-4", "node-5", "node-6", "node-7")));
            unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, null, null, randomIntBetween(1, 10),
                System.nanoTime(), System.currentTimeMillis(), false, UnassignedInfo.AllocationStatus.NO_ATTEMPT, failedNodeIds);
        }
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders(), unassignedInfo);
        long retainingSeqNo = randomLongBetween(1, Long.MAX_VALUE);
        testAllocator.addData(node1, Arrays.asList(newRetentionLease(node1, retainingSeqNo), newRetentionLease(node3, retainingSeqNo)),
            "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node2, "NO_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged()).isTrue();
        List<ShardRouting> unassignedShards = allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED);
        assertThat(unassignedShards).hasSize(1);
        assertThat(unassignedShards.getFirst().shardId()).isEqualTo(shardId);
        assertThat(unassignedShards.getFirst().unassignedInfo().getNumFailedAllocations()).isEqualTo(0);
        assertThat(unassignedShards.getFirst().unassignedInfo().getFailedNodeIds()).isEqualTo(failedNodeIds);
    }

    @Test
    public void testNotCancellingRecoveryIfCurrentRecoveryHasRetentionLease() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        List<RetentionLease> peerRecoveryRetentionLeasesOnPrimary = new ArrayList<>();
        long retainingSeqNo = randomLongBetween(1, Long.MAX_VALUE);
        peerRecoveryRetentionLeasesOnPrimary.add(newRetentionLease(node1, retainingSeqNo));
        peerRecoveryRetentionLeasesOnPrimary.add(newRetentionLease(node2, randomLongBetween(1, retainingSeqNo)));
        if (randomBoolean()) {
            peerRecoveryRetentionLeasesOnPrimary.add(newRetentionLease(node3, randomLongBetween(0, retainingSeqNo)));
        }
        testAllocator.addData(node1, peerRecoveryRetentionLeasesOnPrimary,
            "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node2, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged()).isFalse();
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED)).hasSize(0);
    }

    @Test
    public void testNotCancelIfPrimaryDoesNotHaveValidRetentionLease() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        testAllocator.addData(node1, Collections.singletonList(newRetentionLease(node3, randomNonNegativeLong())),
            "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node2, "NOT_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node3, "NOT_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged()).isFalse();
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED)).hasSize(0);
    }

    @Test
    public void testIgnoreRetentionLeaseIfCopyIsEmpty() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        long retainingSeqNo = randomLongBetween(1, Long.MAX_VALUE);
        List<RetentionLease> retentionLeases = new ArrayList<>();
        retentionLeases.add(newRetentionLease(node1, retainingSeqNo));
        retentionLeases.add(newRetentionLease(node2, randomLongBetween(0, retainingSeqNo)));
        if (randomBoolean()) {
            retentionLeases.add(newRetentionLease(node3, randomLongBetween(0, retainingSeqNo)));
        }
        testAllocator.addData(node1, retentionLeases, randomSyncId(),
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node2, null); // has retention lease but store is empty
        testAllocator.addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).getFirst().currentNodeId()).isEqualTo(node3.getId());
    }

    /**
     * When we can't find primary data, but still find replica data, we go ahead and keep it unassigned
     * to be allocated. This is today behavior, which relies on a primary corruption identified with
     * adding a replica and having that replica actually recover and cause the corruption to be identified
     * See CorruptFileTest#
     */
    @Test
    public void testNoPrimaryData() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).getFirst().shardId()).isEqualTo(shardId);
    }

    /**
     * Verifies that when there is primary data, but no data at all on other nodes, the shard keeps
     * unassigned to be allocated later on.
     */
    @Test
    public void testNoDataForReplicaOnAnyNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).getFirst().shardId()).isEqualTo(shardId);
    }

    /**
     * Verifies that when there is primary data, but no matching data at all on other nodes, the shard keeps
     * unassigned to be allocated later on.
     */
    @Test
    public void testNoMatchingFilesForReplicaOnAnyNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
                .addData(node2, "NO_MATCH", new StoreFileMetadata("file1", 10, "NO_MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).getFirst().shardId()).isEqualTo(shardId);
    }

    /**
     * When there is no decision or throttle decision across all nodes for the shard, make sure the shard
     * moves to the ignore unassigned list.
     */
    @Test
    public void testNoOrThrottleDecidersRemainsInUnassigned() {
        RoutingAllocation allocation =
            onePrimaryOnNode1And1Replica(randomBoolean() ? noAllocationDeciders() : throttleAllocationDeciders());
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
                .addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored()).hasSize(1);
        assertThat(allocation.routingNodes().unassigned().ignored().getFirst().shardId()).isEqualTo(shardId);
    }

    /**
     * Tests when the node to allocate to due to matching is being throttled, we move the shard to ignored
     * to wait till throttling on it is done.
     */
    @Test
    public void testThrottleWhenAllocatingToMatchingNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(new AllocationDeciders(
            Arrays.asList(new TestAllocateDecision(Decision.YES),
                new SameShardAllocationDecider(
                    Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
                new AllocationDecider() {
                    @Override
                    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                        if (node.node().equals(node2)) {
                            return Decision.THROTTLE;
                        }
                        return Decision.YES;
                    }
                })));
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
                .addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored()).hasSize(1);
        assertThat(allocation.routingNodes().unassigned().ignored().getFirst().shardId()).isEqualTo(shardId);
    }

    @Test
    public void testDelayedAllocation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders(),
                Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueHours(1))
                    .build(), UnassignedInfo.Reason.NODE_LEFT);
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        if (randomBoolean()) {
            // we sometime return empty list of files, make sure we test this as well
            testAllocator.addData(node2, null);
        }
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged()).isFalse();
        assertThat(allocation.routingNodes().unassigned().ignored()).hasSize(1);
        assertThat(allocation.routingNodes().unassigned().ignored().getFirst().shardId()).isEqualTo(shardId);

        allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders(),
                Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(),
                    TimeValue.timeValueHours(1)).build(), UnassignedInfo.Reason.NODE_LEFT);
        testAllocator.addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged()).isTrue();
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).getFirst().currentNodeId()).isEqualTo(node2.getId());
    }

    @Test
    public void testCancelRecoveryBetterSyncId() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
                .addData(node2, "NO_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
                .addData(node3, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged()).isTrue();
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED)).hasSize(1);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).getFirst().shardId()).isEqualTo(shardId);
    }

    @Test
    public void testNotCancellingRecoveryIfSyncedOnExistingRecovery() {
        final UnassignedInfo unassignedInfo;
        if (randomBoolean()) {
            unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null);
        } else {
            unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, null, null, randomIntBetween(1, 10),
                System.nanoTime(), System.currentTimeMillis(), false, UnassignedInfo.AllocationStatus.NO_ATTEMPT, Set.of("node-4"));
        }
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders(), unassignedInfo);
        List<RetentionLease> retentionLeases = new ArrayList<>();
        if (randomBoolean()) {
            long retainingSeqNoOnPrimary = randomLongBetween(0, Long.MAX_VALUE);
            retentionLeases.add(newRetentionLease(node1, retainingSeqNoOnPrimary));
            if (randomBoolean()) {
                retentionLeases.add(newRetentionLease(node2, randomLongBetween(0, retainingSeqNoOnPrimary)));
            }
            if (randomBoolean()) {
                retentionLeases.add(newRetentionLease(node3, randomLongBetween(0, retainingSeqNoOnPrimary)));
            }
        }
        testAllocator.addData(node1, retentionLeases, "MATCH",
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM",
            MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged()).isFalse();
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED)).hasSize(0);
    }

    @Test
    public void testNotCancellingRecovery() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
                .addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged()).isFalse();
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED)).hasSize(0);
    }

    @Test
    public void testDoNotCancelForBrokenNode() {
        Set<String> failedNodes = new HashSet<>();
        failedNodes.add(node3.getId());
        if (randomBoolean()) {
            failedNodes.add("node4");
        }
        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, null, null,
            randomIntBetween(failedNodes.size(), 10), System.nanoTime(), System.currentTimeMillis(), false,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT, failedNodes);
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders(), unassignedInfo);
        long retainingSeqNoOnPrimary = randomLongBetween(0, Long.MAX_VALUE);
        List<RetentionLease> retentionLeases = Arrays.asList(
            newRetentionLease(node1, retainingSeqNoOnPrimary), newRetentionLease(node3, retainingSeqNoOnPrimary));
        testAllocator
            .addData(node1, retentionLeases, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged()).isFalse();
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED)).isEmpty();
    }

    private RoutingAllocation onePrimaryOnNode1And1Replica(AllocationDeciders deciders) {
        return onePrimaryOnNode1And1Replica(deciders, Settings.EMPTY, UnassignedInfo.Reason.CLUSTER_RECOVERED);
    }

    private RoutingAllocation onePrimaryOnNode1And1Replica(AllocationDeciders deciders, Settings settings, UnassignedInfo.Reason reason) {
        ShardRouting primaryShard = TestShardRouting.newShardRouting(shardId, node1.getId(), true, ShardRoutingState.STARTED);
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(settings(Version.CURRENT).put(settings))
            .numberOfShards(1).numberOfReplicas(1)
            .putInSyncAllocationIds(0, Set.of(primaryShard.allocationId().getId()));
        Metadata metaData = Metadata.builder().put(indexMetadata).build();
        // mark shard as delayed if reason is NODE_LEFT
        boolean delayed = reason == UnassignedInfo.Reason.NODE_LEFT &&
            UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(settings).nanos() > 0;
        int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0;
        RoutingTable routingTable = RoutingTable.builder()
                .add(IndexRoutingTable.builder(shardId.getIndex())
                                .addIndexShard(new IndexShardRoutingTable.Builder(shardId)
                                        .addShard(primaryShard)
                                        .addShard(ShardRouting.newUnassigned(shardId, false,
                                            RecoverySource.PeerRecoverySource.INSTANCE,
                                            new UnassignedInfo(reason, null, null, failedAllocations, System.nanoTime(),
                                                System.currentTimeMillis(), delayed, UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                                                Collections.emptySet())))
                                        .build())
                )
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3)).build();
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY,
            System.nanoTime());
    }

    private RoutingAllocation onePrimaryOnNode1And1ReplicaRecovering(AllocationDeciders deciders, UnassignedInfo unassignedInfo) {
        ShardRouting primaryShard = TestShardRouting.newShardRouting(shardId, node1.getId(), true, ShardRoutingState.STARTED);
        Metadata metaData = Metadata.builder()
                .put(IndexMetadata.builder(shardId.getIndexUUID()).settings(settings(Version.CURRENT))
                    .numberOfShards(1).numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Set.of(primaryShard.allocationId().getId())))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .add(IndexRoutingTable.builder(shardId.getIndex())
                                .addIndexShard(new IndexShardRoutingTable.Builder(shardId)
                                        .addShard(primaryShard)
                                        .addShard(TestShardRouting.newShardRouting(shardId, node2.getId(), null, false,
                                            ShardRoutingState.INITIALIZING, unassignedInfo))
                                        .build())
                )
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3)).build();
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY,
            System.nanoTime());
    }

    private RoutingAllocation onePrimaryOnNode1And1ReplicaRecovering(AllocationDeciders deciders) {
        return onePrimaryOnNode1And1ReplicaRecovering(deciders, new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));
    }

    static RetentionLease newRetentionLease(DiscoveryNode node, long retainingSeqNo) {
        return new RetentionLease(ReplicationTracker.getPeerRecoveryRetentionLeaseId(node.getId()),
            retainingSeqNo, randomNonNegativeLong(), ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE);
    }

    static String randomSyncId() {
        return randomFrom("MATCH", "NOT_MATCH", null);
    }

    class TestAllocator extends ReplicaShardAllocator {

        private Map<DiscoveryNode, TransportNodesListShardStoreMetadata.StoreFilesMetadata> data = null;
        private final AtomicBoolean fetchDataCalled = new AtomicBoolean(false);

        public void clean() {
            data = null;
        }

        public boolean getFetchDataCalledAndClean() {
            return fetchDataCalled.getAndSet(false);
        }

        public TestAllocator addData(DiscoveryNode node, String syncId, StoreFileMetadata... files) {
            return addData(node, Collections.emptyList(), syncId, files);
        }

        TestAllocator addData(DiscoveryNode node, List<RetentionLease> peerRecoveryRetentionLeases,
                              String syncId, StoreFileMetadata... files) {
            if (data == null) {
                data = new HashMap<>();
            }
            Map<String, StoreFileMetadata> filesAsMap = new HashMap<>();
            for (StoreFileMetadata file : files) {
                filesAsMap.put(file.name(), file);
            }
            Map<String, String> commitData = new HashMap<>();
            if (syncId != null) {
                commitData.put(Engine.SYNC_COMMIT_ID, syncId);
            }
            data.put(node, new TransportNodesListShardStoreMetadata.StoreFilesMetadata(shardId,
                    new Store.MetadataSnapshot(unmodifiableMap(filesAsMap), unmodifiableMap(commitData), randomInt()),
                    peerRecoveryRetentionLeases));
            return this;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata>
                                                                            fetchData(ShardRouting shard, RoutingAllocation allocation) {
            fetchDataCalled.set(true);
            Map<DiscoveryNode, TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> tData = null;
            if (data != null) {
                tData = new HashMap<>();
                for (Map.Entry<DiscoveryNode, TransportNodesListShardStoreMetadata.StoreFilesMetadata> entry : data.entrySet()) {
                    tData.put(entry.getKey(),
                        new TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata(entry.getKey(), entry.getValue()));
                }
            }
            return new AsyncShardFetch.FetchResult<>(shardId, tData, Collections.emptySet());
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return fetchDataCalled.get();
        }
    }
}
