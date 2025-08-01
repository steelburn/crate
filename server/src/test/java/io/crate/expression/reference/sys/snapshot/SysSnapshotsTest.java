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

package io.crate.expression.reference.sys.snapshot;

import static io.crate.testing.Asserts.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.assertj.core.api.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import io.crate.metadata.RelationName;

public class SysSnapshotsTest extends ESTestCase {

    @Test
    public void testUnavailableSnapshotsAreFilteredOut() throws Exception {
        HashMap<String, SnapshotId> snapshots = new HashMap<>();
        SnapshotId s1 = new SnapshotId("s1", UUIDs.randomBase64UUID());
        SnapshotId s2 = new SnapshotId("s2", UUIDs.randomBase64UUID());
        SnapshotId s3 = new SnapshotId("s3", UUIDs.randomBase64UUID());
        snapshots.put(s1.getUUID(), s1);
        snapshots.put(s2.getUUID(), s2);
        snapshots.put(s3.getUUID(), s3);
        RepositoryData repositoryData = new RepositoryData(
            1, snapshots, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), ShardGenerations.EMPTY, IndexMetaDataGenerations.EMPTY);

        Repository r1 = mock(Repository.class);
        doReturn(CompletableFuture.completedFuture(repositoryData))
            .when(r1)
            .getRepositoryData();

        when(r1.getMetadata()).thenReturn(new RepositoryMetadata("repo1", "fs", Settings.EMPTY));

        Metadata metadata = mock(Metadata.class);
        when(metadata.templates()).thenReturn(ImmutableOpenMap.of());

        // s1 fails for SnapshotInfo
        doReturn(CompletableFuture.completedFuture(metadata))
            .when(r1)
            .getSnapshotGlobalMetadata(s1);
        doReturn(CompletableFuture.failedFuture(new SnapshotException("repo1", "s1", "Everything is wrong")))
            .when(r1)
            .getSnapshotInfo(s1);

        // s1 fails for GlobalMetadata
        doReturn(CompletableFuture.failedFuture(new SnapshotException("repo1", "s2", "Everything is wrong")))
            .when(r1)
            .getSnapshotGlobalMetadata(s2);
        doReturn(CompletableFuture.completedFuture(new SnapshotInfo(s2, Collections.emptyList(), SnapshotState.SUCCESS)))
            .when(r1)
            .getSnapshotInfo(s2);

        // s3 succeeds in both
        doReturn(CompletableFuture.completedFuture(metadata))
            .when(r1)
            .getSnapshotGlobalMetadata(s3);
        doReturn(CompletableFuture.completedFuture(new SnapshotInfo(s3, Collections.emptyList(), SnapshotState.SUCCESS)))
            .when(r1)
            .getSnapshotInfo(s3);

        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        when(clusterService.state().custom(SnapshotsInProgress.TYPE)).thenReturn(null);
        SysSnapshots sysSnapshots = new SysSnapshots(() -> Collections.singletonList(r1), clusterService);
        Stream<SysSnapshot> currentSnapshots = StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(sysSnapshots.currentSnapshots().get().iterator(), Spliterator.ORDERED),
            false
        );
        List<String> names = new ArrayList<>();
        List<String> uuids = new ArrayList<>();
        currentSnapshots.forEach(sysSnapshot -> {
            names.add(sysSnapshot.name());
            uuids.add(sysSnapshot.uuid());
        });
        assertThat(names).containsExactlyInAnyOrder("s1", "s2", "s3");
        assertThat(uuids).containsExactlyInAnyOrder(s1.getUUID(), s2.getUUID(), s3.getUUID());
    }

    @Test
    public void test_global_state_and_reason_and_total_shards_are_exposed_in_sys_snapshots() throws Exception {
        HashMap<String, SnapshotId> snapshots = new HashMap<>();
        SnapshotId sid1 = new SnapshotId("s1", UUIDs.randomBase64UUID());
        snapshots.put(sid1.getUUID(), sid1);

        RepositoryData repositoryData = new RepositoryData(
            1, snapshots, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), ShardGenerations.EMPTY, IndexMetaDataGenerations.EMPTY);

        Repository r1 = mock(Repository.class);
        doReturn(CompletableFuture.completedFuture(repositoryData))
            .when(r1)
            .getRepositoryData();

        when(r1.getMetadata()).thenReturn(new RepositoryMetadata("repo1", "fs", Settings.EMPTY));

        Metadata metadata = mock(Metadata.class);
        when(metadata.templates()).thenReturn(ImmutableOpenMap.of());

        doReturn(CompletableFuture.completedFuture(metadata))
            .when(r1)
            .getSnapshotGlobalMetadata(sid1);

        int totalShards = 4;
        String reason = "dummy";
        boolean includeGlobalState = false;
        doReturn(CompletableFuture.completedFuture(
            new SnapshotInfo(
                sid1,
                List.of(),
                1,
                reason,
                2,
                totalShards,
                List.of(),
                includeGlobalState
            )
        ))
            .when(r1)
            .getSnapshotInfo(sid1);

        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        when(clusterService.state().custom(SnapshotsInProgress.TYPE)).thenReturn(null);
        SysSnapshots sysSnapshots = new SysSnapshots(() -> Collections.singletonList(r1), clusterService);
        Iterable<SysSnapshot> iterable = sysSnapshots.currentSnapshots().get();
        SysSnapshot sysSnapshot = iterable.iterator().next();

        assertThat(sysSnapshot.name()).isEqualTo("s1");
        assertThat(sysSnapshot.reason()).isEqualTo(reason);
        assertThat(sysSnapshot.includeGlobalState()).isEqualTo(includeGlobalState);
        assertThat(sysSnapshot.totalShards()).isEqualTo(totalShards);
    }

    @Test
    public void test_unavailable_snapshot_has_zero_total_shards_and_null_include_global_state() throws Exception {
        HashMap<String, SnapshotId> snapshots = new HashMap<>();
        SnapshotId sid1 = new SnapshotId("s1", UUIDs.randomBase64UUID());
        snapshots.put(sid1.getUUID(), sid1);

        RepositoryData repositoryData = new RepositoryData(
            1, snapshots, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), ShardGenerations.EMPTY, IndexMetaDataGenerations.EMPTY);

        Repository r1 = mock(Repository.class);
        doReturn(CompletableFuture.completedFuture(repositoryData))
            .when(r1)
            .getRepositoryData();

        when(r1.getMetadata()).thenReturn(new RepositoryMetadata("repo1", "fs", Settings.EMPTY));

        Metadata metadata = mock(Metadata.class);
        when(metadata.templates()).thenReturn(ImmutableOpenMap.of());

        doReturn(CompletableFuture.completedFuture(metadata))
            .when(r1)
            .getSnapshotGlobalMetadata(sid1);
        doReturn(CompletableFuture.failedFuture(new SnapshotException("repo1", "s1", "Everything is wrong")))
            .when(r1)
            .getSnapshotInfo(sid1);

        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        when(clusterService.state().custom(SnapshotsInProgress.TYPE)).thenReturn(null);
        SysSnapshots sysSnapshots = new SysSnapshots(() -> Collections.singletonList(r1), clusterService);
        Iterable<SysSnapshot> iterable = sysSnapshots.currentSnapshots().get();
        SysSnapshot sysSnapshot = iterable.iterator().next();
        Assertions.assertThat(sysSnapshot.name()).isEqualTo("s1");
        Assertions.assertThat(sysSnapshot.reason()).isNull();
        Assertions.assertThat(sysSnapshot.includeGlobalState()).isNull();
        Assertions.assertThat(sysSnapshot.totalShards()).isEqualTo(0);
    }

    @Test
    public void test_current_snapshot_does_not_fail_if_get_repository_data_returns_failed_future() throws Exception {
        Repository r1 = mock(Repository.class, Answers.RETURNS_DEEP_STUBS);
        Mockito
            .doReturn(CompletableFuture.failedFuture(new IllegalStateException("some error")))
            .when(r1).getRepositoryData();
        when(r1.getMetadata().name()).thenReturn(null); // Not important for this test, avoiding NPE.

        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        when(clusterService.state().custom(SnapshotsInProgress.TYPE)).thenReturn(null);
        SysSnapshots sysSnapshots = new SysSnapshots(() -> List.of(r1), clusterService);
        CompletableFuture<Iterable<SysSnapshot>> currentSnapshots = sysSnapshots.currentSnapshots();
        Iterable<SysSnapshot> iterable = currentSnapshots.get(5, TimeUnit.SECONDS);
        assertThat(iterable.iterator().hasNext()).isFalse();
    }

    @Test
    public void test_snapshot_in_progress_shown_in_sys_snapshots() throws Exception {
        RepositoryData repositoryData = new RepositoryData(
            1,
            Collections.emptyMap(), // No completed snapshots
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY
        );

        Repository r1 = mock(Repository.class);
        doReturn(CompletableFuture.completedFuture(repositoryData))
            .when(r1)
            .getRepositoryData();

        String repositoryName = "repo";
        String snapshot = "snapshot";
        when(r1.getMetadata()).thenReturn(new RepositoryMetadata(repositoryName, "fs", Settings.EMPTY));

        SnapshotId snapshotId = new SnapshotId(snapshot, UUIDs.randomBase64UUID());
        RelationName relationName = new RelationName("my_schema", "empty_parted_table");
        SnapshotsInProgress.Entry entry = SnapshotsInProgress.startedEntry(
            new Snapshot(repositoryName, snapshotId),
            true,
            true,
            List.of(),
            List.of(relationName),
            123L,
            1L,
            ImmutableOpenMap.of(),
            Version.CURRENT
        );
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS);
        when(clusterService.state().custom(SnapshotsInProgress.TYPE)).thenReturn(SnapshotsInProgress.of(List.of(entry)));
        List<String> names = new ArrayList<>();
        List<List<String>> tables = new ArrayList<>();
        SysSnapshots sysSnapshots = new SysSnapshots(() -> Collections.singletonList(r1), clusterService);
        sysSnapshots.currentSnapshots().get().forEach(sysSnapshot -> {
            names.add(sysSnapshot.name());
            tables.add(sysSnapshot.tables());
        });
        assertThat(names).containsExactlyInAnyOrder("snapshot");
        assertThat(tables).hasSize(1);
        assertThat(tables.get(0)).containsExactlyInAnyOrder("my_schema.empty_parted_table");
    }
}
