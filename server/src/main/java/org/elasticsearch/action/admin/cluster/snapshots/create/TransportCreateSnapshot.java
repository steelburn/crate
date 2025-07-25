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

package org.elasticsearch.action.admin.cluster.snapshots.create;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for create snapshot operation
 */
public class TransportCreateSnapshot extends TransportMasterNodeAction<CreateSnapshotRequest, CreateSnapshotResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<CreateSnapshotResponse> {
        private static final String NAME = "cluster:admin/snapshot/create";

        private Action() {
            super(NAME);
        }
    }

    private final SnapshotsService snapshotsService;

    @Inject
    public TransportCreateSnapshot(TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, SnapshotsService snapshotsService) {
        super(ACTION.name(), transportService, clusterService, threadPool,
            CreateSnapshotRequest::new);
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected String executor() {
        // Using the generic instead of the snapshot threadpool here as the snapshot threadpool might be blocked on long running tasks
        // which would block the request from getting an error response because of the ongoing task
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected CreateSnapshotResponse read(StreamInput in) throws IOException {
        return new CreateSnapshotResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateSnapshotRequest request, ClusterState state) {
        // We are reading the cluster metadata and indices - so we need to check both blocks
        ClusterBlockException clusterBlockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        if (clusterBlockException != null) {
            return clusterBlockException;
        }
        String[] indices = request.relationNames().stream()
            .map(r ->
                state.metadata().getIndices(r, List.of(), false, IndexMetadata::getIndexUUID))
            .flatMap(Collection::stream)
            .toArray(String[]::new);
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.READ, indices);
    }

    @Override
    protected void masterOperation(final CreateSnapshotRequest request,
                                   ClusterState state,
                                   final ActionListener<CreateSnapshotResponse> listener) {
        if (request.waitForCompletion()) {
            snapshotsService.executeSnapshot(request, listener.map(CreateSnapshotResponse::new));
        } else {
            snapshotsService.createSnapshot(request, listener.map(_ -> new CreateSnapshotResponse()));
        }
    }
}
