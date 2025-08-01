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
 * KIND, either express or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.shrink;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.ddl.index.SwapAndDropIndexRequest;
import io.crate.execution.ddl.index.TransportSwapAndDropIndexName;
import io.crate.execution.ddl.tables.GCDanglingArtifactsRequest;
import io.crate.execution.ddl.tables.TransportGCDanglingArtifacts;
import io.crate.metadata.PartitionName;

/**
 * Main class to initiate resizing (shrink / split) an index into a new index
 */
public class TransportResize extends TransportMasterNodeAction<ResizeRequest, ResizeResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<ResizeResponse> {
        private static final String NAME = "indices:admin/resize";

        private Action() {
            super(NAME);
        }
    }

    private final MetadataCreateIndexService createIndexService;
    private final Client client;
    private final TransportSwapAndDropIndexName swapAndDropIndexAction;
    private final TransportGCDanglingArtifacts gcDanglingArtifactsAction;

    @Inject
    public TransportResize(TransportService transportService,
                           ClusterService clusterService,
                           ThreadPool threadPool,
                           MetadataCreateIndexService createIndexService,
                           TransportSwapAndDropIndexName swapAndDropIndexAction,
                           TransportGCDanglingArtifacts gcDanglingArtifactsAction,
                           Client client) {
        super(ACTION.name(), transportService, clusterService, threadPool, ResizeRequest::new);
        this.createIndexService = createIndexService;
        this.swapAndDropIndexAction = swapAndDropIndexAction;
        this.gcDanglingArtifactsAction = gcDanglingArtifactsAction;
        this.client = client;
    }


    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ResizeResponse read(StreamInput in) throws IOException {
        return new ResizeResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ResizeRequest request, ClusterState state) {
        String[] indices = state.metadata().getIndices(
            request.table(),
            request.partitionValues(),
            false,
            idxMd -> idxMd.getIndex().getUUID()
        ).toArray(String[]::new);
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices);
    }

    @Override
    protected void masterOperation(final ResizeRequest request,
                                   final ClusterState state,
                                   final ActionListener<ResizeResponse> listener) {
        if (state.nodes().getMinNodeVersion().onOrAfter(Version.V_6_0_0) == false) {
            throw new IllegalStateException("Cannot resize a table/partition until all nodes are upgraded to 6.0");
        }

        final String sourceIndexUUID = state.metadata().getIndex(request.table(), request.partitionValues(), true, IndexMetadata::getIndexUUID);
        final String resizedIndexUUID = UUIDs.randomBase64UUID();

        client.stats(new IndicesStatsRequest(new PartitionName(request.table(), request.partitionValues()))
            .clear()
            .docs(true))
            .thenCompose(statsResponse -> createIndexService.resizeIndex(request, sourceIndexUUID, resizedIndexUUID, statsResponse))
            .thenCompose(resizeResp -> {
                if (resizeResp.isAcknowledged() && resizeResp.isShardsAcknowledged()) {
                    SwapAndDropIndexRequest req = new SwapAndDropIndexRequest(resizedIndexUUID, sourceIndexUUID);
                    return swapAndDropIndexAction.execute(req).thenApply(ignored -> resizeResp);
                } else {
                    return gcDanglingArtifactsAction.execute(GCDanglingArtifactsRequest.INSTANCE).handle(
                        (ignored, err) -> {
                            throw new IllegalStateException(
                                "Resize operation wasn't acknowledged. Check shard allocation and retry", err);
                        });
                }
            })

            .whenComplete(listener);
    }
}
