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

package org.elasticsearch.action.support.replication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportService;

import io.crate.concurrent.MultiActionListener;
import io.crate.exceptions.SQLExceptions;

/**
 * Base class for requests that should be executed on all shards of an index or several indices.
 * This action sends shard requests to all primary shards of the indices and they are then replicated like write requests
 * <p/>
 * The indices set on a BroadcastRequest should already be resolved to concrete index names
 */
public abstract class TransportBroadcastReplicationAction<Request extends BroadcastRequest, Response extends BroadcastResponse, ShardRequest extends ReplicationRequest<ShardRequest>, ShardResponse extends ReplicationResponse>
        extends HandledTransportAction<Request, Response> {

    private final ActionType<ShardResponse> replicatedBroadcastShardAction;
    private final ClusterService clusterService;
    private final NodeClient client;

    public TransportBroadcastReplicationAction(String name,
                                               Writeable.Reader<Request> reader,
                                               ClusterService clusterService,
                                               TransportService transportService,
                                               NodeClient client,
                                               ActionType<ShardResponse> replicatedBroadcastShardAction) {
        super(name, transportService, reader);
        this.client = client;
        this.replicatedBroadcastShardAction = replicatedBroadcastShardAction;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        final ClusterState clusterState = clusterService.state();
        List<ShardId> shards = shards(request, clusterState);
        ActionListener<ShardResponse> multiListener = new MultiActionListener<>(
            shards.size(),
            Collectors.collectingAndThen(Collectors.toList(), this::mergeResponse),
            listener
        );
        for (final ShardId shardId : shards) {
            shardExecute(request, shardId, ActionListener.wrap(multiListener::onResponse, e -> {
                int totalNumCopies = clusterState.metadata().getIndexSafe(shardId.getIndex()).getNumberOfReplicas() + 1;
                ShardResponse shardResponse = newShardResponse();
                ReplicationResponse.ShardInfo.Failure[] failures;
                if (SQLExceptions.isShardNotAvailable(e)) {
                    failures = new ReplicationResponse.ShardInfo.Failure[0];
                } else {
                    ReplicationResponse.ShardInfo.Failure failure = new ReplicationResponse.ShardInfo.Failure(
                        shardId,
                        null,
                        e,
                        SQLExceptions.status(e),
                        true
                    );
                    failures = new ReplicationResponse.ShardInfo.Failure[totalNumCopies];
                    Arrays.fill(failures, failure);
                }
                shardResponse.setShardInfo(new ReplicationResponse.ShardInfo(totalNumCopies, 0, failures));
                multiListener.onResponse(shardResponse);
            }));
        }
    }

    protected void shardExecute(Request request, ShardId shardId, ActionListener<ShardResponse> shardActionListener) {
        ShardRequest shardRequest = newShardRequest(request, shardId);
        client.execute(replicatedBroadcastShardAction, shardRequest).whenComplete(shardActionListener);
    }

    /**
     * @return all shard ids the request should run on
     */
    protected List<ShardId> shards(Request request, ClusterState clusterState) {
        List<ShardId> shardIds = new ArrayList<>();
        for (IndexRoutingTable routing : clusterState.metadata().getIndices(
            request.partitions(), false, im -> clusterState.routingTable().indicesRouting().get(im.getIndex().getUUID())
        )) {
            for (var shardRouting : routing.getShards()) {
                shardIds.add(shardRouting.value.shardId());
            }
        }
        return shardIds;
    }

    protected abstract ShardResponse newShardResponse();

    protected abstract ShardRequest newShardRequest(Request request, ShardId shardId);

    private Response mergeResponse(List<ShardResponse> shardsResponses) {
        logger.trace("{}: got all shard responses", actionName);
        int successfulShards = 0;
        int failedShards = 0;
        int totalNumCopies = 0;
        List<DefaultShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.size(); i++) {
            ReplicationResponse shardResponse = shardsResponses.get(i);
            if (shardResponse != null) {
                failedShards += shardResponse.getShardInfo().getFailed();
                successfulShards += shardResponse.getShardInfo().getSuccessful();
                totalNumCopies += shardResponse.getShardInfo().getTotal();
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                for (ReplicationResponse.ShardInfo.Failure failure : shardResponse.getShardInfo().getFailures()) {
                    shardFailures.add(new DefaultShardOperationFailedException(new BroadcastShardOperationFailedException(failure.fullShardId(), failure.getCause())));
                }
            }
        }
        return newResponse(successfulShards, failedShards, totalNumCopies, shardFailures);
    }

    protected abstract Response newResponse(int successfulShards,
                                            int failedShards,
                                            int totalNumCopies,
                                            List<DefaultShardOperationFailedException> shardFailures);
}
