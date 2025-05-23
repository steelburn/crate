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

package org.elasticsearch.action.admin.cluster.configuration;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateObserver.Listener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import io.crate.common.unit.TimeValue;

public class TransportAddVotingConfigExclusions
    extends TransportMasterNodeAction<AddVotingConfigExclusionsRequest, AddVotingConfigExclusionsResponse> {

    public static final Setting<Integer> MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING
        = Setting.intSetting("cluster.max_voting_config_exclusions", 10, 1, Property.Dynamic, Property.NodeScope);

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<AddVotingConfigExclusionsResponse> {
        private static final String NAME = "cluster:admin/voting_config/add_exclusions";

        private Action() {
            super(NAME);
        }
    }

    @Inject
    public TransportAddVotingConfigExclusions(TransportService transportService,
                                              ClusterService clusterService,
                                              ThreadPool threadPool) {
        super(ACTION.name(), transportService, clusterService, threadPool,
            AddVotingConfigExclusionsRequest::new);
    }

    @Override
    protected String executor() {
        return Names.SAME;
    }

    @Override
    protected AddVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
        return new AddVotingConfigExclusionsResponse(in);
    }

    @Override
    protected void masterOperation(AddVotingConfigExclusionsRequest request,
                                   ClusterState state,
                                   ActionListener<AddVotingConfigExclusionsResponse> listener) throws Exception {

        resolveVotingConfigExclusionsAndCheckMaximum(request, state); // throws IAE if no nodes matched or maximum exceeded

        clusterService.submitStateUpdateTask("add-voting-config-exclusions", new ClusterStateUpdateTask(Priority.URGENT) {

            private Set<VotingConfigExclusion> resolvedExclusions;

            @Override
            public ClusterState execute(ClusterState currentState) {
                assert resolvedExclusions == null : resolvedExclusions;
                resolvedExclusions = resolveVotingConfigExclusionsAndCheckMaximum(request, currentState);

                final CoordinationMetadata.Builder builder = CoordinationMetadata.builder(currentState.coordinationMetadata());
                resolvedExclusions.forEach(builder::addVotingConfigExclusion);
                final Metadata newMetadata = Metadata.builder(currentState.metadata()).coordinationMetadata(builder.build()).build();
                final ClusterState newState = ClusterState.builder(currentState).metadata(newMetadata).build();
                assert newState.getVotingConfigExclusions().size() <= MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(
                    currentState.metadata().settings());
                return newState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

                final ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.getTimeout(), logger);

                final Set<String> excludedNodeIds = resolvedExclusions.stream().map(VotingConfigExclusion::getNodeId)
                    .collect(Collectors.toSet());

                final Predicate<ClusterState> allNodesRemoved = clusterState -> {
                    final Set<String> votingConfigNodeIds = clusterState.getLastCommittedConfiguration().getNodeIds();
                    return excludedNodeIds.stream().noneMatch(votingConfigNodeIds::contains);
                };

                final Listener clusterStateListener = new Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        listener.onResponse(new AddVotingConfigExclusionsResponse());
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new ElasticsearchException("cluster service closed while waiting for voting config exclusions " +
                            resolvedExclusions + " to take effect"));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(new ElasticsearchTimeoutException("timed out waiting for voting config exclusions "
                            + resolvedExclusions + " to take effect"));
                    }
                };

                if (allNodesRemoved.test(newState)) {
                    clusterStateListener.onNewClusterState(newState);
                } else {
                    observer.waitForNextChange(clusterStateListener, allNodesRemoved);
                }
            }
        });
    }

    private static Set<VotingConfigExclusion> resolveVotingConfigExclusionsAndCheckMaximum(AddVotingConfigExclusionsRequest request,
                                                                                           ClusterState state) {
        return request.resolveVotingConfigExclusionsAndCheckMaximum(state,
            MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(state.metadata().settings()), MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey());
    }

    @Override
    protected ClusterBlockException checkBlock(AddVotingConfigExclusionsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
