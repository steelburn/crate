/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.RelationMetadata;
import io.crate.role.Role;
import io.crate.role.Roles;

public class PublicationsStateAction extends ActionType<PublicationsStateAction.Response> {

    public static final String NAME = "internal:crate:replication/logical/publication/state";
    public static final PublicationsStateAction INSTANCE = new PublicationsStateAction();

    private static final Logger LOGGER = LogManager.getLogger(PublicationsStateAction.class);

    public PublicationsStateAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    @Singleton
    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        private final Roles roles;

        @Inject
        public TransportAction(TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool,
                               Roles roles) {
            super(Settings.EMPTY,
                  NAME,
                  false,
                  transportService,
                  clusterService,
                  threadPool,
                  Request::new);
            this.roles = roles;

            TransportActionProxy.registerProxyAction(transportService, NAME, Response::new);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void masterOperation(Request request,
                                       ClusterState state,
                                       ActionListener<Response> listener) throws Exception {
            // Ensure subscribing user was not dropped after remote connection was established on another side.
            // Subscribing users must be checked on a publisher side as they belong to the publishing cluster.
            Role subscriber = roles.findUser(request.subscribingUserName());
            if (subscriber == null) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ENGLISH, "Cannot build publication state, subscribing user '%s' was not found.",
                        request.subscribingUserName()
                    )
                );
            }

            PublicationsMetadata publicationsMetadata = state.metadata().custom(PublicationsMetadata.TYPE);
            if (publicationsMetadata == null) {
                LOGGER.trace("No publications found on remote cluster.");
                throw new IllegalStateException("Cannot build publication state, no publications found");
            }

            Metadata.Builder metadataBuilder = Metadata.builder();
            List<String> unknownPublications = new ArrayList<>();
            for (var publicationName : request.publications()) {
                var publication = publicationsMetadata.publications().get(publicationName);
                if (publication == null) {
                    unknownPublications.add(publicationName);
                    continue;
                }

                // Publication owner cannot be null as we ensure that users who own publication cannot be dropped.
                // Also, before creating publication or subscription we check that owner was not dropped right before creation.
                Role publicationOwner = roles.findUser(publication.owner());
                publication.resolveCurrentRelations(
                    state,
                    roles,
                    publicationOwner,
                    subscriber,
                    publicationName,
                    metadataBuilder
                );
            }
            listener.onResponse(new Response(metadataBuilder.build(), unknownPublications));
        }

        @Override
        protected ClusterBlockException checkBlock(Request request,
                                                   ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final List<String> publications;
        private final String subscribingUserName;

        public Request(List<String> publications, String subscribingUserName) {
            this.publications = publications;
            this.subscribingUserName = subscribingUserName;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            publications = in.readList(StreamInput::readString);
            subscribingUserName = in.readString();
        }

        public List<String> publications() {
            return publications;
        }

        public String subscribingUserName() {
            return subscribingUserName;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(publications);
            out.writeString(subscribingUserName);
        }
    }

    public static class Response extends TransportResponse {

        private final Metadata metadata;
        private final List<String> unknownPublications;
        /**
         * The action is registered as a proxy action,
         * thus a node may forward the request/response to/from its master node.
         * In such cases, the received metadata won't be upgraded,
         * and we cannot recreate this old payload using the new RelationMetadata.
         * The payload is then stored and send out as-is.
         */
        @Nullable
        private final Map<RelationName, RelationMetadata> relationsInPublications;

        public Response(Metadata metadata, List<String> unknownPublications) {
            this.metadata = metadata;
            this.unknownPublications = unknownPublications;
            this.relationsInPublications = null;
        }

        public Response(StreamInput in) throws IOException {
            if (in.getVersion().before(Version.V_6_0_0)) {
                Map<RelationName, RelationMetadata> relationsInPublications = in.readMap(RelationName::new, RelationMetadata::new);
                this.relationsInPublications = relationsInPublications;
                Metadata.Builder mdBuilder = Metadata.builder();
                for (Map.Entry<RelationName, RelationMetadata> entry : relationsInPublications.entrySet()) {
                    RelationMetadata relationMetadata = entry.getValue();
                    for (IndexMetadata indexMetadata : relationMetadata.indices()) {
                        mdBuilder.put(indexMetadata, false);
                    }
                    IndexTemplateMetadata templateMetadata = relationMetadata.template();
                    if (templateMetadata != null) {
                        mdBuilder.put(templateMetadata);
                    }
                }
                metadata = mdBuilder.build();
            } else {
                relationsInPublications = null;
                metadata = Metadata.readFrom(in);
            }
            unknownPublications = in.readList(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().before(Version.V_6_0_0)) {
                Map<RelationName, RelationMetadata> relationsInPublications = this.relationsInPublications != null ?
                    this.relationsInPublications :
                    metadata.relations(org.elasticsearch.cluster.metadata.RelationMetadata.Table.class).stream()
                        .map(table -> RelationMetadata.fromMetadata(table, metadata))
                        .collect(Collectors.toMap(RelationMetadata::name, x -> x));
                out.writeMap(relationsInPublications, (o, v) -> v.writeTo(out), (o, v) -> v.writeTo(out));
            } else {
                metadata.writeTo(out);
            }
            out.writeStringCollection(unknownPublications);
        }

        public Metadata metadata() {
            return metadata;
        }

        public List<String> unknownPublications() {
            return unknownPublications;
        }

        @Override
        public String toString() {
            return "Response{" + "metadata:" + metadata + ", unknownPublications:" + unknownPublications + '}';
        }
    }
}
