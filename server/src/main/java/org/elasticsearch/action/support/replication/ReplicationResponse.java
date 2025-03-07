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

import java.io.IOException;
import java.util.Arrays;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportResponse;
import org.jetbrains.annotations.Nullable;

import io.crate.common.exceptions.Exceptions;

/**
 * Base class for write action responses.
 */
public class ReplicationResponse extends TransportResponse {

    public static final ReplicationResponse.ShardInfo.Failure[] EMPTY = new ReplicationResponse.ShardInfo.Failure[0];

    private ShardInfo shardInfo;

    public ReplicationResponse() {
    }

    public ReplicationResponse(StreamInput in) throws IOException {
        shardInfo = new ReplicationResponse.ShardInfo(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardInfo.writeTo(out);
    }

    public ShardInfo getShardInfo() {
        return shardInfo;
    }

    public void setShardInfo(ShardInfo shardInfo) {
        this.shardInfo = shardInfo;
    }

    public static class ShardInfo implements Writeable {

        private final int total;
        private final int successful;
        private final Failure[] failures;

        public ShardInfo(int total, int successful, Failure... failures) {
            assert total >= 0 && successful >= 0;
            this.total = total;
            this.successful = successful;
            this.failures = failures;
        }

        public ShardInfo(StreamInput in) throws IOException {
            total = in.readVInt();
            successful = in.readVInt();
            int size = in.readVInt();
            failures = new Failure[size];
            for (int i = 0; i < size; i++) {
                failures[i] = new Failure(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(successful);
            out.writeVInt(failures.length);
            for (Failure failure : failures) {
                failure.writeTo(out);
            }
        }

        /**
         * @return the total number of shards the write should go to (replicas and primaries). This includes relocating shards, so this
         *         number can be higher than the number of shards.
         */
        public int getTotal() {
            return total;
        }

        /**
         * @return the total number of shards the write succeeded on (replicas and primaries). This includes relocating shards, so this
         *         number can be higher than the number of shards.
         */
        public int getSuccessful() {
            return successful;
        }

        /**
         * @return The total number of replication failures.
         */
        public int getFailed() {
            return failures.length;
        }

        /**
         * @return The replication failures that have been captured in the case writes have failed on replica shards.
         */
        public Failure[] getFailures() {
            return failures;
        }

        public RestStatus status() {
            RestStatus status = RestStatus.OK;
            for (Failure failure : failures) {
                if (failure.primary() && failure.status().getStatus() > status.getStatus()) {
                    status = failure.status();
                }
            }
            return status;
        }

        @Override
        public String toString() {
            return "ShardInfo{" +
                "total=" + total +
                ", successful=" + successful +
                ", failures=" + Arrays.toString(failures) +
                '}';
        }

        public static class Failure extends ShardOperationFailedException {

            private final ShardId shardId;
            private final String nodeId;
            private final boolean primary;

            public Failure(ShardId shardId,
                           @Nullable String nodeId,
                           Exception cause,
                           RestStatus status,
                           boolean primary) {
                super(shardId.getIndexName(), shardId.id(), Exceptions.stackTrace(cause), status, cause);
                this.shardId = shardId;
                this.nodeId = nodeId;
                this.primary = primary;
            }

            public ShardId fullShardId() {
                return shardId;
            }

            /**
             * @return On what node the failure occurred.
             */
            @Nullable
            public String nodeId() {
                return nodeId;
            }

            /**
             * @return Whether this failure occurred on a primary shard.
             * (this only reports true for delete by query)
             */
            public boolean primary() {
                return primary;
            }

            public Failure(StreamInput in) throws IOException {
                shardId = new ShardId(in);
                super.shardId = shardId.id();
                index = shardId.getIndexName();
                nodeId = in.readOptionalString();
                cause = in.readException();
                status = RestStatus.readFrom(in);
                primary = in.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                shardId.writeTo(out);
                out.writeOptionalString(nodeId);
                out.writeException(cause);
                RestStatus.writeTo(out, status);
                out.writeBoolean(primary);
            }
        }
    }
}
