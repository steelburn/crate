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

package org.elasticsearch.action.resync;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

/**
 * Represents a batch of operations sent from the primary to its replicas during the primary-replica resync.
 */
public final class ResyncReplicationRequest extends ReplicationRequest<ResyncReplicationRequest> {

    private final long trimAboveSeqNo;
    private final Translog.Operation[] operations;
    private final long maxSeenAutoIdTimestampOnPrimary;

    public ResyncReplicationRequest(final ShardId shardId, final long trimAboveSeqNo, final long maxSeenAutoIdTimestampOnPrimary,
                                    final Translog.Operation[]operations) {
        super(shardId);
        this.trimAboveSeqNo = trimAboveSeqNo;
        this.maxSeenAutoIdTimestampOnPrimary = maxSeenAutoIdTimestampOnPrimary;
        this.operations = operations;
    }

    public long getTrimAboveSeqNo() {
        return trimAboveSeqNo;
    }

    public long getMaxSeenAutoIdTimestampOnPrimary() {
        return maxSeenAutoIdTimestampOnPrimary;
    }

    public Translog.Operation[] getOperations() {
        return operations;
    }

    public ResyncReplicationRequest(final StreamInput in) throws IOException {
        super(in);
        trimAboveSeqNo = in.readZLong();
        maxSeenAutoIdTimestampOnPrimary = in.readZLong();
        operations = in.readArray(Translog.Operation::readOperation, Translog.Operation[]::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeZLong(trimAboveSeqNo);
        out.writeZLong(maxSeenAutoIdTimestampOnPrimary);
        out.writeArray(Translog.Operation::writeOperation, operations);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ResyncReplicationRequest that = (ResyncReplicationRequest) o;
        return trimAboveSeqNo == that.trimAboveSeqNo && maxSeenAutoIdTimestampOnPrimary == that.maxSeenAutoIdTimestampOnPrimary
            && Arrays.equals(operations, that.operations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trimAboveSeqNo, maxSeenAutoIdTimestampOnPrimary, operations);
    }

    @Override
    public String toString() {
        return "TransportResyncReplicationAction.Request{" +
            "shardId=" + shardId +
            ", timeout=" + timeout +
            ", trimAboveSeqNo=" + trimAboveSeqNo +
            ", maxSeenAutoIdTimestampOnPrimary=" + maxSeenAutoIdTimestampOnPrimary +
            ", ops=" + operations.length +
            "}";
    }

}
