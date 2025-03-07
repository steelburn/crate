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
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.transport.TransportResponse;
import org.jetbrains.annotations.Nullable;

/**
 * Create snapshot response
 */
public class CreateSnapshotResponse extends TransportResponse {

    @Nullable
    private SnapshotInfo snapshotInfo;

    CreateSnapshotResponse(@Nullable SnapshotInfo snapshotInfo) {
        this.snapshotInfo = snapshotInfo;
    }

    CreateSnapshotResponse() {
    }

    /**
     * Returns snapshot information if snapshot was completed by the time this method returned or null otherwise.
     *
     * @return snapshot information or null
     */
    public SnapshotInfo getSnapshotInfo() {
        return snapshotInfo;
    }

    public CreateSnapshotResponse(StreamInput in) throws IOException {
        snapshotInfo = in.readOptionalWriteable(SnapshotInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(snapshotInfo);
    }

    /**
     * Returns HTTP status
     * <ul>
     * <li>{@link RestStatus#ACCEPTED} if snapshot is still in progress</li>
     * <li>{@link RestStatus#OK} if snapshot was successful or partially successful</li>
     * <li>{@link RestStatus#INTERNAL_SERVER_ERROR} if snapshot failed completely</li>
     * </ul>
     */
    public RestStatus status() {
        if (snapshotInfo == null) {
            return RestStatus.ACCEPTED;
        }
        return snapshotInfo.status();
    }

    @Override
    public String toString() {
        return "CreateSnapshotResponse{" +
            "snapshotInfo=" + snapshotInfo +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateSnapshotResponse that = (CreateSnapshotResponse) o;
        return Objects.equals(snapshotInfo, that.snapshotInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotInfo);
    }
}
