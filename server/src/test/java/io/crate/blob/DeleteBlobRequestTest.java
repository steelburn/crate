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

package io.crate.blob;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.Hex;

public class DeleteBlobRequestTest extends ESTestCase {

    @Test
    public void testDeleteBlobRequestStreaming() throws Exception {
        byte[] digest = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        String uuid = UUIDs.randomBase64UUID();
        DeleteBlobRequest request = new DeleteBlobRequest(
            new ShardId("foo", uuid, 1),
            digest
        );
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        DeleteBlobRequest fromStream = new DeleteBlobRequest(out.bytes().streamInput());

        assertThat(fromStream.shardId().getIndexUUID()).isEqualTo(uuid);
        assertThat(fromStream.id()).isEqualTo(Hex.encodeHexString(digest));
    }
}
