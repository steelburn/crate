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

package org.elasticsearch.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

public class InboundDecoderTests extends ESTestCase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testDecode() throws IOException {
        boolean isRequest = randomBoolean();
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        OutboundMessage message;
        if (isRequest) {
            message = new OutboundMessage.Request(new TestRequest(randomAlphaOfLength(100)),
                Version.CURRENT, action, requestId, false, false);
        } else {
            message = new OutboundMessage.Response(new TestResponse(randomAlphaOfLength(100)),
                Version.CURRENT, requestId, false, false);
        }

        final BytesReference totalBytes = message.serialize(new BytesStreamOutput());
        int totalHeaderSize = TcpHeader.headerSize(Version.CURRENT) + totalBytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
        final BytesReference messageBytes = totalBytes.slice(totalHeaderSize, totalBytes.length() - totalHeaderSize);

        InboundDecoder decoder = new InboundDecoder(PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertThat(bytesConsumed).isEqualTo(totalHeaderSize);
        assertThat(releasable1.refCount()).isEqualTo(1);

        final Header header = (Header) fragments.get(0);
        assertThat(header.getRequestId()).isEqualTo(requestId);
        assertThat(header.getVersion()).isEqualTo(Version.CURRENT);
        assertThat(header.isCompressed()).isFalse();
        assertThat(header.isHandshake()).isFalse();
        if (isRequest) {
            assertThat(header.getActionName()).isEqualTo(action);
            assertThat(header.isRequest()).isTrue();
        } else {
            assertThat(header.isResponse()).isTrue();
        }
        assertThat(header.needsToReadVariableHeader()).isFalse();
        fragments.clear();

        final BytesReference bytes2 = totalBytes.slice(bytesConsumed, totalBytes.length() - bytesConsumed);
        final ReleasableBytesReference releasable2 = ReleasableBytesReference.wrap(bytes2);
        int bytesConsumed2 = decoder.decode(releasable2, fragments::add);
        assertThat(bytesConsumed2).isEqualTo(totalBytes.length() - totalHeaderSize);

        final Object content = fragments.get(0);
        final Object endMarker = fragments.get(1);

        assertThat(content).isEqualTo(messageBytes);
        // Ref count is incremented since the bytes are forwarded as a fragment
        assertThat(releasable2.refCount()).isEqualTo(2);
        assertThat(endMarker).isEqualTo(InboundDecoder.END_CONTENT);
    }

    @Test
    public void testDecodeHandshakeCompatibility() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        Version handshakeCompat = Version.CURRENT.minimumCompatibilityVersion();
        OutboundMessage message = new OutboundMessage.Request(new TestRequest(randomAlphaOfLength(100)),
            handshakeCompat, action, requestId, true, false);

        final BytesReference bytes = message.serialize(new BytesStreamOutput());
        int totalHeaderSize = TcpHeader.headerSize(handshakeCompat) + bytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);

        InboundDecoder decoder = new InboundDecoder(PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertThat(bytesConsumed).isEqualTo(totalHeaderSize);
        assertThat(releasable1.refCount()).isEqualTo(1);

        final Header header = (Header) fragments.get(0);
        assertThat(header.getRequestId()).isEqualTo(requestId);
        assertThat(header.getVersion()).isEqualTo(handshakeCompat);
        assertThat(header.isCompressed()).isFalse();
        assertThat(header.isHandshake()).isTrue();
        assertThat(header.isRequest()).isTrue();
        assertThat(header.needsToReadVariableHeader()).isFalse();
        fragments.clear();
    }

    @Test
    public void testCompressedDecode() throws IOException {
        boolean isRequest = randomBoolean();
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        OutboundMessage message;
        Writeable transportMessage;
        if (isRequest) {
            transportMessage = new TestRequest(randomAlphaOfLength(100));
            message = new OutboundMessage.Request(transportMessage, Version.CURRENT, action, requestId,
                false, true);
        } else {
            transportMessage = new TestResponse(randomAlphaOfLength(100));
            message = new OutboundMessage.Response(transportMessage, Version.CURRENT, requestId,
                false, true);
        }

        final BytesReference totalBytes = message.serialize(new BytesStreamOutput());
        final BytesStreamOutput out = new BytesStreamOutput();
        transportMessage.writeTo(out);
        final BytesReference uncompressedBytes =out.bytes();
        int totalHeaderSize = TcpHeader.headerSize(Version.CURRENT) + totalBytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);

        InboundDecoder decoder = new InboundDecoder(PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertThat(bytesConsumed).isEqualTo(totalHeaderSize);
        assertThat(releasable1.refCount()).isEqualTo(1);

        final Header header = (Header) fragments.get(0);
        assertThat(header.getRequestId()).isEqualTo(requestId);
        assertThat(header.getVersion()).isEqualTo(Version.CURRENT);
        assertThat(header.isCompressed()).isTrue();
        assertThat(header.isHandshake()).isFalse();
        if (isRequest) {
            assertThat(header.getActionName()).isEqualTo(action);
            assertThat(header.isRequest()).isTrue();
        } else {
            assertThat(header.isResponse()).isTrue();
        }
        assertThat(header.needsToReadVariableHeader()).isFalse();
        fragments.clear();

        final BytesReference bytes2 = totalBytes.slice(bytesConsumed, totalBytes.length() - bytesConsumed);
        final ReleasableBytesReference releasable2 = ReleasableBytesReference.wrap(bytes2);
        int bytesConsumed2 = decoder.decode(releasable2, fragments::add);
        assertThat(bytesConsumed2).isEqualTo(totalBytes.length() - totalHeaderSize);

        final Object content = fragments.get(0);
        final Object endMarker = fragments.get(1);

        assertThat(content).isEqualTo(uncompressedBytes);
        // Ref count is not incremented since the bytes are immediately consumed on decompression
        assertThat(releasable2.refCount()).isEqualTo(1);
        assertThat(endMarker).isEqualTo(InboundDecoder.END_CONTENT);
    }

    @Test
    public void testCompressedDecodeHandshakeCompatibility() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        Version handshakeCompat = Version.CURRENT.minimumCompatibilityVersion();
        OutboundMessage message = new OutboundMessage.Request(new TestRequest(randomAlphaOfLength(100)),
            handshakeCompat, action, requestId, true, true);

        final BytesReference bytes = message.serialize(new BytesStreamOutput());
        int totalHeaderSize = TcpHeader.headerSize(handshakeCompat) + bytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);

        InboundDecoder decoder = new InboundDecoder(PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertThat(bytesConsumed).isEqualTo(totalHeaderSize);
        assertThat(releasable1.refCount()).isEqualTo(1);

        final Header header = (Header) fragments.get(0);
        assertThat(header.getRequestId()).isEqualTo(requestId);
        assertThat(header.getVersion()).isEqualTo(handshakeCompat);
        assertThat(header.isCompressed()).isTrue();
        assertThat(header.isHandshake()).isTrue();
        assertThat(header.isRequest()).isTrue();
        assertThat(header.needsToReadVariableHeader()).isFalse();
        fragments.clear();
    }
}
