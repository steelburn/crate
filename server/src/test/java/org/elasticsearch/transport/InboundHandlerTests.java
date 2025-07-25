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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

public class InboundHandlerTests extends ESTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final Version version = Version.CURRENT;

    private Transport.ResponseHandlers responseHandlers;
    private Transport.RequestHandlers requestHandlers;
    private InboundHandler handler;
    private CloseableChannel channel;
    private EmbeddedChannel embeddedChannel;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        boolean isServer = randomBoolean();
        embeddedChannel = new EmbeddedChannel();
        InetSocketAddress localAddress = buildNewFakeTransportAddress().address();
        InetSocketAddress remoteAddress = buildNewFakeTransportAddress().address();
        channel = new CloseableChannel(embeddedChannel, isServer) {

            @Override
            public InetSocketAddress getRemoteAddress() {
                return remoteAddress;
            }

            @Override
            public InetSocketAddress getLocalAddress() {
                return localAddress;
            }
        };
        requestHandlers = new Transport.RequestHandlers();
        responseHandlers = new Transport.ResponseHandlers();
        handler = handlerForVersion(version);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    @Test
    public void testPing() throws Exception {
        final AtomicReference<TransportChannel> channelCaptor = new AtomicReference<>();
        RequestHandlerRegistry<TestRequest> registry = new RequestHandlerRegistry<>(
            "test-request",
            TestRequest::new,
            (_, channel) -> channelCaptor.set(channel),
            ThreadPool.Names.SAME,
            false,
            true
        );
        requestHandlers.registerHandler(registry);

        handler.inboundMessage(channel, new InboundMessage(null, true));
        if (channel.isServerChannel()) {
            ByteBuf ping = (ByteBuf) embeddedChannel.outboundMessages().poll();
            assertThat((char) ping.getByte(0)).isEqualTo('E');
            assertThat(ping.readableBytes()).isEqualTo(6);
        }
    }

    @Test
    public void testRequestAndResponse() throws Exception {
        String action = "test-request";
        int headerSize = TcpHeader.headerSize(version);
        boolean isError = randomBoolean();
        AtomicReference<TestRequest> requestCaptor = new AtomicReference<>();
        AtomicReference<TestResponse> responseCaptor = new AtomicReference<>();
        AtomicReference<Exception> exceptionCaptor = new AtomicReference<>();
        AtomicReference<TransportChannel> channelCaptor = new AtomicReference<>();

        long requestId = responseHandlers.newRequestId();
        responseHandlers.add(requestId, new Transport.ResponseContext<>(new TransportResponseHandler<TestResponse>() {
            @Override
            public void handleResponse(TestResponse response) {
                responseCaptor.set(response);
            }

            @Override
            public void handleException(TransportException exp) {
                exceptionCaptor.set(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        }, null, action));
        RequestHandlerRegistry<TestRequest> registry = new RequestHandlerRegistry<>(
            action,
            TestRequest::new,
            (request, channel) -> {
                channelCaptor.set(channel);
                requestCaptor.set(request);
            },
            ThreadPool.Names.SAME,
            false,
            true
        );
        requestHandlers.registerHandler(registry);
        String requestValue = randomAlphaOfLength(10);
        OutboundMessage.Request request = new OutboundMessage.Request(
            new TestRequest(requestValue), version, action, requestId, false, false);

        BytesReference fullRequestBytes = request.serialize(new BytesStreamOutput());
        BytesReference requestContent = fullRequestBytes.slice(headerSize, fullRequestBytes.length() - headerSize);
        Header requestHeader = new Header(fullRequestBytes.length() - 6, requestId, TransportStatus.setRequest((byte) 0), version);
        InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(requestContent), () -> {});
        requestHeader.finishParsingHeader(requestMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, requestMessage);

        TransportChannel transportChannel = channelCaptor.get();
        assertThat(transportChannel.getVersion()).isEqualTo(Version.CURRENT);
        assertThat(transportChannel.getChannelType()).isEqualTo("transport");
        assertThat(requestValue).isEqualTo(requestCaptor.get().value);

        String responseValue = randomAlphaOfLength(10);
        byte responseStatus = TransportStatus.setResponse((byte) 0);
        if (isError) {
            responseStatus = TransportStatus.setError(responseStatus);
            transportChannel.sendResponse(new ElasticsearchException("boom"));
        } else {
            transportChannel.sendResponse(new TestResponse(responseValue));
        }

        ByteBuf fullResponse = (ByteBuf) embeddedChannel.outboundMessages().poll();
        BytesReference fullResponseBytes = Netty4Utils.toBytesReference(fullResponse);
        BytesReference responseContent = fullResponseBytes.slice(headerSize, fullResponseBytes.length() - headerSize);
        Header responseHeader = new Header(fullRequestBytes.length() - 6, requestId, responseStatus, version);
        InboundMessage responseMessage = new InboundMessage(responseHeader, ReleasableBytesReference.wrap(responseContent), () -> {});
        responseHeader.finishParsingHeader(responseMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, responseMessage);

        if (isError) {
            assertThat(exceptionCaptor.get()).isInstanceOf(RemoteTransportException.class);
            assertThat(exceptionCaptor.get().getCause()).isInstanceOf(ElasticsearchException.class);
            assertThat(exceptionCaptor.get().getCause().getMessage()).isEqualTo("boom");
        } else {
            assertThat(responseValue).isEqualTo(responseCaptor.get().value);
        }
    }

    @Test
    public void testSendsErrorResponseToHandshakeFromCompatibleVersion() throws Exception {
        // Nodes use their minimum compatibility version for the TCP handshake, so a node from v(major-1).x will report its version as
        // v(major-2).last in the TCP handshake, with which we are not really compatible. We put extra effort into making sure that if
        // successful we can respond correctly in a format this old, but we do not guarantee that we can respond correctly with an error
        // response. However if the two nodes are from the same major version then we do guarantee compatibility of error responses.

        final Version remoteVersion = VersionUtils.randomCompatibleVersion(random(), version);
        final long requestId = randomNonNegativeLong();
        final Header requestHeader = new Header(between(0, 100), requestId,
            TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)), remoteVersion);
        final InboundMessage requestMessage = unreadableInboundHandshake(remoteVersion, requestHeader);
        requestHeader.actionName = TransportHandshaker.HANDSHAKE_ACTION_NAME;
        requestHeader.bwcNeedsToReadVariableHeader = false;
        handler.inboundMessage(channel, requestMessage);

        ByteBuf msg = (ByteBuf) embeddedChannel.outboundMessages().poll();
        final BytesReference responseBytesReference = Netty4Utils.toBytesReference(msg);
        final Header responseHeader = InboundDecoder.readHeader(responseBytesReference.length(), responseBytesReference);
        assertThat(responseHeader.isResponse()).isTrue();
        assertThat(responseHeader.isError()).isTrue();
    }


    @Test
    public void testClosesChannelOnErrorInHandshakeWithIncompatibleVersion() throws Exception {
        // Nodes use their minimum compatibility version for the TCP handshake, so a node from v(major-1).x will report its version as
        // v(major-2).last in the TCP handshake, with which we are not really compatible. We put extra effort into making sure that if
        // successful we can respond correctly in a format this old, but we do not guarantee that we can respond correctly with an error
        // response so we must just close the connection on an error. To avoid the failure disappearing into a black hole we at least log
        // it.

        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "expected message",
                InboundHandler.class.getCanonicalName(),
                Level.WARN,
                "could not send error response to handshake"));
        final Logger inboundHandlerLogger = LogManager.getLogger(InboundHandler.class);
        Loggers.addAppender(inboundHandlerLogger, mockAppender);

        try {
            final AtomicBoolean isClosed = new AtomicBoolean();
            channel.addCloseListener(ActionListener.wrap(() -> assertThat(isClosed.compareAndSet(false, true)).isTrue()));

            final Version remoteVersion = Version.fromId(randomIntBetween(5_00_00_00, version.minimumCompatibilityVersion().internalId - 1));
            final long requestId = randomNonNegativeLong();
            final Header requestHeader = new Header(between(0, 100), requestId,
                TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)), remoteVersion);
            requestHeader.bwcNeedsToReadVariableHeader = false;
            final InboundMessage requestMessage = unreadableInboundHandshake(remoteVersion, requestHeader);
            requestHeader.actionName = TransportHandshaker.HANDSHAKE_ACTION_NAME;
            handler.inboundMessage(channel, requestMessage);
            assertThat(isClosed.get()).isTrue();
            assertThat(embeddedChannel.outboundMessages().poll()).isNull();
            assertThat(embeddedChannel.inboundMessages().poll()).isNull();
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(inboundHandlerLogger, mockAppender);
            mockAppender.stop();
        }
    }

    @Test
    public void test_handshake_precedes_stream_version_for_compatibility() throws Exception {
        // This isn't compatible with the receivers version V_6_0_0, but the original version (V_5_10_1) precedes the
        // minimum compatible one.
        Version localVersion = Version.V_6_0_0;
        Version remoteVersion = Version.V_5_10_1;
        InboundHandler handler = handlerForVersion(localVersion);
        int headerSize = TcpHeader.headerSize(remoteVersion);

        final long requestId = randomNonNegativeLong();
        final Header requestHeader = new Header(between(0, 100), requestId,
            TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)), remoteVersion);
        OutboundMessage.Request request = new OutboundMessage.Request(
            new TransportHandshaker.HandshakeRequest(remoteVersion),
            remoteVersion,
            TransportHandshaker.HANDSHAKE_ACTION_NAME,
            requestId,
            true,
            false
        );

        BytesReference fullRequestBytes = request.serialize(new BytesStreamOutput());
        BytesReference requestContent = fullRequestBytes.slice(headerSize, fullRequestBytes.length() - headerSize);
        InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(requestContent), () -> {});
        requestHeader.finishParsingHeader(requestMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, requestMessage);

        int responseHeaderSize = TcpHeader.headerSize(localVersion.minimumCompatibilityVersion());
        ByteBuf fullResponse = (ByteBuf) embeddedChannel.outboundMessages().poll();
        BytesReference fullResponseBytes = Netty4Utils.toBytesReference(fullResponse);
        BytesReference responseContent = fullResponseBytes.slice(responseHeaderSize + 2, fullResponseBytes.length() - responseHeaderSize - 2);
        Header responseHeader = InboundDecoder.readHeader(fullResponseBytes.length(), fullResponseBytes);
        InboundMessage responseMessage = new InboundMessage(responseHeader, ReleasableBytesReference.wrap(responseContent), () -> {});

        TransportHandshaker.HandshakeResponse response = new TransportHandshaker.HandshakeResponse(responseMessage.openOrGetStreamInput());
        assertThat(response.getResponseVersion()).isEqualTo(localVersion);
    }

    /**
     * Tests a handshake initiated by a newer node to an older node while the newer nodes minimum compatible version matches.
     * <p/>
     *  6.0.0 -> send handshake request -> 5.10.1       (5.10.1 accepts this as the minimum compatible version sent is 5.10.1)
     *  5.10.1 -> send handshake response -> 6.0.0      (6.0.0 accepts this as the minimum compatible version defined is 5.10.1)
     *
     */
    @Test
    public void test_handshake_checks_minimum_compatible_version_if_normal_version_is_not_compatible() throws Exception {
        Version localVersion = Version.V_6_0_0;
        Version remoteVersion = Version.V_5_10_1;
        InboundHandler handler = handlerForVersion(remoteVersion);

        int headerSize = TcpHeader.headerSize(localVersion.minimumCompatibilityVersion());
        final long requestId = randomNonNegativeLong();
        final Header requestHeader = new Header(between(0, 100), requestId,
            TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)), localVersion.minimumCompatibilityVersion());
        OutboundMessage.Request request = new OutboundMessage.Request(
            new TransportHandshaker.HandshakeRequest(localVersion),
            localVersion,
            TransportHandshaker.HANDSHAKE_ACTION_NAME,
            requestId,
            true,
            false
        );

        BytesReference fullRequestBytes = request.serialize(new BytesStreamOutput());
        BytesReference requestContent = fullRequestBytes.slice(headerSize, fullRequestBytes.length() - headerSize);
        InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(requestContent), () -> {});
        requestHeader.finishParsingHeader(requestMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, requestMessage);

        int responseHeaderSize = TcpHeader.headerSize(remoteVersion);
        ByteBuf fullResponse = (ByteBuf) embeddedChannel.outboundMessages().poll();
        BytesReference fullResponseBytes = Netty4Utils.toBytesReference(fullResponse);
        BytesReference responseContent = fullResponseBytes.slice(responseHeaderSize + 2, fullResponseBytes.length() - responseHeaderSize - 2);
        Header responseHeader = InboundDecoder.readHeader(fullResponseBytes.length(), fullResponseBytes);
        InboundMessage responseMessage = new InboundMessage(responseHeader, ReleasableBytesReference.wrap(responseContent), () -> {});

        TransportHandshaker.HandshakeResponse response = new TransportHandshaker.HandshakeResponse(responseMessage.openOrGetStreamInput());

        assertThat(response.getResponseVersion()).isEqualTo(remoteVersion);
    }

    @Test
    public void test_handshake_error_if_version_and_minimum_compatible_version_is_not_compatible() throws Exception {
        Version localVersion = Version.V_6_0_0;
        Version remoteVersion = Version.V_4_8_4;
        InboundHandler handler = handlerForVersion(localVersion);
        int headerSize = TcpHeader.headerSize(remoteVersion);

        final long requestId = randomNonNegativeLong();
        final Header requestHeader = new Header(between(0, 100), requestId,
            TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)), remoteVersion);
        OutboundMessage.Request request = new OutboundMessage.Request(
            new TransportHandshaker.HandshakeRequest(remoteVersion),
            remoteVersion,
            TransportHandshaker.HANDSHAKE_ACTION_NAME,
            requestId,
            true,
            false
        );

        BytesReference fullRequestBytes = request.serialize(new BytesStreamOutput());
        BytesReference requestContent = fullRequestBytes.slice(headerSize, fullRequestBytes.length() - headerSize);
        InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(requestContent), () -> {});
        requestHeader.finishParsingHeader(requestMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, requestMessage);

        int responseHeaderSize = TcpHeader.headerSize(localVersion.minimumCompatibilityVersion());
        ByteBuf fullResponse = (ByteBuf) embeddedChannel.outboundMessages().poll();
        BytesReference fullResponseBytes = Netty4Utils.toBytesReference(fullResponse);
        BytesReference responseContent = fullResponseBytes.slice(responseHeaderSize + 2, fullResponseBytes.length() - responseHeaderSize - 2);
        Header responseHeader = InboundDecoder.readHeader(fullResponseBytes.length(), fullResponseBytes);
        assertThat(responseHeader.isError()).isTrue();

        InboundMessage responseMessage = new InboundMessage(responseHeader, ReleasableBytesReference.wrap(responseContent), () -> {});
        Exception exception = responseMessage.openOrGetStreamInput().readException();
        assertThat(exception).isInstanceOf(RemoteTransportException.class);
        assertThat(exception.getCause()).isInstanceOf(IllegalStateException.class);
        assertThat(exception.getCause().getMessage()).isEqualTo(
            "Received handshake message from unsupported version: [4.8.4] minimal compatible version is: [5.10.1]");
    }

    @Test
    public void testLogsSlowInboundProcessing() throws Exception {
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "expected message",
                InboundHandler.class.getCanonicalName(),
                Level.WARN,
                "handling inbound transport message "));
        final Logger inboundHandlerLogger = LogManager.getLogger(InboundHandler.class);
        Loggers.addAppender(inboundHandlerLogger, mockAppender);

        handler.setSlowLogThreshold(TimeValue.timeValueMillis(5L));
        try {
            final Version remoteVersion = Version.CURRENT;
            final long requestId = randomNonNegativeLong();
            final Header requestHeader = new Header(between(0, 100), requestId,
                TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)), remoteVersion);
            final InboundMessage requestMessage =
                new InboundMessage(requestHeader, ReleasableBytesReference.wrap(BytesArray.EMPTY), () -> {
                    try {
                        TimeUnit.SECONDS.sleep(1L);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                });
            requestHeader.actionName = TransportHandshaker.HANDSHAKE_ACTION_NAME;
            // Imitate that header have been read in order to pass assertion
            // in handler.inboundMessage -> handler.messageReceived() calls below
            requestHeader.bwcNeedsToReadVariableHeader = false;
            handler.inboundMessage(channel, requestMessage);

            Object msg = embeddedChannel.outboundMessages().poll();
            assertThat(msg).isNotNull();
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(inboundHandlerLogger, mockAppender);
            mockAppender.stop();
        }
    }

    private static InboundMessage unreadableInboundHandshake(Version remoteVersion, Header requestHeader) {
        return new InboundMessage(requestHeader, ReleasableBytesReference.wrap(BytesArray.EMPTY), () -> { }) {
            @Override
            public StreamInput openOrGetStreamInput() {
                final StreamInput streamInput = new InputStreamStreamInput(new InputStream() {
                    @Override
                    public int read() {
                        throw new ElasticsearchException("unreadable handshake");
                    }
                });
                streamInput.setVersion(remoteVersion);
                return streamInput;
            }
        };
    }

    private InboundHandler handlerForVersion(Version localVersion) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        TransportHandshaker handshaker = new TransportHandshaker(localVersion, threadPool, (_, _, _, _) -> {});
        TransportKeepAlive keepAlive = new TransportKeepAlive(threadPool, (_, b) -> channel.writeAndFlush(Unpooled.wrappedBuffer(b)));
        OutboundHandler outboundHandler = new OutboundHandler(
            "node",
            localVersion,
            new StatsTracker(),
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE
        );
        return new InboundHandler(
            threadPool,
            outboundHandler,
            namedWriteableRegistry,
            handshaker,
            keepAlive,
            requestHandlers,
            responseHandlers
        );
    }
}
