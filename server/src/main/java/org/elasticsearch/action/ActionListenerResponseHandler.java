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

package org.elasticsearch.action;

import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

/**
 * A simple base class for action response listeners, defaulting to using the SAME executor (as its
 * very common on response handlers).
 */
public class ActionListenerResponseHandler<Response extends TransportResponse> implements TransportResponseHandler<Response> {

    private final String action;
    private final ActionListener<? super Response> listener;
    private final Writeable.Reader<Response> reader;
    private final String executor;

    public ActionListenerResponseHandler(String action,
                                         ActionListener<? super Response> listener,
                                         Writeable.Reader<Response> reader,
                                         String executor) {
        this.action = action;
        this.listener = Objects.requireNonNull(listener);
        this.reader = Objects.requireNonNull(reader);
        this.executor = Objects.requireNonNull(executor);
    }

    public ActionListenerResponseHandler(String action,
                                         ActionListener<? super Response> listener,
                                         Writeable.Reader<Response> reader) {
        this(action, listener, reader, ThreadPool.Names.SAME);
    }

    @Override
    public void handleResponse(Response response) {
        listener.onResponse(response);
    }

    @Override
    public void handleException(TransportException e) {
        listener.onFailure(e);
    }

    @Override
    public Response read(StreamInput in) throws IOException {
        return reader.read(in);
    }

    @Override
    public String executor() {
        return executor;
    }

    @Override
    public String toString() {
        return "ActionResponseHandler{" + action + ", executor=" + executor + "}";
    }
}
