/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.contrib.http;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.contrib.http.impl.HttpListenerSinkContext;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.util.Objects;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.impl.pipeline.SinkImpl.Type.TOTAL_PARALLELISM_ONE;

/**
 * See {@link HttpListenerSinks#builder()}.
 *
 * @param <T> the type of the pipeline item.
 */
public class HttpListenerSinkBuilder<T> {

    /**
     * Default port for HTTP Listener sink
     */
    public static final int DEFAULT_PORT = 8081;

    /**
     * Default path for HTTP Listener sink
     */
    public static final String DEFAULT_PATH = "/";

    private static final int PORT_MAX = 0xFFFF;

    private int port = DEFAULT_PORT;
    private String path = DEFAULT_PATH;
    private boolean accumulateItems;
    private boolean mutualAuthentication;
    private SupplierEx<SSLContext> sslContextFn;
    private FunctionEx<T, String> toStringFn = Object::toString;

    HttpListenerSinkBuilder() {
    }

    /**
     * Set the port which the sink binds and listens.
     * <p>
     * For example to bind to port `5902`:
     * <pre>{@code
     * builder.port(5902)
     * }</pre>
     * <p>
     * Default value is {@link #DEFAULT_PORT} {@code 8081}.
     *
     * @param port the port which the source binds and listens.
     */
    @Nonnull
    public HttpListenerSinkBuilder<T> port(int port) {
        if (port < 0 || port > PORT_MAX) {
            throw new IllegalArgumentException("Port out of range: " + port + ". Allowed range [0,65535]");
        }
        this.port = port;
        return this;
    }

    /**
     * Set the SSL Context function which will be used to initialize underlying
     * HTTPs listener for secure connections.
     * <p>
     * For example:
     * <pre>{@code
     * builder.sslContextFn(() -> {
     *     SSLContext context = SSLContext.getInstance("TLS");
     *     KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
     *     KeyStore ks = KeyStore.getInstance("PKCS12");
     *     char[] password = "123456".toCharArray();
     *     ks.load(new FileInputStream("the.keystore"), password);
     *     kmf.init(ks, password);
     *     TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
     *     KeyStore ts = KeyStore.getInstance("PKCS12");
     *     ts.load(new FileInputStream("the.truststore"), password);
     *     tmf.init(ts);
     *     context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
     *     return context;
     * })
     * }</pre>
     * <p>
     * Default value is {@code null}.
     *
     * @param sslContextFn the function to create {@link SSLContext} which used
     *                     to initialize underlying HTTPs listener for secure
     *                     connections.
     */
    @Nonnull
    public HttpListenerSinkBuilder<T> sslContextFn(@Nonnull SupplierEx<SSLContext> sslContextFn) {
        this.sslContextFn = Objects.requireNonNull(sslContextFn);
        return this;
    }

    /**
     * Set that sink should authenticate the connected clients. This
     * parameter is ignored if {@link #sslContextFn(SupplierEx)} is not set.
     * <p>
     * Default values is {@code false}, sink does not authenticate connected
     * clients.
     */
    @Nonnull
    public HttpListenerSinkBuilder<T> enableMutualAuthentication() {
        this.mutualAuthentication = true;
        return this;
    }

    /**
     * Set the path which server accepts connections.
     * <p>
     * For example:
     * <pre>{@code
     * builder.path("/user")
     * }</pre>
     * <p>
     * Default value is {@code /}.
     *
     * @param path the path which server accepts connections
     */
    @Nonnull
    public HttpListenerSinkBuilder<T> path(@Nonnull String path) {
        this.path = Objects.requireNonNull(path);
        return this;
    }

    /**
     * Set that sink should accumulate items if there is no connected
     * client and send them when a client connects. The items will be
     * accumulated in an unbounded fashion thus may create memory
     * issues.
     * <p>
     * Default value is {@code false}, sink drops the items if no there
     * is no connected client.
     */
    @Nonnull
    public HttpListenerSinkBuilder<T> accumulateItems() {
        this.accumulateItems = true;
        return this;
    }

    /**
     * Set the function which converts each item to a string.
     * <p>
     * By default source converts each item to string using
     * {@link Object#toString()}.
     *
     * @param toStringFn the function which converts each item to a string.
     */
    @Nonnull
    public HttpListenerSinkBuilder<T> toStringFn(@Nonnull FunctionEx<T, String> toStringFn) {
        this.toStringFn = Objects.requireNonNull(toStringFn);
        return this;
    }

    /**
     * Build a Websocket {@link Sink} with supplied parameters.
     */
    @Nonnull
    public Sink<T> buildWebsocket() {
        return build(path, port, accumulateItems, mutualAuthentication, true, sslContextFn, toStringFn);
    }

    /**
     * Build a Server Sent Events {@link Sink} with supplied parameters.
     */
    @Nonnull
    public Sink<T> buildServerSent() {
        return build(path, port, accumulateItems, mutualAuthentication, false, sslContextFn, toStringFn);
    }

    private Sink<T> build(
            @Nonnull String path,
            int port,
            boolean accumulateItems,
            boolean mutualAuthentication,
            boolean websocket,
            @Nullable SupplierEx<SSLContext> sslContextFn,
            @Nonnull FunctionEx<T, String> toStringFn
    ) {
        SupplierEx<Processor> supplier =
                SinkProcessors.writeBufferedP(ctx -> new HttpListenerSinkContext<>(ctx, path, port,
                                accumulateItems, mutualAuthentication, websocket, sslContextFn, toStringFn),
                        HttpListenerSinkContext::receive,
                        HttpListenerSinkContext::flush,
                        HttpListenerSinkContext::close
                );
        return new SinkImpl<>(websocket ? websocketName() : serverSentName(),
                forceTotalParallelismOne(ProcessorSupplier.of(supplier), String.valueOf(port)), TOTAL_PARALLELISM_ONE);
    }

    private String websocketName() {
        return "websocket@" + port;
    }

    private String serverSentName() {
        return "serverSent@" + port;
    }

}