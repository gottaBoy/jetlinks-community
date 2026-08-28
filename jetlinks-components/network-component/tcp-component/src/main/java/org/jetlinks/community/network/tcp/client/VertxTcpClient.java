/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.community.network.tcp.client;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.jetlinks.community.network.tcp.TcpMessage;
import org.jetlinks.community.network.tcp.parser.PayloadParser;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.community.network.DefaultNetworkType;
import org.jetlinks.community.network.NetworkType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class VertxTcpClient implements TcpClient {

    private static final long WRITE_SLOW_THRESHOLD_MS = Math.max(
        0,
        Long.getLong("gateway.tcp.network.write-slow-ms", 300L)
    );

    public volatile NetClient client;

    public volatile NetSocket socket;

    volatile PayloadParser payloadParser;

    @Getter
    private final String id;

    private volatile String deviceId = "unknown";

    private volatile long socketConnectedAtNanos;

    private volatile long socketGeneration;

    private final AtomicLong writeSequence = new AtomicLong();

    @Setter
    private long keepAliveTimeoutMs = Duration.ofMinutes(10).toMillis();

    private volatile long lastKeepAliveTime = System.currentTimeMillis();

    private final List<Runnable> disconnectListener = new CopyOnWriteArrayList<>();

    private final Sinks.Many<TcpMessage> sink = Reactors.createMany(Integer.MAX_VALUE, false);

    private final boolean serverClient;

    @Override
    public void keepAlive() {
        lastKeepAliveTime = System.currentTimeMillis();
    }

    @Override
    public void setKeepAliveTimeout(Duration timeout) {
        keepAliveTimeoutMs = timeout.toMillis();
    }

    @Override
    public void reset() {
        if (null != payloadParser) {
            payloadParser.reset();
        }
    }

    @Override
    public InetSocketAddress address() {
        return getRemoteAddress();
    }

    @Override
    public Mono<Void> sendMessage(EncodedMessage message) {
        return Mono
            .<Void>create((sink) -> {
                NetSocket currentSocket = socket;
                if (currentSocket == null) {
                    ReferenceCountUtil.safeRelease(message.getPayload());
                    sink.error(new SocketException("socket closed"));
                    return;
                }
                ByteBuf buf = message.getPayload();
                Buffer buffer = Buffer.buffer(buf);
                int len = buffer.length();
                long writeStartNanos = System.nanoTime();
                long writeConnectionStartNanos = socketConnectedAtNanos;
                long writeSocketGeneration = socketGeneration;
                long writeId = writeSequence.incrementAndGet();
                boolean queueFull = currentSocket.writeQueueFull();
                sink.onCancel(() -> log.debug(
                    "[tcp-write] write_cancel writeId={} clientId={} deviceId={} remoteAddress={} "
                        + "connectionGeneration={} connectionAgeMs={} payloadBytes={} "
                        + "writeQueueFullAtStart={}",
                    writeId, id, deviceId, remoteAddress(currentSocket), writeSocketGeneration,
                    connectionAgeMs(writeConnectionStartNanos), len, queueFull
                ));
                log.debug("[tcp-write] write_start writeId={} clientId={} deviceId={} remoteAddress={} "
                        + "connectionGeneration={} connectionAgeMs={} payloadBytes={} "
                        + "writeQueueFull={}",
                    writeId, id, deviceId, remoteAddress(currentSocket),
                    writeSocketGeneration, connectionAgeMs(writeConnectionStartNanos), len, queueFull);
                if (queueFull) {
                    log.warn("[tcp-write] write_queue_full writeId={} clientId={} deviceId={} remoteAddress={} "
                            + "connectionGeneration={} connectionAgeMs={} payloadBytes={}",
                        writeId, id, deviceId, remoteAddress(currentSocket), writeSocketGeneration,
                        connectionAgeMs(writeConnectionStartNanos), len);
                }
                AtomicBoolean released = new AtomicBoolean();
                try {
                    currentSocket.write(buffer, r -> {
                        if (released.compareAndSet(false, true)) {
                            ReferenceCountUtil.safeRelease(buf);
                        }
                        long durationMs = Duration.ofNanos(
                            Math.max(0, System.nanoTime() - writeStartNanos)
                        ).toMillis();
                        if (r.succeeded()) {
                            keepAlive();
                            log.debug("[tcp-write] write_complete writeId={} clientId={} deviceId={} "
                                    + "remoteAddress={} connectionGeneration={} connectionAgeMs={} payloadBytes={} "
                                    + "writeDurationMs={} writeQueueFullAtStart={}",
                                writeId, id, deviceId, remoteAddress(currentSocket), writeSocketGeneration,
                                connectionAgeMs(writeConnectionStartNanos), len, durationMs, queueFull);
                            if (durationMs >= WRITE_SLOW_THRESHOLD_MS && WRITE_SLOW_THRESHOLD_MS > 0) {
                                log.warn("[tcp-write] write_complete_slow writeId={} clientId={} deviceId={} "
                                        + "remoteAddress={} connectionGeneration={} connectionAgeMs={} payloadBytes={} "
                                        + "writeDurationMs={} writeQueueFullAtStart={}",
                                    writeId, id, deviceId, remoteAddress(currentSocket), writeSocketGeneration,
                                    connectionAgeMs(writeConnectionStartNanos), len, durationMs, queueFull);
                            }
                            sink.success();
                        } else {
                            log.warn("[tcp-write] write_error writeId={} clientId={} deviceId={} "
                                    + "remoteAddress={} connectionGeneration={} connectionAgeMs={} payloadBytes={} "
                                    + "writeDurationMs={} writeQueueFullAtStart={} cause={}",
                                writeId, id, deviceId, remoteAddress(currentSocket), writeSocketGeneration,
                                connectionAgeMs(writeConnectionStartNanos), len, durationMs, queueFull, r.cause());
                            sink.error(r.cause());
                        }
                    });
                } catch (Throwable error) {
                    if (released.compareAndSet(false, true)) {
                        ReferenceCountUtil.safeRelease(buf);
                    }
                    log.warn("[tcp-write] write_throwable writeId={} clientId={} deviceId={} "
                            + "remoteAddress={} connectionGeneration={} connectionAgeMs={} payloadBytes={} "
                            + "writeDurationMs={} writeQueueFullAtStart={}",
                        writeId, id, deviceId, remoteAddress(currentSocket), writeSocketGeneration,
                        connectionAgeMs(writeConnectionStartNanos), len, Duration.ofNanos(
                            Math.max(0, System.nanoTime() - writeStartNanos)
                        ).toMillis(), queueFull, error);
                    sink.error(error);
                }
            });
    }

    @Override
    public Flux<EncodedMessage> receiveMessage() {
        return this
            .subscribe()
            .cast(EncodedMessage.class);
    }

    @Override
    public void disconnect() {
        shutdown();
    }

    @Override
    public boolean isAlive() {
        return socket != null && (keepAliveTimeoutMs < 0 || System.currentTimeMillis() - lastKeepAliveTime < keepAliveTimeoutMs);
    }

    @Override
    public boolean isAutoReload() {
        return true;
    }

    public VertxTcpClient(String id) {
        this.id = id;
        this.serverClient = true;
    }

    protected void received(TcpMessage message) {
        sink.emitNext(message,Reactors.RETRY_NON_SERIALIZED);
    }

    @Override
    public Flux<TcpMessage> subscribe() {
        return sink.asFlux();
    }

    private void execute(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            log.warn("close tcp client error", e);
        }
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        if (null == socket) {
            return null;
        }
        SocketAddress socketAddress = socket.remoteAddress();
        return InetSocketAddress.createUnresolved(socketAddress.host(), socketAddress.port());
    }

    @Override
    public NetworkType getType() {
        return DefaultNetworkType.TCP_CLIENT;
    }

    @Override
    public void shutdown() {
        NetSocket currentSocket;
        long currentGeneration;
        synchronized (this) {
            currentSocket = socket;
            currentGeneration = socketGeneration;
        }
        if (currentSocket != null) {
            shutdownIfCurrent(currentSocket, currentGeneration);
        }
    }

    private void shutdownIfCurrent(NetSocket expectedSocket, long expectedGeneration) {
        NetClient clientToClose;
        NetSocket socketToClose;
        PayloadParser parserToClose;
        synchronized (this) {
            if (socket != expectedSocket || socketGeneration != expectedGeneration) {
                log.debug("[tcp-connection] stale_close_ignored clientId={} deviceId={} "
                        + "expectedGeneration={} currentGeneration={}",
                    id, deviceId, expectedGeneration, socketGeneration);
                return;
            }
            clientToClose = client;
            socketToClose = socket;
            parserToClose = payloadParser;
            client = null;
            socket = null;
            payloadParser = null;
        }

        log.info("[tcp-connection] close clientId={} deviceId={} remoteAddress={} "
                + "connectionGeneration={} connectionAgeMs={}",
            id, deviceId, remoteAddress(expectedSocket), expectedGeneration,
            connectionAgeMs(socketConnectedAtNanos));
        if (clientToClose != null) {
            execute(clientToClose::close);
        }
        if (socketToClose != null) {
            execute(socketToClose::close);
        }
        if (parserToClose != null) {
            execute(parserToClose::close);
        }
        for (Runnable runnable : disconnectListener) {
            execute(runnable);
        }
        disconnectListener.clear();
        if (serverClient) {
            sink.tryEmitComplete();
        }
    }

    public void setClient(NetClient client) {
        if (this.client != null && this.client != client) {
            this.client.close();
        }
        keepAlive();
        this.client = client;
    }

    public void setRecordParser(PayloadParser payloadParser) {
        synchronized (this) {
            if (null != this.payloadParser && this.payloadParser != payloadParser) {
                this.payloadParser.close();
            }
            this.payloadParser = payloadParser;
            this.payloadParser
                .handlePayload()
                .subscribe(buffer -> received(new TcpMessage(buffer.getByteBuf())));
        }
    }

    public void setSocket(NetSocket socket) {
        NetSocket oldSocket;
        long connectionGeneration;
        long connectionStartNanos;
        synchronized (this) {
            Objects.requireNonNull(payloadParser);
            oldSocket = this.socket != socket ? this.socket : null;
            connectionGeneration = ++socketGeneration;
            connectionStartNanos = System.nanoTime();
            socketConnectedAtNanos = connectionStartNanos;
            this.socket = socket;
            if (oldSocket != null) {
                log.warn("[tcp-connection] replace_socket clientId={} deviceId={} "
                        + "oldRemoteAddress={} newRemoteAddress={}",
                    id, deviceId, remoteAddress(oldSocket), remoteAddress(socket));
            }
            socket
                .exceptionHandler(error -> log.error(
                    "[tcp-connection] exception clientId={} deviceId={} remoteAddress={} "
                        + "connectionGeneration={} connectionAgeMs={}",
                    id, deviceId, remoteAddress(socket), connectionGeneration,
                    connectionAgeMs(connectionStartNanos), error))
                .closeHandler(v -> {
                    log.info("[tcp-connection] closed clientId={} deviceId={} remoteAddress={} "
                            + "connectionGeneration={} connectionAgeMs={}",
                        id, deviceId, remoteAddress(socket), connectionGeneration,
                        connectionAgeMs(connectionStartNanos));
                    shutdownIfCurrent(socket, connectionGeneration);
                })
                .handler(buffer -> {
                    PayloadParser currentParser;
                    synchronized (this) {
                        if (this.socket != socket || socketGeneration != connectionGeneration) {
                            log.debug("[tcp-connection] stale_data_ignored clientId={} deviceId={} "
                                    + "connectionGeneration={} currentGeneration={} payloadBytes={}",
                                id, deviceId, connectionGeneration, socketGeneration, buffer.length());
                            execute(socket::close);
                            return;
                        }
                        currentParser = payloadParser;
                        if (currentParser == null) {
                            return;
                        }
                        keepAlive();
                        // Keep the parser alive while it consumes the buffer. This is a
                        // short critical section and prevents setRecordParser/shutdown
                        // from closing it concurrently.
                        currentParser.handle(buffer);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("handle tcp client[{}] payload:[{}]",
                                  socket.remoteAddress(),
                                  Hex.encodeHexString(buffer.getBytes()));
                    }
                });
            log.info("[tcp-connection] connected clientId={} deviceId={} remoteAddress={} "
                        + "connectionGeneration={}",
                id, deviceId, remoteAddress(socket), connectionGeneration);
        }
        if (oldSocket != null) {
            execute(oldSocket::close);
        }
    }

    @Override
    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId == null || deviceId.isEmpty() ? "unknown" : deviceId;
        log.info("[tcp-connection] device_bound clientId={} deviceId={} remoteAddress={} "
                + "connectionAgeMs={}",
            id, this.deviceId, remoteAddress(socket), connectionAgeMs());
    }

    private String remoteAddress(NetSocket currentSocket) {
        if (currentSocket == null || currentSocket.remoteAddress() == null) {
            return "unknown";
        }
        return String.valueOf(currentSocket.remoteAddress());
    }

    private long connectionAgeMs() {
        return connectionAgeMs(socketConnectedAtNanos);
    }

    private long connectionAgeMs(long connectedAtNanos) {
        if (connectedAtNanos <= 0) {
            return 0;
        }
        return Duration.ofNanos(Math.max(0, System.nanoTime() - connectedAtNanos)).toMillis();
    }

    @Override
    public Mono<Boolean> send(TcpMessage message) {
        return sendMessage(message)
            .thenReturn(true);
    }

    @Override
    public void onDisconnect(Runnable disconnected) {
        disconnectListener.add(disconnected);
    }
}
