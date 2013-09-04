/*
 * Copyright 2013 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.connection2;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.tomp2p.futures.FutureDone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the channels. This class is created by {@link ConnectionReservation} and should never be called directly.
 * With this class one can create TCP or UDP channels up to a certain extend. Thus it must be know beforehand how much
 * connection will be created.
 * 
 * @author Thomas Bocek
 */
public class ChannelCreator {
    private static final Logger LOG = LoggerFactory.getLogger(ChannelCreator.class);
    // statistics
    private static final AtomicInteger CREATED_TCP_CONNECTIONS = new AtomicInteger(0);
    private static final AtomicInteger CREATED_UDP_CONNECTIONS = new AtomicInteger(0);

    private final EventLoopGroup workerGroup;
    private final ChannelGroup recipients = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private final int maxPermitsUDP;
    private final int maxPermitsTCP;

    private final Semaphore semaphoreUPD;
    private final Semaphore semaphoreTCP;

    // we should be fair, otherwise we see connection timeouts due to unfairness if busy
    private final ReadWriteLock readWriteLockUDP = new ReentrantReadWriteLock(true);
    private final Lock readUDP = readWriteLockUDP.readLock();
    private final Lock writeUDP = readWriteLockUDP.writeLock();

    private final ReadWriteLock readWriteLockTCP = new ReentrantReadWriteLock(true);
    private final Lock readTCP = readWriteLockTCP.readLock();
    private final Lock writeTCP = readWriteLockTCP.writeLock();

    private boolean shutdownUDP = false;
    private boolean shutdownTCP = false;

    private final FutureDone<Void> futureChannelCreationDone;

    private final ChannelClientConfiguration channelClientConfiguration;

    /**
     * Package private constructor, since this is created by {@link ConnectionReservation} and should never be called
     * directly.
     * 
     * @param workerGroup
     *            The worker group for netty that is shared between TCP and UDP. This workergroup is not shutdown if
     *            this class is shutdown
     * @param futureChannelCreationDone
     *            We need to set this from the outside as we want to attach listeners to it
     * @param maxPermitsUDP
     *            The number of max. parallel UDP connections.
     * @param maxPermitsTCP
     *            The number of max. parallel TCP connections.
     * @param channelClientConfiguration
     *            The configuration that contains the pipeline filter
     */
    ChannelCreator(final EventLoopGroup workerGroup, final FutureDone<Void> futureChannelCreationDone,
            final int maxPermitsUDP, final int maxPermitsTCP,
            final ChannelClientConfiguration channelClientConfiguration) {
        this.workerGroup = workerGroup;
        this.futureChannelCreationDone = futureChannelCreationDone;
        this.maxPermitsUDP = maxPermitsUDP;
        this.maxPermitsTCP = maxPermitsTCP;
        this.semaphoreUPD = new Semaphore(maxPermitsUDP);
        this.semaphoreTCP = new Semaphore(maxPermitsTCP);
        this.channelClientConfiguration = channelClientConfiguration;
    }

    /**
     * Creates a "channel" to the given address. This won't send any message unlike TCP.
     * 
     * @param recipient
     *            The recipient of the a message
     * 
     * @param broadcast
     *            Sets this channel to be able to broadcast
     * @param channelHandlers
     *            The handlers to set
     * @return The channel future object or null if we are shut down
     */
    public ChannelFuture createUDP(final SocketAddress recipient, final boolean broadcast,
            final Map<String, ChannelHandler> channelHandlers) {
        readUDP.lock();
        try {
            if (shutdownUDP) {
                return null;
            }
            if (!semaphoreUPD.tryAcquire()) {
                LOG.error("Tried to acquire more resources (UDP) than announced!");
                throw new RuntimeException("Tried to acquire more resources (UDP) than announced!");
            }
            final Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioDatagramChannel.class);
            b.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(ConnectionBean.UDP_LIMIT));
            if (broadcast) {
                b.option(ChannelOption.SO_BROADCAST, true);
            }
            channelClientConfiguration.pipelineFilter().filter(channelHandlers, false, true);
            addHandlers(b, channelHandlers);
            // Here we need to bind, as opposed to the TCP, were we connect if we do a connect, we cannot receive
            // broadcast messages
            final ChannelFuture channelFuture;
            if (broadcast) {
                channelFuture = b.bind(new InetSocketAddress(0));
            } else {
                channelFuture = b.connect(recipient);
            }

            setupCloseListener(channelFuture, semaphoreUPD);
            CREATED_UDP_CONNECTIONS.incrementAndGet();
            return channelFuture;
        } finally {
            readUDP.unlock();
        }
    }

    /**
     * Creates a channel to the given address. This will setup the TCP connection
     * 
     * @param socketAddress
     *            The address to send future messages
     * @param connectionTimeoutMillis
     *            The timeout for establishing a TCP connection
     * @param channelHandlers
     *            The handlers to set
     * @return The channel future object or null if we are shut down.
     */
    public ChannelFuture createTCP(final SocketAddress socketAddress, final int connectionTimeoutMillis,
            final Map<String, ChannelHandler> channelHandlers) {
        readTCP.lock();
        try {
            if (shutdownTCP) {
                return null;
            }
            if (!semaphoreTCP.tryAcquire()) {
                LOG.error("Tried to acquire more resources (TCP) than announced!");
                throw new RuntimeException("Tried to acquire more resources (TCP) than announced!");
            }
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeoutMillis);
            b.option(ChannelOption.TCP_NODELAY, true);
            b.option(ChannelOption.SO_LINGER, 0);
            b.option(ChannelOption.SO_REUSEADDR, true);
            // b.option(ChannelOption.SO_KEEPALIVE, Boolean.TRUE);
            channelClientConfiguration.pipelineFilter().filter(channelHandlers, true, true);
            addHandlers(b, channelHandlers);

            ChannelFuture channelFuture = b.connect(socketAddress);

            setupCloseListener(channelFuture, semaphoreTCP);
            CREATED_TCP_CONNECTIONS.incrementAndGet();
            return channelFuture;
        } finally {
            readTCP.unlock();
        }
    }

    /**
     * Since we want to add multiple handlers, we need to do this with the pipeline.
     * 
     * @param b
     *            The boostrap
     * @param channelHandlers
     *            The handlers to be added.
     */
    private void addHandlers(final Bootstrap b, final Map<String, ChannelHandler> channelHandlers) {
        b.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(final Channel ch) throws Exception {
                for (Map.Entry<String, ChannelHandler> entry : channelHandlers.entrySet()) {
                    ch.pipeline().addLast(entry.getKey(), entry.getValue());
                }
            }
        });
    }

    /**
     * When a channel is closed, the semaphore is released an other channel can be created.
     * 
     * @param channelFuture
     *            The channel future
     * @param semaphore
     *            The semaphore to decrease
     * @return The same future that was passed as an argument
     */
    private ChannelFuture setupCloseListener(final ChannelFuture channelFuture, final Semaphore semaphore) {
        channelFuture.channel().closeFuture().addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                semaphore.release();
            }
        });
        recipients.add(channelFuture.channel());
        return channelFuture;
    }

    /**
     * Shutdown this channel creator. This means that no TCP or UDP connection can be established.
     * 
     * @return The shutdown future.
     */
    public FutureDone<Void> shutdown() {
        // set shutdown flag for UDP and TCP, if we acquire a write lock, all read locks are blocked as well
        writeUDP.lock();
        writeTCP.lock();
        try {
            if (shutdownTCP || shutdownUDP) {
                shutdownFuture().setFailed("already shutting down");
                return shutdownFuture();
            }
            shutdownUDP = true;
            shutdownTCP = true;
        } finally {
            writeTCP.unlock();
            writeUDP.unlock();
        }

        recipients.close().addListener(new GenericFutureListener<ChannelGroupFuture>() {
            @Override
            public void operationComplete(final ChannelGroupFuture future) throws Exception {
                if (!semaphoreUPD.tryAcquire(maxPermitsUDP)) {
                    LOG.error("Cannot shutdown, as connections (UDP) are still alive");
                    shutdownFuture().setFailed("Cannot shutdown, as connections (UDP) are still alive");
                    throw new RuntimeException("Cannot shutdown, as connections (UDP) are still alive");
                }
                if (!semaphoreTCP.tryAcquire(maxPermitsTCP)) {
                    LOG.error("Cannot shutdown, as connections (TCP) are still alive");
                    shutdownFuture().setFailed("Cannot shutdown, as connections (TCP) are still alive");
                    throw new RuntimeException("Cannot shutdown, as connections (TCP) are still alive");
                }
                shutdownFuture().setDone();
            }
        });
        return shutdownFuture();
    }

    /**
     * @return The shutdown future that is used when calling {@link #shutdown()}
     */
    public FutureDone<Void> shutdownFuture() {
        return futureChannelCreationDone;
    }

    /**
     * @return The number of created TCP connections for *all* created TCP connection
     */
    public static int tcpConnectionCount() {
        return CREATED_TCP_CONNECTIONS.get();
    }

    /**
     * @return The number of created UDP connections for *all* created TCP connection
     */
    public static int udpConnectionCount() {
        return CREATED_UDP_CONNECTIONS.get();
    }

    /**
     * Resets the statistical data.
     */
    public static void resetConnectionCounts() {
        CREATED_TCP_CONNECTIONS.set(0);
        CREATED_UDP_CONNECTIONS.set(0);
    }
}
