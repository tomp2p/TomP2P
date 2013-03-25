/*
 * Copyright 2011 Thomas Bocek
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

package net.tomp2p.connection;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureChannel;
import net.tomp2p.message.TomP2PDecoderTCP;
import net.tomp2p.message.TomP2PDecoderUDP;
import net.tomp2p.message.TomP2PEncoderTCP;
import net.tomp2p.message.TomP2PEncoderUDP;
import net.tomp2p.p2p.Statistics;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.rpc.RequestHandlerUDP;

import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictor;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the channels. This class is created by {@link ConnectionReservation}
 * and should never be called directly.
 * 
 * @author Thomas Bocek
 */
public class ChannelCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelCreator.class);

    private static final FutureChannel FAILED_FUTURE = new FutureChannel();

    private final Semaphore connectionSemaphore;

    private final ChannelGroup channelsTCP = new DefaultChannelGroup("TomP2P ConnectionPool TCP");

    private final ChannelGroup channelsUDP = new DefaultChannelGroup("TomP2P ConnectionPool UDP");

    private final String channelCreatorName;

    private final long creatorThread;

    // objects needed to create the connection
    private final MessageLogger messageLoggerFilter;

    private final ChannelFactory tcpClientChannelFactory;

    private final ChannelFactory udpChannelFactory;

    // indicates if the TCP connections are kept alive
    private final boolean keepAliveAndReuse;

    private final Map<InetSocketAddress, ChannelFuture> cacheMap;

    private final Statistics statistics;

    private final int permits;

    private final Scheduler scheduler;

    private boolean shutdownUDP;

    private boolean shutdownTCP;

    private AtomicInteger permitsCount;

    static {
        FAILED_FUTURE.setFailed("shutting down!");
    }

    /**
     * Package private constructor, since this is created by
     * {@link ConnectionReservation} and should never be called directly.
     * 
     * @param permits
     *            The number of max. parallel connections.
     * @param statistics
     *            The class that counts the created TCP and UDP connections.
     * @param messageLoggerFilter
     * @param tcpClientChannelFactory
     * @param udpClientChannelFactory
     * @param keepAliveAndReuse
     */
    ChannelCreator(int permits, Statistics statistics, MessageLogger messageLoggerFilter,
            ChannelFactory tcpClientChannelFactory, ChannelFactory udpClientChannelFactory, boolean keepAliveAndReuse,
            String name, long creatorThread, Scheduler scheduler) {
        this.permitsCount = new AtomicInteger(permits);
        this.connectionSemaphore = new Semaphore(permits);
        this.cacheMap = new ConcurrentHashMap<InetSocketAddress, ChannelFuture>(permits);
        this.messageLoggerFilter = messageLoggerFilter;
        this.tcpClientChannelFactory = tcpClientChannelFactory;
        this.udpChannelFactory = udpClientChannelFactory;
        this.keepAliveAndReuse = keepAliveAndReuse;
        this.statistics = statistics;
        this.channelCreatorName = name;
        this.creatorThread = creatorThread;
        this.permits = permits;
        this.scheduler = scheduler;
    }

    /**
     * Creates a UDP channel.
     * 
     * @param timeoutHandler
     *            The handler that deals with timeouts
     * @param requestHandler
     *            The handler that deals with incoming replies
     * @param broadcast
     *            Set to true if broadcast is allowed
     * @return The created channel or null if we are shutting down.
     */
    public FutureChannel createUDPChannel(ReplyTimeoutHandler timeoutHandler,
            RequestHandlerUDP<? extends BaseFuture> requestHandler, boolean broadcast) {
        synchronized (channelsUDP) {
            if (shutdownUDP) {
                return FAILED_FUTURE;
            }
            final FutureChannel futureChannelCreation = new FutureChannel();
            createUDPChannel(futureChannelCreation, timeoutHandler, requestHandler, broadcast);
            return futureChannelCreation;
        }
    }

    private void createUDPChannel(FutureChannel futureChannelCreation, ReplyTimeoutHandler timeoutHandler,
            RequestHandlerUDP<? extends BaseFuture> requestHandler, boolean broadcast) {
        // If we are out of semaphores, we cannot create any channels. Since we
        // know how many channels max. in parallel are created, we can reserve
        // it.
        if (!futureChannelCreation.isAcquired() && !connectionSemaphore.tryAcquire()) {
            connectionNotReadyYetUDP(futureChannelCreation, timeoutHandler, requestHandler, broadcast,
                    connectionSemaphore);
            return;
        }
        statistics.incrementUDPChannelCreation();
        // now, we don't exceeded the limits, so create channels
        Channel channel;
        try {
            channel = createChannelUDP(timeoutHandler, requestHandler, broadcast);
            futureChannelCreation.setChannel(channel);
        } catch (Exception e) {
            futureChannelCreation.setFailed("Cannot create channel " + e);
            connectionSemaphore.release();
            statistics.decrementUDPChannelCreation();
            return;
        }

        channel.getCloseFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                connectionSemaphore.release();
                statistics.decrementUDPChannelCreation();
            }
        });
        channelsUDP.add(channel);
    }

    /**
     * Creates a TCP channel future. Once the future finishes, the channel can
     * be used to connect to peers.
     * 
     * @param timeoutHandler
     *            The handler that deals with timeouts
     * @param requestHandler
     *            The handler that deals with incoming replies
     * @param connectTimeoutMillis
     *            The timeout after which a connection attempt is considered a
     *            failure
     * @param recipient
     *            The recipient to create the connection. If the recipient is
     *            already open, the connection will be reused.
     * @return The channel future
     */
    public FutureChannel createTCPChannel(ReplyTimeoutHandler timeoutHandler,
            RequestHandlerTCP<? extends BaseFuture> requestHandler, int connectTimeoutMillis,
            final InetSocketAddress recipient) {
        synchronized (channelsTCP) {
            if (shutdownTCP) {
                return FAILED_FUTURE;
            }
            final FutureChannel futureChannelCreation = new FutureChannel();
            createTCPChannel(futureChannelCreation, timeoutHandler, requestHandler, connectTimeoutMillis, recipient);
            return futureChannelCreation;
        }
    }

    private void createTCPChannel(final FutureChannel futureChannelCreation, ReplyTimeoutHandler timeoutHandler,
            final RequestHandlerTCP<? extends BaseFuture> requestHandler, int connectTimeoutMillis,
            final InetSocketAddress recipient) {

        ChannelFuture channelFuture;
        boolean newConnection = true;
        if (keepAliveAndReuse) {
            channelFuture = cacheMap.get(recipient);
            if (channelFuture == null) {
                // If we are out of semaphores, we cannot create any channels.
                // Since we know how many channels max. in parallel are created,
                // we can reserve it. The acquiering can be done in Scheduler
                //
                // The connectionSemaphore can be either acquired in this place
                // or in the Scheduler.
                if (!futureChannelCreation.isAcquired() && !connectionSemaphore.tryAcquire()) {
                    connectionNotReadyYetTCP(futureChannelCreation, timeoutHandler, requestHandler,
                            connectTimeoutMillis, recipient, connectionSemaphore);
                    return;
                }
                statistics.incrementTCPChannelCreation();
                try {
                    // now, we don't exceeded the limits, so create channels
                    channelFuture = createChannelTCP(timeoutHandler, requestHandler, recipient,
                            new InetSocketAddress(0), connectTimeoutMillis);
                    futureChannelCreation.setChannelFuture(channelFuture);
                    channelFuture.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(final ChannelFuture future) throws Exception {
                            if (future.isSuccess()) {
                                futureChannelCreation.setChannel(future.getChannel());
                            } else {
                                futureChannelCreation.setFailed("ChannelFuture failed (TCP, reuse)", future.getCause());
                                connectionSemaphore.release();
                                statistics.decrementTCPChannelCreation();
                            }
                        }
                    });
                    cacheMap.put(recipient, channelFuture);
                } catch (Exception e) {
                    futureChannelCreation.setFailed("Cannot create channel (TCP, reuse)", e);
                    connectionSemaphore.release();
                    statistics.decrementTCPChannelCreation();
                    return;
                }
            } else {
                futureChannelCreation.setChannelFuture(channelFuture);
                newConnection = false;
                Channel channel = channelFuture.getChannel();
                // we can keep our old timeouthandler since for keep-alive
                // connections, this is still valid.
                // we need a new RequestHandlerTCP in order for the new message
                channel.getPipeline().replace("request", "request", requestHandler);
                futureChannelCreation.setChannel(channel);
            }
        } else {
            // If we are out of semaphores, we cannot create any channels. Since
            // we know how many channels max. in parallel are created, we can
            // reserve it.
            if (!futureChannelCreation.isAcquired() && !connectionSemaphore.tryAcquire()) {
                connectionNotReadyYetTCP(futureChannelCreation, timeoutHandler, requestHandler, connectTimeoutMillis,
                        recipient, connectionSemaphore);
                return;
            }
            statistics.incrementTCPChannelCreation();
            try {
                channelFuture = createChannelTCP(timeoutHandler, requestHandler, recipient, new InetSocketAddress(0),
                        connectTimeoutMillis);
                futureChannelCreation.setChannelFuture(channelFuture);
                channelFuture.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(final ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            futureChannelCreation.setChannel(future.getChannel());
                        } else {
                            futureChannelCreation.setFailed("ChannelFuture failed (TCP)", future.getCause());
                            connectionSemaphore.release();
                            statistics.decrementTCPChannelCreation();
                        }
                    }
                });
            } catch (Exception e) {
                futureChannelCreation.setFailed("Cannot create channel (TCP)", e);
                connectionSemaphore.release();
                statistics.decrementTCPChannelCreation();
                return;
            }
        }
        final Channel channel = channelFuture.getChannel();
        if (newConnection) {
            channel.getCloseFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(final ChannelFuture future) throws Exception {
                    if (LOGGER.isDebugEnabled()) {
                        
                        if (requestHandler.getFutureResponse() != null) {
                            LOGGER.debug("channel close, set failure for request message: "
                                    + requestHandler.getFutureResponse().getFailedReason());
                        } else {
                            LOGGER.debug("channel close");    
                        }
                    }
                    connectionSemaphore.release();
                    statistics.decrementTCPChannelCreation();
                    if (keepAliveAndReuse) {
                        cacheMap.remove(recipient);
                    }
                }
            });
        }
        channelsTCP.add(channel);
    }

    private void connectionNotReadyYetTCP(final FutureChannel futureChannelCreation,
            final ReplyTimeoutHandler timeoutHandler, final RequestHandlerTCP<? extends BaseFuture> requestHandler,
            final int connectTimeoutMillis, final InetSocketAddress recipient, final Semaphore connectionSemaphore2) {
        scheduler.addConnectionQueue(futureChannelCreation, connectionSemaphore2, new Runnable() {
            @Override
            public void run() {
                createTCPChannel(futureChannelCreation, timeoutHandler, requestHandler, connectTimeoutMillis, recipient);
            }
        });
    }

    private void connectionNotReadyYetUDP(final FutureChannel futureChannelCreation,
            final ReplyTimeoutHandler timeoutHandler, final RequestHandlerUDP<? extends BaseFuture> requestHandler,
            final boolean broadcast, final Semaphore connectionSemaphore2) {
        scheduler.addConnectionQueue(futureChannelCreation, connectionSemaphore2, new Runnable() {
            @Override
            public void run() {
                createUDPChannel(futureChannelCreation, timeoutHandler, requestHandler, broadcast);
            }
        });

    }

    /**
     * Creates a channel the Netty way. We set soLinger to 0 since we may end up
     * with too many connections in a WAIT state. Setting soLinger to 0 sends
     * back an RST in case of a close, which may get an exception
     * "connection reset by peer".
     * 
     * @param timeoutHandler
     *            The handler that deals with timeouts
     * @param requestHandler
     *            The handler that deals with incoming replies
     * @param remoteAddress
     *            The remote address we connect to
     * @param localAddress
     *            The local address we bind to
     * @param connectionTimoutMillis
     *            The timeout after which a connection attempt is considered a
     *            failure
     * @return The channel future
     */
    private ChannelFuture createChannelTCP(ChannelHandler timeoutHandler, ChannelHandler requestHandler,
            SocketAddress remoteAddress, SocketAddress localAddress, int connectionTimoutMillis) {
        ClientBootstrap bootstrap = new ClientBootstrap(tcpClientChannelFactory);
        // this option is used in Netty
        bootstrap.setOption("connectTimeoutMillis", connectionTimoutMillis);
        setupBootstrapTCP(bootstrap, timeoutHandler, requestHandler, new TomP2PDecoderTCP(), new TomP2PEncoderTCP(),
                new ChunkedWriteHandler(), messageLoggerFilter);
        ChannelFuture channelFuture = bootstrap.connect(remoteAddress);
        // try to set, otherwise give up if not supported
        trySetOption(channelFuture.getChannel(), "tcpNoDelay", true);
        trySetOption(channelFuture.getChannel(), "soLinger", 0);
        trySetOption(channelFuture.getChannel(), "reuseAddress", true);
        trySetOption(channelFuture.getChannel(), "keepAlive", true);
        return channelFuture;
    }

    private void trySetOption(Channel channel, String name, Object value) {
        try {
            channel.getConfig().setOption(name, value);
        } catch (ChannelException e) {
            // try hard
        }
    }

    /**
     * Creates a channel the Netty way. We need to set the receive buftfer,
     * since we need to reserve enough space and the default 786 bytes is not
     * enough.
     * 
     * @param timeoutHandler
     *            The handler that deals with timeouts
     * @param requestHandler
     *            The handler that deals with incoming replies
     * @param allowBroadcast
     *            Set to true if broadcast is allowed
     * @return The channel
     */
    private Channel createChannelUDP(ChannelHandler timeoutHandler, ChannelHandler requestHandler,
            boolean allowBroadcast) {
        ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(udpChannelFactory);
        setupBootstrapUDP(bootstrap, timeoutHandler, requestHandler, new TomP2PDecoderUDP(), new TomP2PEncoderUDP(),
                messageLoggerFilter);
        // enable per default, as we support a broadcast ping to find other
        // peers.
        bootstrap.setOption("broadcast", allowBroadcast ? true : false);
        bootstrap.setOption("receiveBufferSizePredictor", new FixedReceiveBufferSizePredictor(
                ConnectionHandler.UDP_LIMIT));
        Channel c = bootstrap.bind(new InetSocketAddress(0));
        return c;
    }

    /**
     * Fill the TCP pipeline with handlers. This pipeline contains the streamer,
     * which the UDP version doesnt.
     * 
     * @param bootstrap
     *            The bootstrap object with settings
     * @param timeoutHandler
     *            The handler that deals with timeouts
     * @param requestHandler
     *            The handler that deals with incoming replies
     * @param decoder
     *            The message decoder that converts from a Netty byte buffer to
     *            an {@link Message} object
     * @param encoder
     *            The message encoder that converts from a {@link Message}
     *            object to a Netty byte buffer
     * @param streamer
     *            The chunk streamer that deals with partial data.
     * @param messageLoggerFilter
     *            The handler to log what was sent over the wire
     */
    private static void setupBootstrapTCP(Bootstrap bootstrap, ChannelHandler timeoutHandler,
            ChannelHandler requestHandler, ChannelUpstreamHandler decoder, ChannelDownstreamHandler encoder,
            ChunkedWriteHandler streamer, ChannelHandler messageLoggerFilter) {
        ChannelPipeline pipe = bootstrap.getPipeline();
        if (timeoutHandler != null) {
            pipe.addLast("timeout", timeoutHandler);
        }
        pipe.addLast("streamer", streamer);
        pipe.addLast("encoder", encoder);
        pipe.addLast("decoder", decoder);
        if (messageLoggerFilter != null) {
            pipe.addLast("loggerUpstream", messageLoggerFilter);
        }
        if (requestHandler != null) {
            pipe.addLast("request", requestHandler);
        }
    }

    /**
     * Fill the TCP pipeline with handlers. . This pipeline does not contains
     * the streamer, which the UDP version does.
     * 
     * @param bootstrap
     *            The bootstrap object with settings
     * @param timeoutHandler
     *            The handler that deals with timeouts
     * @param requestHandler
     *            The handler that deals with incoming replies
     * @param decoder
     *            The message decoder that converts from a Netty byte buffer to
     *            an {@link Message} object
     * @param encoder
     *            The message encoder that converts from a {@link Message}
     *            object to a Netty byte buffer
     * @param messageLoggerFilter
     *            The handler to log what was sent over the wire
     */
    private static void setupBootstrapUDP(Bootstrap bootstrap, ChannelHandler timeoutHandler,
            ChannelHandler requestHandler, ChannelUpstreamHandler decoder, ChannelDownstreamHandler encoder,
            ChannelHandler messageLoggerFilter) {
        ChannelPipeline pipe = bootstrap.getPipeline();
        if (timeoutHandler != null) {
            pipe.addLast("timeout", timeoutHandler);
        }
        pipe.addLast("encoder", encoder);
        pipe.addLast("decoder", decoder);
        if (messageLoggerFilter != null) {
            pipe.addLast("loggerUpstream", messageLoggerFilter);
        }
        if (requestHandler != null) {
            pipe.addLast("request", requestHandler);
        }
    }

    /**
     * Closes a permanent connection. If no connection existent, then this
     * method returns
     * 
     * @param destination
     *            The address of the destination peer of the permanent
     *            connection.
     * @return The ChannelFuture of the close operation or null if the
     *         connection was not in the cached map.
     */
    public ChannelFuture close(PeerAddress destination) {
        ChannelFuture channelFuture = cacheMap.get(destination.createSocketTCP());
        if (channelFuture != null) {
            return channelFuture.getChannel().close();
        }
        return null;
    }

    /**
     * @return The number of permits, which is the max. number of allowed
     *         parallel connections
     */
    public int getPermits() {
        return permits;
    }

    public int getCurrentPermits() {
        return permitsCount.get();
    }

    /**
     * Releases permits. This can also be a partial release
     * 
     * @param permits
     *            The number of permits to be released
     * @return Returns true if the channel creator has no permits anymore
     */
    boolean release(int freedPermits) {
        int result = permitsCount.addAndGet(-freedPermits);
        if (result < 0) {
            throw new RuntimeException("Cannot release more than I acquired");
        }
        if (result == 0) {
            synchronized (channelsTCP) {
                shutdownTCP = true;
            }
            synchronized (channelsUDP) {
                shutdownUDP = true;
            }
            return true;
        }
        return false;
    }

    /**
     * Shuts down this channelcreator. That means a flag is set and if a
     * connection should be created, null is returned.
     */
    public void shutdown() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("shutting down in ChannelCreator");
        }
        synchronized (channelsTCP) {
            shutdownTCP = true;
        }
        channelsTCP.close().awaitUninterruptibly();
        synchronized (channelsUDP) {
            shutdownUDP = true;
        }
        channelsUDP.close().awaitUninterruptibly();
    }

    /**
     * @return The name of this ChannelCreator, used for debugging
     */
    public String getName() {
        return channelCreatorName;
    }

    public long getCreatorThread() {
        return creatorThread;
    }

    /**
     * @return True if we created a keep-alive connection with this creator
     */
    public boolean isKeepAliveAndReuse() {
        return keepAliveAndReuse;
    }
}
