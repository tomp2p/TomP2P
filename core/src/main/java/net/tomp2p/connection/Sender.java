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

package net.tomp2p.connection;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Cancel;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.TomP2PCumulationTCP;
import net.tomp2p.message.TomP2POutbound;
import net.tomp2p.message.TomP2PSinglePacketUDP;
import net.tomp2p.p2p.builder.PingBuilder;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class that sends out messages.
 * 
 * @author Thomas Bocek
 * 
 */
public class Sender {

    private static final Logger LOG = LoggerFactory.getLogger(Sender.class);
    private final PeerStatusListener[] peerStatusListeners;
    private final ChannelClientConfiguration channelClientConfiguration;
    private final Dispatcher dispatcher;
    private PingBuilder pingBuilder;

    /**
     * * Creates a new sender with the listeners for offline peers. * * @param
     * peerStatusListeners * The listener for offline peers * @param
     * channelClientConfiguration * The configuration used to get the signature
     * factory * @param dispatcher
     */
    public Sender(final PeerStatusListener[] peerStatusListeners, final ChannelClientConfiguration channelClientConfiguration, Dispatcher dispatcher) {
        this.peerStatusListeners = peerStatusListeners;
        this.channelClientConfiguration = channelClientConfiguration;
        this.dispatcher = dispatcher;
    }

    public ChannelClientConfiguration channelClientConfiguration() {
        return channelClientConfiguration;
    }

    public PingBuilder pingBuilder() {
        return pingBuilder;
    }

    public Sender pingBuilder(PingBuilder pingBuilder) {
        this.pingBuilder = pingBuilder;
        return this;
    }

    //TODO: if message.getRecipient() is me, than call dispatcher directly without sending over Internet. 
    public void sendTCP(final SimpleChannelInboundHandler<Message> handler, final FutureResponse futureResponse, final Message message, final ChannelCreator channelCreator,
            final int idleTCPSeconds, final int connectTimeoutMillis, final PeerConnection peerConnection) {
        // no need to continue if we already finished
        if (futureResponse.isCompleted()) {
            return;
        }
        ChannelFuture channelFuture = null;
        if (peerConnection != null && peerConnection.channelFuture() != null && peerConnection.channelFuture().channel().isActive()) {
            channelFuture = sendTCPPeerConnection(peerConnection, handler, channelCreator, futureResponse);
            afterConnect(futureResponse, message, channelFuture, handler == null);
        } else if (channelCreator != null) {
            final TimeoutFactory timeoutHandler = createTimeoutHandler(futureResponse, idleTCPSeconds, handler == null);
            InetSocketAddress recipient = null;
            if (message.getRecipient().isRelay()) {
                FutureDone<PeerSocketAddress> futurePing = pingFirst(message.getRecipient().getPeerSocketAddresses(), pingBuilder);
                futurePing.addListener(new BaseFutureAdapter<FutureDone<PeerSocketAddress>>() {
                    @Override
                    public void operationComplete(final FutureDone<PeerSocketAddress> futureDone) throws Exception {
                        if (futureDone.isSuccess()) {
                            InetSocketAddress recipient = PeerSocketAddress.createSocketTCP(futureDone.getObject());
                            ChannelFuture channelFuture = sendTCPCreateChannel(recipient, channelCreator, peerConnection, handler, timeoutHandler, connectTimeoutMillis,
                                    futureResponse);
                            afterConnect(futureResponse, message, channelFuture, handler == null);

                            futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
                                @Override
                                public void operationComplete(FutureResponse future) throws Exception {
                                    if (future.isFailed()) {

                                        if (future.getResponse() != null && future.getResponse().getType() != Message.Type.USER1) {
                                            for (int i = 0; i < message.getRecipient().getPeerSocketAddresses().length; i++) {
                                                if (message.getRecipient().getPeerSocketAddresses()[i].equals(futureDone.getObject())) {
                                                    message.getRecipient().getPeerSocketAddresses()[i] = null;
                                                    break;
                                                }
                                            }
                                            message.getRecipient().getPeerSocketAddresses()[12] = null;
                                            sendTCP(handler, futureResponse, message, channelCreator, idleTCPSeconds, connectTimeoutMillis, peerConnection);
                                        }
                                    }

                                }
                            });

                        } else {
                            futureResponse.setFailed("no relay could be contacted");
                        }
                    }
                });

            } else {
                recipient = message.getRecipient().createSocketTCP();
                channelFuture = sendTCPCreateChannel(recipient, channelCreator, peerConnection, handler, timeoutHandler, connectTimeoutMillis, futureResponse);
                afterConnect(futureResponse, message, channelFuture, handler == null);
            }
        }
    }

    private FutureDone<PeerSocketAddress> pingFirst(PeerSocketAddress[] peerSocketAddresses, PingBuilder pingBuilder) {
        final FutureDone<PeerSocketAddress> futureDone = new FutureDone<PeerSocketAddress>();

        BaseFuture[] forks = new BaseFuture[peerSocketAddresses.length];
        for (int i = 0; i < forks.length; i++) {
            if (peerSocketAddresses[i] != null) {
                InetSocketAddress inetSocketAddress = PeerSocketAddress.createSocketUDP(peerSocketAddresses[i]);
                forks[i] = pingBuilder.setInetAddress(inetSocketAddress.getAddress()).setPort(inetSocketAddress.getPort()).start();
            }
        }
        FutureForkJoin<BaseFuture> ffk = new FutureForkJoin<BaseFuture>(1, true, new AtomicReferenceArray<>(forks));
        ffk.addListener(new BaseFutureAdapter<FutureForkJoin<BaseFuture>>() {
            @Override
            public void operationComplete(FutureForkJoin<BaseFuture> future) throws Exception {
                if (future.isSuccess()) {
                    futureDone.setDone(((FutureResponse) (future.getCompleted().get(0))).getResponse().getSender().peerSocketAddress());
                }
            }
        });
        return futureDone;
    }

    private ChannelFuture sendTCPCreateChannel(InetSocketAddress recipient, ChannelCreator channelCreator, PeerConnection peerConnection, ChannelHandler handler,
            TimeoutFactory timeoutHandler, int connectTimeoutMillis, FutureResponse futureResponse) {

        final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;
        if (timeoutHandler != null) {
            final int nrTCPHandlers = peerConnection != null ? 10 : 7; // 10 = 7 / 0.75 ** 7 = 5 / 0.75;
            handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrTCPHandlers);
            handlers.put("timeout0", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.idleStateHandlerTomP2P()));
            handlers.put("timeout1", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.timeHandler()));
        } else {
            final int nrTCPHandlers = 3; // 2 / 0.75;
            handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrTCPHandlers);
        }

        handlers.put("decoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2PCumulationTCP(channelClientConfiguration.signatureFactory())));
        handlers.put("encoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2POutbound(false, channelClientConfiguration.signatureFactory())));

        if (peerConnection != null) {
            // we expect replies on this connection
            handlers.put("dispatcher", new Pair<EventExecutorGroup, ChannelHandler>(null, dispatcher));
        }

        if (timeoutHandler != null) {
            handlers.put("handler", new Pair<EventExecutorGroup, ChannelHandler>(null, handler));
        }

        HeartBeat heartBeat = null;
        if (peerConnection != null) {
            heartBeat = new HeartBeat(peerConnection.heartBeatMillis(), TimeUnit.MILLISECONDS, pingBuilder);
            handlers.put("heartbeat", new Pair<EventExecutorGroup, ChannelHandler>(null, heartBeat));
        }

        ChannelFuture channelFuture = channelCreator.createTCP(recipient, connectTimeoutMillis, handlers, futureResponse);

        if (peerConnection != null) {
            peerConnection.channelFuture(channelFuture);
            heartBeat.peerConnection(peerConnection);
        }

        return channelFuture;
    }

    private ChannelFuture sendTCPPeerConnection(PeerConnection peerConnection, ChannelHandler handler, final ChannelCreator channelCreator, final FutureResponse futureResponse) {
        //if the channel gets closed, the future should get notified
        ChannelFuture channelFuture = peerConnection.channelFuture();
        //channelCreator can be null if we don't need to create any channels
        if (channelCreator != null) {
            channelCreator.setupCloseListener(channelFuture, futureResponse);
        }
        ChannelPipeline pipeline = channelFuture.channel().pipeline();

        // we need to replace the handler if this comes from the peer that
        // create a peerconnection, otherwise we
        // need to add a handler
        addOrReplace(pipeline, "dispatcher", "handler", handler);
        // uncomment this if the recipient should also heartbeat
        // addIfAbsent(pipeline, "handler", "heartbeat",
        // new HeartBeat(2, pingBuilder).peerConnection(peerConnection));
        return channelFuture;
    }

    // private boolean addIfAbsent(ChannelPipeline pipeline, String before,
    // String name,
    // ChannelHandler channelHandler) {
    // List<String> names = pipeline.names();
    // if (names.contains(name)) {
    // return false;
    // } else {
    // if (before == null) {
    // pipeline.addFirst(name, channelHandler);
    // } else {
    // pipeline.addBefore(before, name, channelHandler);
    // }
    // return true;
    // }
    // }

    private boolean addOrReplace(ChannelPipeline pipeline, String before, String name, ChannelHandler channelHandler) {
        List<String> names = pipeline.names();
        if (names.contains(name)) {
            pipeline.replace(name, name, channelHandler);
            return false;
        } else {
            if (before == null) {
                pipeline.addFirst(name, channelHandler);
            } else {
                pipeline.addBefore(before, name, channelHandler);
            }
            return true;
        }
    }

    /**
     * Send a message via UDP.
     * 
     * @param handler
     *            The handler to deal with a reply message
     * @param futureResponse
     *            The future to set the response
     * @param message
     *            The message to send
     * @param channelCreator
     *            The channel creator for the UPD channel
     * @param idleUDPSeconds
     *            The idle time of a message until we fail
     * @param broadcast
     *            True to send via layer 2 broadcast
     */
    //TODO: if message.getRecipient() is me, than call dispatcher directly without sending over Internet. 
    public void sendUDP(final SimpleChannelInboundHandler<Message> handler, final FutureResponse futureResponse, final Message message, final ChannelCreator channelCreator,
            final int idleUDPSeconds, final boolean broadcast) {
        // no need to continue if we already finished
        if (futureResponse.isCompleted()) {
            return;
        }
        boolean isFireAndForget = handler == null;

        final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;
        if (isFireAndForget) {
            final int nrTCPHandlers = 3; // 2 / 0.75
            handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrTCPHandlers);
        } else {
            final int nrTCPHandlers = 7; // 5 / 0.75
            handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrTCPHandlers);
            final TimeoutFactory timeoutHandler = createTimeoutHandler(futureResponse, idleUDPSeconds, isFireAndForget);
            handlers.put("timeout0", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.idleStateHandlerTomP2P()));
            handlers.put("timeout1", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.timeHandler()));
        }

        handlers.put("decoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2PSinglePacketUDP(channelClientConfiguration.signatureFactory())));
        handlers.put("encoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2POutbound(false, channelClientConfiguration.signatureFactory())));
        if (!isFireAndForget) {
            handlers.put("handler", new Pair<EventExecutorGroup, ChannelHandler>(null, handler));
        }
        if (message.getRecipient().isRelay()) {
            LOG.warn("Tried to send UDP message to unreachable peers. Only TCP messages can be sent to unreachable peers");
            futureResponse.setFailed("Tried to send UDP message to unreachable peers. Only TCP messages can be sent to unreachable peers");
        } else {
            final ChannelFuture channelFuture = channelCreator.createUDP(message.getRecipient().createSocketUDP(), broadcast, handlers, futureResponse);
            if (channelFuture == null) {
                futureResponse.setFailed("could not create a UDP channel");
            } else {
                afterConnect(futureResponse, message, channelFuture, handler == null);
            }
        }

    }

    /**
     * Create a timeout handler or null if its a fire and forget. In this case
     * we don't expect a reply and we don't need a timeout.
     * 
     * @param futureResponse
     *            The future to set the response
     * @param idleMillis
     *            The timeout
     * @param fireAndForget
     *            True, if we don't expect a message
     * @return The timeout creator that will create timeout handlers
     */
    private TimeoutFactory createTimeoutHandler(final FutureResponse futureResponse, final int idleMillis, final boolean fireAndForget) {
        return fireAndForget ? null : new TimeoutFactory(futureResponse, idleMillis, peerStatusListeners, "Sender");
    }

    /**
     * After connecting, we check if the connect was successful.
     * 
     * @param futureResponse
     *            The future to set the response
     * @param message
     *            The message to send
     * @param channelFuture
     *            the future of the connect
     * @param fireAndForget
     *            True, if we don't expect a message
     */
    private void afterConnect(final FutureResponse futureResponse, final Message message, final ChannelFuture channelFuture, final boolean fireAndForget) {
        if (channelFuture == null) {
            futureResponse.setFailed("could not create a " + (message.isUdp() ? "UDP" : "TCP") + " channel");
            return;
        }
        final Cancel connectCancel = createCancel(channelFuture);
        futureResponse.addCancel(connectCancel);
        channelFuture.addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                futureResponse.removeCancel(connectCancel);
                if (future.isSuccess()) {
                    futureResponse.setProgressHandler(new ProgresHandler() {
                        @Override
                        public void progres() {
                            final ChannelFuture writeFuture = future.channel().writeAndFlush(message);
                            afterSend(writeFuture, futureResponse, fireAndForget);
                        }
                    });
                    // this needs to be called first before all other progress
                    futureResponse.progressFirst();
                } else {
                    futureResponse.setFailed("Channel creation failed " + future.cause());
                    // may have been closed by the other side,
                    // or it may have been canceled from this side
                    if (!(future.cause() instanceof CancellationException) && !(future.cause() instanceof ClosedChannelException)) {
                        LOG.warn("Channel creation failed ", future.cause());
                    }
                }
            }
        });
    }

    /**
     * After sending, we check if the write was successful or if it was a fire
     * and forget.
     * 
     * @param writeFuture
     *            The future of the write operation. Can be UDP or TCP
     * @param futureResponse
     *            The future to set the response
     * @param fireAndForget
     *            True, if we don't expect a message
     */
    private void afterSend(final ChannelFuture writeFuture, final FutureResponse futureResponse, final boolean fireAndForget) {
        final Cancel writeCancel = createCancel(writeFuture);
        writeFuture.addListener(new GenericFutureListener<ChannelFuture>() {

            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                futureResponse.removeCancel(writeCancel);
                if (!future.isSuccess()) {
                    futureResponse.setFailedLater(future.cause());
                    reportFailed(futureResponse, future.channel().close());
                    LOG.warn("Failed to write channel the request {}", futureResponse.getRequest(), future.cause());

                }
                if (fireAndForget) {
                    futureResponse.setResponseLater(null);
                    LOG.debug("fire and forget, close channel now");
                    reportMessage(futureResponse, future.channel().close());
                }
            }
        });

    }

    /**
     * Report a failure after the channel was closed.
     * 
     * @param futureResponse
     *            The future to set the response
     * @param close
     *            The close future
     * @param cause
     *            The response message
     */
    private void reportFailed(final FutureResponse futureResponse, final ChannelFuture close) {
        close.addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(final ChannelFuture arg0) throws Exception {
                futureResponse.setResponseNow();
            }
        });
    }

    /**
     * Report a successful response after the channel was closed.
     * 
     * @param futureResponse
     *            The future to set the response
     * @param close
     *            The close future
     * @param responseMessage
     *            The response message
     */
    private void reportMessage(final FutureResponse futureResponse, final ChannelFuture close) {
        close.addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(final ChannelFuture arg0) throws Exception {
                futureResponse.setResponseNow();
            }
        });
    }

    /**
     * @param channelFuture
     *            The channel future that can be canceled
     * @return Create a cancel class for the channel future
     */
    private static Cancel createCancel(final ChannelFuture channelFuture) {
        return new Cancel() {
            @Override
            public void cancel() {
                channelFuture.cancel(true);
            }
        };
    }
}
