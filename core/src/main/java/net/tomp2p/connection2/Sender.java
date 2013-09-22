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

import java.util.LinkedHashMap;
import java.util.Map;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.GenericFutureListener;
import net.tomp2p.futures.Cancel;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message2;
import net.tomp2p.message.TomP2PCumulationTCP;
import net.tomp2p.message.TomP2POutbound;
import net.tomp2p.message.TomP2PSinglePacketUDP;
import net.tomp2p.peers.PeerStatusListener;

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

    /**
     * Creates a new sender with the listeners for offline peers.
     * 
     * @param peerStatusListeners
     *            The listener for offline peers
     * @param channelClientConfiguration
     *            The configuration used to get the signature factory
     */
    public Sender(final PeerStatusListener[] peerStatusListeners,
            final ChannelClientConfiguration channelClientConfiguration) {
        this.peerStatusListeners = peerStatusListeners;
        this.channelClientConfiguration = channelClientConfiguration;
    }

    /**
     * Send a message via TCP.
     * 
     * @param handler
     *            The handler to deal with a reply message
     * @param futureResponse
     *            The future to set the response
     * @param message
     *            The message to send
     * @param channelCreator
     *            The channel creator for the UPD channel
     * @param idleTCPSeconds
     *            The idle time of a message until we fail
     * @param connectTimeoutMillis
     *            The idle we set for the connection setup
     */
    public void sendTCP(final SimpleChannelInboundHandler<Message2> handler,
            final FutureResponse futureResponse, final Message2 message, final ChannelCreator channelCreator,
            final int idleTCPSeconds, final int connectTimeoutMillis, final PeerConnection peerConnection) {
        // no need to continue if we already finished
        if (futureResponse.isCompleted()) {
            return;
        }
        final TimeoutFactory timeoutHandler = createTimeoutHandler(futureResponse, idleTCPSeconds,
                handler == null);
        
        final ChannelFuture channelFuture;
        if (peerConnection != null && peerConnection.channelFuture() != null
                && peerConnection.channelFuture().channel().isActive()) {
            ChannelHandler[] c = timeoutHandler.twoTimeoutHandlers();
            channelFuture = peerConnection.channelFuture();
            channelFuture.channel().pipeline().replace("timeout0", "timeout0", c[0]);
            channelFuture.channel().pipeline().replace("timeout1", "timeout1", c[1]);
            channelFuture
                    .channel()
                    .pipeline()
                    .replace("decoder", "decoder",
                            new TomP2PCumulationTCP(channelClientConfiguration.signatureFactory()));
            channelFuture
                    .channel()
                    .pipeline()
                    .replace("encoder", "encoder",
                            new TomP2POutbound(false, channelClientConfiguration.signatureFactory()));
            channelFuture.channel().pipeline().replace("handler", "handler", handler);
        } else {
            final int nrTCPHandlers;
            final ChannelHandler[] c;
            if (timeoutHandler != null) {
                c = timeoutHandler.twoTimeoutHandlers();
                nrTCPHandlers = 7; // 5 / 0.75;
            } else {
                c = null;
                nrTCPHandlers = 3; // 2 / 0.75;
            }
            final Map<String, ChannelHandler> handlers = new LinkedHashMap<String, ChannelHandler>(
                    nrTCPHandlers);
            
            if (timeoutHandler != null) {
                handlers.put("timeout0", c[0]);
                handlers.put("timeout1", c[1]);
            }
            handlers.put("decoder", new TomP2PCumulationTCP(channelClientConfiguration.signatureFactory()));
            handlers.put("encoder", new TomP2POutbound(false, channelClientConfiguration.signatureFactory()));
            if (timeoutHandler != null) {
                handlers.put("handler", handler);
            }

            channelFuture = channelCreator.createTCP(message.getRecipient().createSocketUDP(),
                    connectTimeoutMillis, handlers);
            if (peerConnection != null) {
                peerConnection.channelFuture(channelFuture);
            }
        }
        futureResponse.setChannelFuture(channelFuture);
        if (channelFuture == null) {
            futureResponse.setFailed("could not create a TCP channel");
        } else {
            afterConnect(futureResponse, message, channelFuture, handler == null);
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
    public void sendUDP(final SimpleChannelInboundHandler<Message2> handler,
            final FutureResponse futureResponse, final Message2 message, final ChannelCreator channelCreator,
            final int idleUDPSeconds, final boolean broadcast) {
        // no need to continue if we already finished
        if (futureResponse.isCompleted()) {
            return;
        }
        boolean isFireAndForget = handler == null;
        final TimeoutFactory timeoutHandler = createTimeoutHandler(futureResponse, idleUDPSeconds,
                isFireAndForget);

        final Map<String, ChannelHandler> handlers;
        if (isFireAndForget) {
            final int nrTCPHandlers = 3; // 2 / 0.75
            handlers = new LinkedHashMap<String, ChannelHandler>(nrTCPHandlers);
        } else {
            final int nrTCPHandlers = 7; // 5 / 0.75
            handlers = new LinkedHashMap<String, ChannelHandler>(nrTCPHandlers);
            ChannelHandler[] c = timeoutHandler.twoTimeoutHandlers();
            handlers.put("timeout0", c[0]);
            handlers.put("timeout1", c[1]);
        }

        handlers.put("decoder", new TomP2PSinglePacketUDP(channelClientConfiguration.signatureFactory()));
        handlers.put("encoder", new TomP2POutbound(false, channelClientConfiguration.signatureFactory()));
        if (!isFireAndForget) {
            handlers.put("handler", handler);
        }

        final ChannelFuture channelFuture = channelCreator.createUDP(
                message.getRecipient().createSocketUDP(), broadcast, handlers);
        futureResponse.setChannelFuture(channelFuture);
        if (channelFuture == null) {
            futureResponse.setFailed("could not create a UDP channel");
        } else {
            afterConnect(futureResponse, message, channelFuture, handler == null);
        }
    }

    /**
     * Create a timeout handler or null if its a fire and forget. In this case we don't expect a reply and we don't need
     * a timeout.
     * 
     * @param futureResponse
     *            The future to set the response
     * @param idleMillis
     *            The timeout
     * @param fireAndForget
     *            True, if we don't expect a message
     * @return The timeout creator that will create timeout handlers
     */
    private TimeoutFactory createTimeoutHandler(final FutureResponse futureResponse, final int idleMillis,
            final boolean fireAndForget) {
        return fireAndForget ? null : new TimeoutFactory(futureResponse, idleMillis, peerStatusListeners);
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
    private void afterConnect(final FutureResponse futureResponse, final Message2 message,
            final ChannelFuture channelFuture, final boolean fireAndForget) {
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
                    LOG.warn("Channel creation failed ", future.cause());
                }
            }
        });
    }

    /**
     * After sending, we check if the write was successful or if it was a fire and forget.
     * 
     * @param writeFuture
     *            The future of the write operation. Can be UDP or TCP
     * @param futureResponse
     *            The future to set the response
     * @param fireAndForget
     *            True, if we don't expect a message
     */
    private void afterSend(final ChannelFuture writeFuture, final FutureResponse futureResponse,
            final boolean fireAndForget) {
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
