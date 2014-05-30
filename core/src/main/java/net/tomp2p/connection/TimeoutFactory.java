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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.InetSocketAddress;

import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Decoder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.peers.PeerStatusListener.FailReason;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory that creates timeout handlers.
 * 
 * @author Thomas Bocek
 * 
 */
public class TimeoutFactory {
    private static final Logger LOG = LoggerFactory.getLogger(TimeoutFactory.class);

    private final FutureResponse futureResponse;
    private final int timeoutSeconds;
    private final PeerStatusListener[] peerStatusListeners;
    private final String name;

    /**
     * @param futureResponse
     *            The future that will be called if a timeout occured
     * @param timeoutSeconds
     *            The time for a timeout
     * @param peerStatusListeners
     *            The listeners that get notified when a timeout happend
     */
    public TimeoutFactory(final FutureResponse futureResponse, final int timeoutSeconds,
            final PeerStatusListener[] peerStatusListeners, final String name) {
        this.futureResponse = futureResponse;
        this.timeoutSeconds = timeoutSeconds;
        this.peerStatusListeners = peerStatusListeners;
        this.name = name;
    }

    /**
     * @return Two handlers, one default Netty that will call the second handler
     */
    public ChannelHandler idleStateHandlerTomP2P() {
        return new IdleStateHandlerTomP2P(timeoutSeconds);
    }
    
    /**
     * @return Two handlers, one default Netty that will call the second handler
     */
    public ChannelHandler timeHandler() {
        return new TimeHandler(futureResponse, peerStatusListeners, name);
    }
    
    public static void removeTimeout(ChannelHandlerContext ctx) {
        if (ctx.channel().pipeline().names().contains("timeout0")) {
             ctx.channel().pipeline().remove("timeout0");
        }
        if (ctx.channel().pipeline().names().contains("timeout1")) {
            ctx.channel().pipeline().remove("timeout1");
        }
    }

    /**
     * The timeout handler that gets called form the {@link IdleStateHandler}.
     * 
     * @author Thomas Bocek
     * 
     */
    private static class TimeHandler extends ChannelDuplexHandler {

        private final FutureResponse futureResponse;
        private final PeerStatusListener[] peerStatusListeners;
        private final String name;

        /**
         * @param futureResponse
         *            The future that will be called if a timeout occured. Can be null if we are server, if we are client, futureResponse will be set
         * @param peerStatusListeners
         *            The listeners that get notified when a timeout happend
         */
        public TimeHandler(final FutureResponse futureResponse, final PeerStatusListener[] peerStatusListeners, final String name) {
            this.futureResponse = futureResponse;
            this.peerStatusListeners = peerStatusListeners;
            this.name = name;
        }

        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
            if (evt instanceof IdleStateHandlerTomP2P) {
                LOG.warn("channel timeout for channel {} {}", name, ctx.channel());
                final PeerAddress recipient;
                if (futureResponse != null) {
                    LOG.warn("Request status is {}", futureResponse.getRequest());
                    ctx.channel().close().addListener(new GenericFutureListener<ChannelFuture>() {
                        @Override
                        public void operationComplete(final ChannelFuture future) throws Exception {
                            futureResponse.setFailed("channel is idle " + evt);
                        }
                    });
                    
                    recipient = futureResponse.getRequest().getRecipient();
                } else {
                    ctx.close();
                    // check if we have set an attribute at least (if we have already decoded the header)
                    final Attribute<PeerAddress> pa = ctx.attr(Decoder.PEER_ADDRESS_KEY);
                    recipient = pa.get();
                }
                
                if(peerStatusListeners == null) {
                	return;
                }

                for (PeerStatusListener peerStatusListener : peerStatusListeners) {
                    if (recipient != null) {
                        peerStatusListener.peerFailed(recipient, FailReason.Timeout);
                    } else {
                        InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel()
                                .remoteAddress();
                        if (inetSocketAddress == null) {
                            final Attribute<InetSocketAddress> pa = ctx.attr(Decoder.INET_ADDRESS_KEY);
                            inetSocketAddress = pa.get();
                        }
                        if (inetSocketAddress != null) {
                            peerStatusListener.peerFailed(
                                    new PeerAddress(Number160.ZERO, inetSocketAddress.getAddress()), FailReason.Timeout);
                        } else {
                            LOG.warn("Cannot determine the address!");
                        }
                    }
                }
            }
        }
    }
}
