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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.InetSocketAddress;

import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.TomP2PDecoder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;

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

    /**
     * @param futureResponse
     *            The future that will be called if a timeout occured
     * @param timeoutSeconds
     *            The time for a timeout
     * @param peerStatusListeners
     *            The listeners that get notified when a timeout happend
     */
    public TimeoutFactory(final FutureResponse futureResponse, final int timeoutSeconds,
            final PeerStatusListener[] peerStatusListeners) {
        this.futureResponse = futureResponse;
        this.timeoutSeconds = timeoutSeconds;
        this.peerStatusListeners = peerStatusListeners;
    }

    /**
     * @return Two handlers, one default Netty that will call the second handler
     */
    public ChannelHandler[] twoTimeoutHandlers() {
        return new ChannelHandler[] {new IdleStateHandler(0, 0, timeoutSeconds),
                new TimeHandler(futureResponse, peerStatusListeners) };
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

        /**
         * @param futureResponse
         *            The future that will be called if a timeout occured
         * @param peerStatusListeners
         *            The listeners that get notified when a timeout happend
         */
        public TimeHandler(final FutureResponse futureResponse, final PeerStatusListener[] peerStatusListeners) {
            this.futureResponse = futureResponse;
            this.peerStatusListeners = peerStatusListeners;
        }

        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                LOG.warn("channel timeout for channel {}", ctx.channel());
                final PeerAddress recipient;
                if (futureResponse != null) {
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
                    final Attribute<PeerAddress> pa = ctx.attr(TomP2PDecoder.PEER_ADDRESS_KEY);
                    recipient = pa.get();
                }

                for (PeerStatusListener peerStatusListener : peerStatusListeners) {
                    if (recipient != null) {
                        peerStatusListener.peerFailed(recipient, false);
                    } else {
                        InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel()
                                .remoteAddress();
                        if (inetSocketAddress == null) {
                            final Attribute<InetSocketAddress> pa = ctx.attr(TomP2PDecoder.INET_ADDRESS_KEY);
                            inetSocketAddress = pa.get();
                        }
                        if (inetSocketAddress != null) {
                            peerStatusListener.peerFailed(
                                    new PeerAddress(Number160.ZERO, inetSocketAddress.getAddress()), false);
                        } else {
                            LOG.warn("Cannot determine the address!");
                        }
                    }
                }
                
            }
        }
    }
}
