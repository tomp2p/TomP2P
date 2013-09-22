/*
 * Copyright 2009 Thomas Bocek
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
package net.tomp2p.rpc;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.connection2.ConnectionConfiguration;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.connection2.RequestHandler;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message2;
import net.tomp2p.message.Message2.Type;
import net.tomp2p.p2p.builder.ShutdownBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;

/**
 * This Quit RPC is used to send friendly shutdown messages by peers that are shutdown regularly.
 * 
 * @author Thomas Bocek
 * 
 */
public class QuitRPC extends DispatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(QuitRPC.class);

    public static final byte QUIT_COMMAND = 6;

    private final List<PeerStatusListener> listeners = new ArrayList<PeerStatusListener>();

    /**
     * Constructor that registers this RPC with the message handler.
     * 
     * @param peerBean
     *            The peer bean that contains data that is unique for each peer
     * @param connectionBean
     *            The connection bean that is unique per connection (multiple peers can share a single connection)
     */
    public QuitRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
        super(peerBean, connectionBean, QUIT_COMMAND);
    }

    /**
     * Add a peer status listener that gets notified when a peer is offline.
     * 
     * @param listener
     *            The listener
     * @return This class
     */
    public QuitRPC addPeerStatusListener(final PeerStatusListener listener) {
        listeners.add(listener);
        return this;
    }

    /**
     * Sends a message that indicates this peer is about to quit. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to send this request
     * @param shutdownBuilder
     *            Used for the sign and force TCP flag Set if the message should be signed
     * @param channelCreator
     *            The channel creator that creates connections
     * @param configuration
     *            The client side connection configuration
     * @return The future response to keep track of future events
     */
    public FutureResponse quit(final PeerAddress remotePeer, final ShutdownBuilder shutdownBuilder,
            final ChannelCreator channelCreator) {
        final Message2 message = createMessage(remotePeer, QUIT_COMMAND, Type.REQUEST_FF_1);
        if (shutdownBuilder.isSignMessage()) {
            message.setPublicKeyAndSign(peerBean().getKeyPair());
        }

        FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), shutdownBuilder);
        LOG.debug("send QUIT message {}" + message);
        if (!shutdownBuilder.isForceTCP()) {
            return requestHandler.fireAndForgetUDP(channelCreator);
        } else {
            return requestHandler.sendTCP(channelCreator);
        }
    }

    @Override
    public Message2 handleResponse(final Message2 message, final boolean sign) throws Exception {
        if (!(message.getType() == Type.REQUEST_FF_1 && message.getCommand() == QUIT_COMMAND)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        LOG.debug("received QUIT message {}" + message);
        synchronized (listeners) {
            for (PeerStatusListener listener : listeners) {
                listener.peerFailed(message.getSender(), true);
            }
        }
        if(message.isUdp()) {
            return message;
        } else {
            return createResponseMessage(message, Type.OK);
        }
    }
}
