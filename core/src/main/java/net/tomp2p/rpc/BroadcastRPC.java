/*
 * Copyright 2012 Thomas Bocek
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

import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.connection2.ConnectionConfiguration;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.connection2.RequestHandler;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message2;
import net.tomp2p.message.Message2.Type;
import net.tomp2p.p2p.BroadcastHandler;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.peers.PeerAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BroadcastRPC extends DispatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BroadcastRPC.class);

    public static final byte BROADCAST_COMMAND = 12;

    private final BroadcastHandler broadcastHandler;

    public BroadcastRPC(PeerBean peerBean, ConnectionBean connectionBean, BroadcastHandler broadcastHandler) {
        super(peerBean, connectionBean, BROADCAST_COMMAND);
        this.broadcastHandler = broadcastHandler;
    }

    public FutureResponse send(final PeerAddress remotePeer, final BroadcastBuilder broadcastBuilder,
            final ChannelCreator channelCreator, final ConnectionConfiguration configuration) {
        final Message2 message = createMessage(remotePeer, BROADCAST_COMMAND, Type.REQUEST_FF_1);
        message.setInteger(broadcastBuilder.hopCounter());
        message.setKey(broadcastBuilder.messageKey());
        if (broadcastBuilder.dataMap() != null) {
            message.setDataMap(new DataMap(broadcastBuilder.dataMap()));
        }
        final FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), configuration);
        if (!broadcastBuilder.isUDP()) {
            return requestHandler.sendTCP(channelCreator);
        } else {
            return requestHandler.fireAndForgetUDP(channelCreator);
        }
    }

    @Override
    public Message2 handleResponse(final Message2 message, final boolean sign) throws Exception {
        if (!(message.getType() == Type.REQUEST_FF_1 && message.getCommand() == BROADCAST_COMMAND)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        LOG.debug("received BRODACAST message: {}", message);
        broadcastHandler.receive(message);
        if(message.isUdp()) {
            return message;
        } else {
            return createResponseMessage(message, Type.OK);
        }
    }

    /**
     * @return The broadcast handler that is currently used
     */
    public BroadcastHandler broadcastHandler() {
        return broadcastHandler;
    }
}
