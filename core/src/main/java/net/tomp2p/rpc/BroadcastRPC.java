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

import net.sctp4nat.core.SctpChannelFacade;
import net.tomp2p.connection.ChannelClient;
import net.tomp2p.connection.ChannelSender;
import net.tomp2p.connection.ClientChannel;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.BroadcastHandler;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Triple;

import org.jdeferred.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BroadcastRPC extends DispatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BroadcastRPC.class);

    private final BroadcastHandler broadcastHandler;

    public BroadcastRPC(PeerBean peerBean, ConnectionBean connectionBean, BroadcastHandler broadcastHandler) {
        super(peerBean, connectionBean);
        register(RPC.Commands.BROADCAST.getNr());
        this.broadcastHandler = broadcastHandler;
    }

    public Triple<FutureDone<Message>, FutureDone<SctpChannelFacade>, FutureDone<Void>> send(final PeerAddress remotePeer, final BroadcastBuilder broadcastBuilder,
            final ChannelClient channelCreator, final ConnectionConfiguration configuration, int bucketNr) {
        final Message message = createMessage(remotePeer, RPC.Commands.BROADCAST.getNr(), Type.REQUEST_FF_1);
        message.intValue(broadcastBuilder.hopCounter());
        message.intValue(bucketNr);
        message.key(broadcastBuilder.messageKey());
        
        if (broadcastBuilder.dataMap() != null) {
            message.setDataMap(new DataMap(broadcastBuilder.dataMap()));
        }
        return channelCreator.sendUDP(message);

    }

    @Override
    public void handleResponse(Responder r, final Message message, final boolean sign, Promise<SctpChannelFacade, Exception, Void> p, ChannelSender sender) throws Exception {
        if (!(message.type() == Type.REQUEST_FF_1 && message.command() == RPC.Commands.BROADCAST.getNr())) {
            throw new IllegalArgumentException("Message content is wrong for this handler.");
        }
        LOG.debug("received BRODACAST message: {}", message);
        broadcastHandler.receive(message);
        r.response(null);
    }

    /**
     * @return The broadcast handler that is currently used
     */
    public BroadcastHandler broadcastHandler() {
        return broadcastHandler;
    }
}
