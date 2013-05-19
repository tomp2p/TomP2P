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

import java.util.Map;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.BroadcastHandler;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class BroadcastRPC extends ReplyHandler {
    final private BroadcastHandler broadcastHandler;

    public BroadcastRPC(PeerBean peerBean, ConnectionBean connectionBean, BroadcastHandler broadcastHandler) {
        super(peerBean, connectionBean);
        registerIoHandler(Command.BROADCAST_DATA);
        this.broadcastHandler = broadcastHandler;
    }

    public FutureResponse send(final PeerAddress remotePeer, final Number160 messageKey,
            final Map<Number160, Data> dataMap, final ChannelCreator channelCreator, int idleTCPMillis, boolean forceUDP) {
        return send(remotePeer, messageKey, dataMap, 0, channelCreator, idleTCPMillis, forceUDP);
    }

    public FutureResponse send(final PeerAddress remotePeer, final Number160 messageKey,
            final Map<Number160, Data> dataMap, int hopCounter, final ChannelCreator channelCreator, int idleTCPMillis,
            boolean forceUDP) {
        final Message message = createMessage(remotePeer, Command.BROADCAST_DATA, Type.REQUEST_FF_1);
        message.setDataMap(dataMap);
        message.setInteger(hopCounter);
        message.setKey(messageKey);
        final FutureResponse futureResponse = new FutureResponse(message);
        if (!forceUDP) {
            final RequestHandlerTCP<FutureResponse> requestHandler = new RequestHandlerTCP<FutureResponse>(
                    futureResponse, getPeerBean(), getConnectionBean(), message);
            return requestHandler.fireAndForgetTCP(channelCreator, idleTCPMillis);
        } else {
            final RequestHandlerUDP<FutureResponse> requestHandler = new RequestHandlerUDP<FutureResponse>(
                    futureResponse, getPeerBean(), getConnectionBean(), message);
            return requestHandler.fireAndForgetUDP(channelCreator);
        }
    }

    @Override
    public Message handleResponse(Message message, boolean sign) throws Exception {
        if (!(message.getType() == Type.REQUEST_FF_1 && message.getCommand() == Command.BROADCAST_DATA)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        broadcastHandler.receive(message);
        return message;
    }

    public BroadcastHandler getBroadcastHandler() {
        return broadcastHandler;
    }
}
