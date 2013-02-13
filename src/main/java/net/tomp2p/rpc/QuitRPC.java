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

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;

public class QuitRPC extends ReplyHandler {
    public QuitRPC(PeerBean peerBean, ConnectionBean connectionBean) {
        super(peerBean, connectionBean);
        registerIoHandler(Command.QUIT);
    }

    /**
     * Sends a message that indicates this peer is about to quit. This is an
     * RPC.
     * 
     * @param remotePeer
     *            The remote peer to send this request
     * @param channelCreator
     *            The channel creator that creates connections
     * @param forceTCP
     *            Set to true if the communication should be TCP, default is UDP
     * @return The future response to keep track of future events
     */
    public FutureResponse quit(final PeerAddress remotePeer, ChannelCreator channelCreator, boolean forceTCP) {
        final Message message = createMessage(remotePeer, Command.QUIT, Type.REQUEST_FF_1);
        FutureResponse futureResponse = new FutureResponse(message);
        if (!forceTCP) {
            final RequestHandlerUDP<FutureResponse> requestHandler = new RequestHandlerUDP<FutureResponse>(
                    futureResponse, getPeerBean(), getConnectionBean(), message);
            return requestHandler.fireAndForgetUDP(channelCreator);
        } else {
            final RequestHandlerTCP<FutureResponse> requestHandler = new RequestHandlerTCP<FutureResponse>(
                    futureResponse, getPeerBean(), getConnectionBean(), message);
            return requestHandler.fireAndForgetTCP(channelCreator);
        }
    }

    @Override
    public Message handleResponse(final Message message, boolean sign) throws Exception {
        if (!(message.getType() == Type.REQUEST_FF_1 && message.getCommand() == Command.QUIT)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        getPeerBean().getPeerMap().peerOffline(message.getSender(), true);
        return message;
    }
}