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

import io.netty.buffer.Unpooled;

import java.io.IOException;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Dispatcher.Responder;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectDataRPC extends DispatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DirectDataRPC.class);

    private volatile RawDataReply rawDataReply;

    private volatile ObjectDataReply objectDataReply;

    public DirectDataRPC(PeerBean peerBean, ConnectionBean connectionBean) {
        super(peerBean, connectionBean, RPC.Commands.DIRECT_DATA.getNr());
    }

    /**
     * Send data directly to a peer. Make sure you have set up a reply handler. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to store the data
     * @param buffer
     *            The data to send to the remote peer
     * @param raw
     *            Set to true if a the byte array is expected or if it should be converted to an object
     * @param channelCreator
     *            The channel creator
     * @param idleTCPMillis
     *            Set the timeout when a connection is considered inactive (idle)
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */

    public RequestHandler<FutureResponse> sendInternal(final PeerAddress remotePeer,
            final SendDirectBuilderI sendDirectBuilder) {
        final Message message = createMessage(remotePeer, RPC.Commands.DIRECT_DATA.getNr(),
                sendDirectBuilder.isRaw() ? Type.REQUEST_1 : Type.REQUEST_2);
        final FutureResponse futureResponse = new FutureResponse(message,
                sendDirectBuilder.progressListener());

        if (sendDirectBuilder.isSign()) {
            message.setPublicKeyAndSign(sendDirectBuilder.keyPair());
        }
        message.streaming(sendDirectBuilder.streaming());

        if (sendDirectBuilder.isRaw()) {
            message.setBuffer(sendDirectBuilder.getBuffer());
        } else {
            byte[] me;
            try {
                me = Utils.encodeJavaObject(sendDirectBuilder.getObject());
                message.setBuffer(new Buffer(Unpooled.wrappedBuffer(me)));
            } catch (IOException e) {
                futureResponse.setFailed("cannot convert object", e);
            }       
        }

        return new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(),
                sendDirectBuilder);
    }

    public FutureResponse send(final PeerAddress remotePeer, final SendDirectBuilderI sendDirectBuilder,
            final ChannelCreator channelCreator) {
        final RequestHandler<FutureResponse> requestHandler = sendInternal(remotePeer, sendDirectBuilder);
        if (!sendDirectBuilder.isForceUDP()) {
            return requestHandler.sendTCP(channelCreator);
        } else {
            return requestHandler.sendUDP(channelCreator);
        }
    }

    public void setReply(final RawDataReply rawDataReply) {
        this.rawDataReply = rawDataReply;
    }

    public void setReply(ObjectDataReply objectDataReply) {
        this.objectDataReply = objectDataReply;
    }

    public boolean hasRawDataReply() {
        return rawDataReply != null;
    }

    public boolean hasObjectDataReply() {
        return objectDataReply != null;
    }

    @Override
    public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {
        if (!((message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2) && message
                .getCommand() == RPC.Commands.DIRECT_DATA.getNr())) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        final Message responseMessage = createResponseMessage(message, Type.OK);

        if (sign) {
            responseMessage.setPublicKeyAndSign(peerBean().getKeyPair());
        }
        final RawDataReply rawDataReply2 = rawDataReply;
        final ObjectDataReply objectDataReply2 = objectDataReply;
        if (message.getType() == Type.REQUEST_1 && rawDataReply2 == null) {
            responseMessage.setType(Type.NOT_FOUND);
        } else if (message.getType() == Type.REQUEST_2 && objectDataReply2 == null) {
            responseMessage.setType(Type.NOT_FOUND);
        } else {
            final Buffer requestBuffer = message.getBuffer(0);
            // the user can reply with null, indicating not found. Or
            // returning the request buffer, which means nothing is
            // returned. Or an exception can be thrown
            if (message.getType() == Type.REQUEST_1) {
                LOG.debug("handling requet1");
                final Buffer replyBuffer = rawDataReply2.reply(message.getSender(), requestBuffer,
                        message.isDone());
                if (replyBuffer == null && message.isDone()) {
                    responseMessage.setType(Type.NOT_FOUND);
                } else if (replyBuffer != requestBuffer) {
                    // can be partial as well
                    if (!replyBuffer.isComplete()) {
                        responseMessage.setStreaming();
                    }
                    responseMessage.setBuffer(replyBuffer);
                }
            } else { // no streaming here when we deal with objects
                Object obj = Utils.decodeJavaObject(requestBuffer.buffer());
                LOG.debug("handling {}", obj);

                Object reply = objectDataReply2.reply(message.getSender(), obj);
                if (reply == null) {
                    responseMessage.setType(Type.NOT_FOUND);
                } else if (reply == obj) {
                    responseMessage.setType(Type.OK);
                } else {
                    byte[] me = Utils.encodeJavaObject(reply);
                    responseMessage.setBuffer(new Buffer(Unpooled.wrappedBuffer(me)));
                }
            }
        }
        responder.response(responseMessage);
    }
}
