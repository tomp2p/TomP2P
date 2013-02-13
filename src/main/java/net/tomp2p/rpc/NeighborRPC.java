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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Content;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.utils.Utils;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NeighborRPC extends ReplyHandler {
    final private static Logger logger = LoggerFactory.getLogger(NeighborRPC.class);

    final public static int NEIGHBOR_SIZE = 20;

    public NeighborRPC(PeerBean peerBean, ConnectionBean connectionBean) {
        super(peerBean, connectionBean);
        registerIoHandler(Command.NEIGHBORS);
    }

    /**
     * Requests close neighbors from the remote peer. The remote peer may
     * idicate if the data is present on that peer. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to send this request
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param contentKeys
     *            For get() and remove() one can provide the content keys and
     *            the remote peer indicates if those keys are on that peer.
     * @param type
     *            The type of the neigbor request:
     *            <ul>
     *            <li>REQUEST_1 for NEIGHBORS means check for put (no digest)
     *            for tracker and storage</li>
     *            <li>REQUEST_2 for NEIGHBORS means check for get (with digest)
     *            for storage</li>
     *            <li>REQUEST_3 for NEIGHBORS means check for get (with digest)
     *            for tracker</li>
     *            <li>REQUEST_4 for NEIGHBORS means check for put (with digest)
     *            for task</li>
     *            </ul>
     * @param isDigest
     *            Set to true to return a digest of the remote content
     * @param channelCreator
     *            The channel creator that creates connections
     * @param forceTCP
     *            Set to true if the communication should be TCP, default is UDP
     * @return The future response to keep track of future events
     */
    public FutureResponse closeNeighbors(PeerAddress remotePeer, Number160 locationKey, Number160 domainKey,
            Collection<Number160> contentKeys, Type type, ChannelCreator channelCreator, boolean forceTCP) {
        Utils.nullCheck(remotePeer, locationKey);
        Message message = createMessage(remotePeer, Command.NEIGHBORS, type);
        if (!message.isRequest()) {
            throw new IllegalArgumentException("The type must be a request");
        }
        message.setKeyKey(locationKey, domainKey == null ? Number160.ZERO : domainKey);
        if (contentKeys != null)
            message.setKeys(contentKeys);
        if (!forceTCP) {
            FutureResponse futureResponse = new FutureResponse(message);
            NeighborsRequestUDP<FutureResponse> request = new NeighborsRequestUDP<FutureResponse>(futureResponse,
                    getPeerBean(), getConnectionBean(), message);
            return request.sendUDP(channelCreator);
        } else {
            FutureResponse futureResponse = new FutureResponse(message);
            NeighborsRequestTCP<FutureResponse> request = new NeighborsRequestTCP<FutureResponse>(futureResponse,
                    getPeerBean(), getConnectionBean(), message);
            return request.sendTCP(channelCreator);
        }
    }

    @Override
    public Message handleResponse(Message message, boolean sign) throws IOException {
        Utils.nullCheck(message.getKeyKey1(), message.getKeyKey2());
        if (!(message.getContentType1() == Content.KEY_KEY
                && (message.getContentType2() == Content.EMPTY || message.getContentType2() == Content.SET_KEY160)
                && (message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2
                        || message.getType() == Type.REQUEST_3 || message.getType() == Type.REQUEST_4) && (message
                    .getCommand() == Command.NEIGHBORS))) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        Number160 locationKey = message.getKeyKey1();
        Number160 domainKey = message.getKeyKey2();
        // Create response message and set neighbors
        final Message responseMessage = createResponseMessage(message, Type.OK);
        if (sign) {
            responseMessage.setPublicKeyAndSign(getPeerBean().getKeyPair());
        }
        Collection<PeerAddress> neighbors = getPeerBean().getPeerMap().closePeers(locationKey, NEIGHBOR_SIZE);
        responseMessage.setNeighbors(neighbors, NEIGHBOR_SIZE);
        // check for fastget, -1 if, no domain provided, so we cannot
        // check content length, 0 for content not here , > 0 content here
        // int contentLength = -1;
        Collection<Number160> contentKeys = message.getKeys();
        // it is important to set an integer if a value is present
        boolean isDigest = message.getType() != Type.REQUEST_1;
        if (isDigest) {
            if (message.getType() == Type.REQUEST_2) {
                DigestInfo digestInfo = getPeerBean().getStorage().digest(locationKey, domainKey, contentKeys);
                responseMessage.setInteger(digestInfo.getSize());
                responseMessage.setKeyKey(digestInfo.getKeyDigest(), digestInfo.getContentDigest());
            } else if (message.getType() == Type.REQUEST_3) {
                DigestInfo digestInfo = getPeerBean().getTrackerStorage().digest(locationKey, domainKey, null);
                if (logger.isDebugEnabled() && digestInfo.getSize() == 0) {
                    logger.debug("No entry found on peer " + message.getRecipient());
                }
                responseMessage.setInteger(digestInfo.getSize());
            } else if (message.getType() == Type.REQUEST_4) {
                DigestInfo digestInfo = getPeerBean().getTaskManager().digest();
                responseMessage.setInteger(digestInfo.getSize());
            }
        }
        return responseMessage;
    }

    private void preHandleMessage(Message message, PeerMap peerMap, PeerAddress referrer) {
        if (!(message.getType() == Type.OK && message.getCommand() == Command.NEIGHBORS)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Message not of type Neighbor, ignoring " + message);
            }
            return;
        }
        Collection<PeerAddress> tmp = message.getNeighbors();
        if (tmp != null) {
            Iterator<PeerAddress> iterator = tmp.iterator();
            while (iterator.hasNext()) {
                PeerAddress addr = iterator.next();
                // if peer is removed due to failure, don't consider
                // that peer for routing anymore
                if (peerMap.isPeerRemovedTemporarly(addr)) {
                    iterator.remove();
                }
                // otherwise try to add it to the map
                else {
                    peerMap.peerFound(addr, referrer);
                }
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Neighbor message received, but does not contain any neighbors.");
            }
        }

    }

    private class NeighborsRequestTCP<K extends FutureResponse> extends RequestHandlerTCP<K> {
        final private Message message;

        public NeighborsRequestTCP(K futureResponse, PeerBean peerBean, ConnectionBean connectionBean, Message message) {
            super(futureResponse, peerBean, connectionBean, message);
            this.message = message;
        }

        @Override
        public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent ce) throws Exception {
            preHandleMessage(message, getPeerMap(), this.message.getRecipient());
            super.handleUpstream(ctx, ce);
        }
    }

    private class NeighborsRequestUDP<K extends FutureResponse> extends RequestHandlerUDP<K> {
        final private Message message;

        public NeighborsRequestUDP(K futureResponse, PeerBean peerBean, ConnectionBean connectionBean, Message message) {
            super(futureResponse, peerBean, connectionBean, message);
            this.message = message;
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            Object object = e.getMessage();
            if (object instanceof Message)
                preHandleMessage((Message) object, getPeerMap(), this.message.getRecipient());
            else
                logger.error("Response received, but not a message: " + object);
            super.messageReceived(ctx, e);
        }
    }
}