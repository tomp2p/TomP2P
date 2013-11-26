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

import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.TrackerData;
import net.tomp2p.p2p.builder.AddTrackerBuilder;
import net.tomp2p.p2p.builder.GetTrackerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.storage.TrackerStorage.ReferrerType;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackerRPC extends DispatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TrackerRPC.class);

    public static final byte TRACKER_ADD_COMMAND = 8;
    public static final byte TRACKER_GET_COMMAND = 9;

    public static final int MAX_MSG_SIZE_UDP = 35;

    // final private ConnectionConfiguration p2pConfiguration;

    /**
     * @param peerBean
     * @param connectionBean
     */
    public TrackerRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
        super(peerBean, connectionBean, TRACKER_ADD_COMMAND, TRACKER_GET_COMMAND);
    }

    public static boolean isPrimary(FutureResponse response) {
        return response.getRequest().getType() == Type.REQUEST_3;
    }

    public static boolean isSecondary(FutureResponse response) {
        return response.getRequest().getType() == Type.REQUEST_1;
    }

    public FutureResponse addToTracker(final PeerAddress remotePeer, AddTrackerBuilder builder,
            ChannelCreator channelCreator) {

        Utils.nullCheck(remotePeer, builder.getLocationKey(), builder.getDomainKey());
        final Message message = createMessage(remotePeer, TRACKER_ADD_COMMAND,
                builder.isPrimary() ? Type.REQUEST_3 : Type.REQUEST_1);
        if (builder.isMessageSign()) {
            message.setPublicKeyAndSign(peerBean().getKeyPair());
        }
        message.setKey(builder.getLocationKey());
        message.setKey(builder.getDomainKey());
        if (builder.getBloomFilter() != null) {
            message.setBloomFilter(builder.getBloomFilter());
        }
        final FutureResponse futureResponse = new FutureResponse(message);
        final TrackerRequest<FutureResponse> requestHandler = new TrackerRequest<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), message, builder.getLocationKey(),
                builder.getDomainKey(), builder);

        TrackerData trackerData = new TrackerData(new HashMap<PeerAddress, Data>(), null);
        trackerData.put(peerBean().serverPeerAddress(), builder.getAttachement());
        message.setTrackerData(trackerData);

        if (builder.isForceTCP()) {
            return requestHandler.sendTCP(channelCreator);
        } else {
            return requestHandler.sendUDP(channelCreator);
        }
    }

    public FutureResponse getFromTracker(final PeerAddress remotePeer, GetTrackerBuilder builder, 
            ChannelCreator channelCreator) {
        
        //final Number160 locationKey,
        //final Number160 domainKey, boolean expectAttachement, boolean signMessage,
        //Set<Number160> knownPeers,
        
        Utils.nullCheck(remotePeer, builder.getLocationKey(), builder.getDomainKey());
        final Message message = createMessage(remotePeer, TRACKER_GET_COMMAND, Type.REQUEST_1);
        if (builder.isSignMessage()) {
            message.setPublicKeyAndSign(peerBean().getKeyPair());
        }
        message.setKey(builder.getLocationKey());
        message.setKey(builder.getDomainKey());
        //TODO: make this always a bloom filter
        if (builder.getKnownPeers() != null && (builder.getKnownPeers() instanceof SimpleBloomFilter)) {
            message.setBloomFilter((SimpleBloomFilter<Number160>) builder.getKnownPeers());
        }

        FutureResponse futureResponse = new FutureResponse(message);
        final TrackerRequest<FutureResponse> requestHandler = new TrackerRequest<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), message, builder.getLocationKey(), builder.getDomainKey(), builder);

        if ((builder.isExpectAttachement() || builder.isForceTCP())) {
            return requestHandler.sendTCP(channelCreator);
        } else {
            return requestHandler.sendUDP(channelCreator);
        }
    }

    @Override
    public Message handleResponse(Message message, PeerConnection peerConnection, boolean sign) throws Exception {
        if (!((message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_3)
                && message.getKey(0) != null && message.getKey(1) != null)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        final Message responseMessage = createResponseMessage(message, Type.OK);

        // get data
        Number160 locationKey = message.getKey(0);
        Number160 domainKey = message.getKey(1);
        SimpleBloomFilter<Number160> knownPeers = message.getBloomFilter(0);

        PublicKey publicKey = message.getPublicKey();
        //
        final TrackerStorage trackerStorage = peerBean().trackerStorage();

        TrackerData meshPeers = trackerStorage.meshPeers(locationKey, domainKey);

        boolean couldProvideMoreData = false;
        if (meshPeers != null) {
            if (knownPeers != null) {
                meshPeers = Utils.disjunction(meshPeers, knownPeers);
            }
            int size = meshPeers.size();

            meshPeers = Utils.limit(meshPeers, TrackerRPC.MAX_MSG_SIZE_UDP);
            couldProvideMoreData = size > meshPeers.size();
            responseMessage.setTrackerData(meshPeers);
        }

        if (couldProvideMoreData) {
            responseMessage.setType(Message.Type.PARTIALLY_OK);
        }

        if (message.getCommand() == TRACKER_ADD_COMMAND) {
            TrackerData trackerData = message.getTrackerData(0);
            if (trackerData.size() != 1) {
                responseMessage.setType(Message.Type.EXCEPTION);
            } else {
                Map.Entry<PeerAddress, Data> entry = trackerData.getPeerAddresses().entrySet().iterator()
                        .next();
                if (!trackerStorage.put(locationKey, domainKey, entry.getKey(), publicKey, entry.getValue())) {
                    responseMessage.setType(Message.Type.DENIED);
                    LOG.debug("tracker NOT put on({}) locationKey:{}, domainKey:{}, address:{}", peerBean()
                            .serverPeerAddress(), locationKey, domainKey, entry.getKey());
                } else {
                    LOG.debug("tracker put on({}) locationKey:{}, domainKey:{}, address: {} sizeP: {}",
                            peerBean().serverPeerAddress(), locationKey, domainKey, entry.getKey(),
                            trackerStorage.sizePrimary(locationKey, domainKey));
                }
            }

        } else {
            LOG.debug("tracker get on({}) locationKey:{}, domainKey:{}, address:{}, returning: {}",
                    peerBean().serverPeerAddress(), locationKey, domainKey, message.getSender(),
                    (meshPeers == null ? "0" : meshPeers.size()));
        }
        if (sign) {
            responseMessage.setPublicKeyAndSign(peerBean().getKeyPair());
        }
        return responseMessage;
    }

    private class TrackerRequest<K> extends RequestHandler<FutureResponse> {
        private final Message message;

        private final Number160 locationKey;

        private final Number160 domainKey;

        public TrackerRequest(FutureResponse futureResponse, PeerBean peerBean,
                ConnectionBean connectionBean, Message message, Number160 locationKey, Number160 domainKey,
                ConnectionConfiguration configuration) {
            super(futureResponse, peerBean, connectionBean, configuration);
            this.message = message;
            this.locationKey = locationKey;
            this.domainKey = domainKey;
        }

        protected void channelRead0(final ChannelHandlerContext ctx, final Message responseMessage)
                throws Exception {
            preHandleMessage(responseMessage, peerBean().trackerStorage(), this.message.getRecipient(),
                    locationKey, domainKey);
            super.channelRead0(ctx, responseMessage);
        }
    }

    private void preHandleMessage(Message message, TrackerStorage trackerStorage, PeerAddress referrer,
            Number160 locationKey, Number160 domainKey) throws IOException, ClassNotFoundException {
        // Since I might become a tracker as well, we keep this information
        // about those trackers.
        TrackerData tmp = message.getTrackerData(0);
        // no data found
        if (tmp == null || tmp.size() == 0) {
            return;
        }
        for (Map.Entry<PeerAddress, Data> trackerData : tmp.getPeerAddresses().entrySet()) {
            // we don't know the public key, since this is not first hand
            // information.
            // TTL will be set in tracker storage, so don't worry about it here.
            trackerStorage.putReferred(locationKey, domainKey, trackerData.getKey(), referrer,
                    trackerData.getValue(), ReferrerType.MESH);
        }
    }
}