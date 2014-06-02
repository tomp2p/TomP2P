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
package net.tomp2p.tracker;

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
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.storage.TrackerStorage.ReferrerType;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackerRPC extends DispatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TrackerRPC.class);

    public static final int MAX_MSG_SIZE_UDP = 35;

    // final private ConnectionConfiguration p2pConfiguration;

    /**
     * @param peerBean
     * @param connectionBean
     */
    public TrackerRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
        super(peerBean, connectionBean);
        register(RPC.Commands.TRACKER_ADD.getNr(), RPC.Commands.TRACKER_GET.getNr());
    }

    public static boolean isPrimary(FutureResponse response) {
        return response.request().type() == Type.REQUEST_3;
    }

    public static boolean isSecondary(FutureResponse response) {
        return response.request().type() == Type.REQUEST_1;
    }

    public FutureResponse addToTracker(final PeerAddress remotePeer, AddTrackerBuilder builder,
            ChannelCreator channelCreator) {

        Utils.nullCheck(remotePeer, builder.getLocationKey(), builder.getDomainKey());
        final Message message = createMessage(remotePeer, RPC.Commands.TRACKER_ADD.getNr(),
                builder.isPrimary() ? Type.REQUEST_3 : Type.REQUEST_1);
        if (builder.isSign()) {
            message.publicKeyAndSign(builder.keyPair());
        }
        message.key(builder.getLocationKey());
        message.key(builder.getDomainKey());
        if (builder.getBloomFilter() != null) {
            message.bloomFilter(builder.getBloomFilter());
        }
        final FutureResponse futureResponse = new FutureResponse(message);
        final TrackerRequest<FutureResponse> requestHandler = new TrackerRequest<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), message, builder.getLocationKey(),
                builder.getDomainKey(), builder);

        TrackerData trackerData = new TrackerData(new HashMap<PeerAddress, Data>(), null);
        PeerAddress peerAddressToAnnounce = builder.peerAddressToAnnounce();
        if(peerAddressToAnnounce == null) {
        	peerAddressToAnnounce = peerBean().serverPeerAddress();
        }
        trackerData.put(peerAddressToAnnounce, builder.getAttachement());
        message.trackerData(trackerData);

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
        final Message message = createMessage(remotePeer, RPC.Commands.TRACKER_GET.getNr(), Type.REQUEST_1);
        if (builder.isSign()) {
            message.publicKeyAndSign(builder.keyPair());
        }
        message.key(builder.getLocationKey());
        message.key(builder.getDomainKey());
        //TODO: make this always a bloom filter
        if (builder.getKnownPeers() != null && (builder.getKnownPeers() instanceof SimpleBloomFilter)) {
            message.bloomFilter((SimpleBloomFilter<Number160>) builder.getKnownPeers());
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
    public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder) throws Exception {
        if (!((message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_3)
                && message.key(0) != null && message.key(1) != null)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        final Message responseMessage = createResponseMessage(message, Type.OK);

        // get data
        Number160 locationKey = message.key(0);
        Number160 domainKey = message.key(1);
        SimpleBloomFilter<Number160> knownPeers = message.bloomFilter(0);

        PublicKey publicKey = message.publicKey(0);
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
            responseMessage.trackerData(meshPeers);
        }

        if (couldProvideMoreData) {
            responseMessage.type(Message.Type.PARTIALLY_OK);
        }

        if (message.command() == RPC.Commands.TRACKER_ADD.getNr()) {
            TrackerData trackerData = message.trackerData(0);
            if (trackerData.size() != 1) {
                responseMessage.type(Message.Type.EXCEPTION);
            } else {
                Map.Entry<PeerAddress, Data> entry = trackerData.peerAddresses().entrySet().iterator()
                        .next();
                if (!trackerStorage.put(locationKey, domainKey, entry.getKey(), publicKey, entry.getValue())) {
                    responseMessage.type(Message.Type.DENIED);
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
                    peerBean().serverPeerAddress(), locationKey, domainKey, message.sender(),
                    (meshPeers == null ? "0" : meshPeers.size()));
        }
        if (sign) {
            responseMessage.publicKeyAndSign(peerBean().getKeyPair());
        }
        responder.response(responseMessage);
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
            preHandleMessage(responseMessage, peerBean().trackerStorage(), this.message.recipient(),
                    locationKey, domainKey);
            super.channelRead0(ctx, responseMessage);
        }
    }

    private void preHandleMessage(Message message, TrackerStorage trackerStorage, PeerAddress referrer,
            Number160 locationKey, Number160 domainKey) throws IOException, ClassNotFoundException {
        // Since I might become a tracker as well, we keep this information
        // about those trackers.
        TrackerData tmp = message.trackerData(0);
        // no data found
        if (tmp == null || tmp.size() == 0) {
            return;
        }
        for (Map.Entry<PeerAddress, Data> trackerData : tmp.peerAddresses().entrySet()) {
            // we don't know the public key, since this is not first hand
            // information.
            // TTL will be set in tracker storage, so don't worry about it here.
            trackerStorage.putReferred(locationKey, domainKey, trackerData.getKey(), referrer,
                    trackerData.getValue(), ReferrerType.MESH);
        }
    }
}
