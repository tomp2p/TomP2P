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

import java.util.Map;

import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.connection2.ConnectionConfiguration;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.connection2.RequestHandler;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message2;
import net.tomp2p.message.Message2.Type;
import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.TrackerStorage.ReferrerType;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerExchangeRPC extends DispatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PeerExchangeRPC.class);

    public static final byte PEX_COMMAND = 10;
    public static final int SENT_PEERS_CACHE_SIZE = 1000;

    // since PEX is push based, each peer needs to keep track what was sent to
    // whom.
    // private final Map<Number160, Set<PeerAddress>> sentPeers;

    /**
     * Create a PEX handler that sends message using fire and forget.
     * 
     * @param peerBean
     *            The peer bean
     * @param connectionBean
     *            The connection bean
     */
    public PeerExchangeRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
        super(peerBean, connectionBean, PEX_COMMAND);
        // sentPeers = new CacheMap<Number160, Set<PeerAddress>>(SENT_PEERS_CACHE_SIZE, true);
    }

    /**
     * Peer exchange (PEX) information about other peers from the swarm, to not ask the primary trackers too often. This
     * is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to send this request
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param isReplication
     *            Set to true if the PEX is started as replication. This means that this peer learned that an other peer
     *            is closer and sends tracker information to that peer.
     * @param channelCreator
     *            The channel creator that creates connections
     * @param forceTCP
     *            Set to true if the communication should be TCP, default is UDP
     * @return The future response to keep track of future events
     */
    public FutureResponse peerExchange(final PeerAddress remotePeer, final Number160 locationKey,
            final Number160 domainKey, final boolean isReplication, final ChannelCreator channelCreator,
            final ConnectionConfiguration connectionConfiguration) {
        final Message2 message = createMessage(remotePeer, PEX_COMMAND, isReplication ? Type.REQUEST_FF_2
                : Type.REQUEST_FF_1);

        TrackerData peers;
        if (isReplication) {
            peers = peerBean().trackerStorage().meshPeers(locationKey, domainKey);
            LOG.debug("we got stored meshPeers size: {}", peers);
        } else {
            peers = peerBean().trackerStorage().activePeers(locationKey, domainKey);
            LOG.debug("we got stored activePeers size: {}", peers);
        }
        
        if (peers == null) {
            //future is success as we did not do PEX, since its not necessary
            return new FutureResponse(null).setResponse(null);
        }

        peers = Utils.limit(peers, TrackerRPC.MAX_MSG_SIZE_UDP);

        message.setKey(locationKey);
        message.setKey(domainKey);

        if (peers.size() > 0) { // || removed.size() > 0)
            LOG.debug("sent ({}) to {} / {}", message.getSender().getPeerId(), remotePeer.getPeerId(),
                    peers.size());
            message.setTrackerData(peers);
            FutureResponse futureResponse = new FutureResponse(message);
            final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(
                    futureResponse, peerBean(), connectionBean(), connectionConfiguration);
            if (!connectionConfiguration.isForceTCP()) {
                return requestHandler.fireAndForgetUDP(channelCreator);
            } else {
                return requestHandler.sendTCP(channelCreator);
            }
        } else {
            // we have nothing to deliver
            FutureResponse futureResponse = new FutureResponse(message);
            futureResponse.setResponse();
            return futureResponse;
        }
    }

    @Override
    public Message2 handleResponse(final Message2 message, final boolean sign) throws Exception {
        if (!((message.getType() == Type.REQUEST_FF_1 || message.getType() == Type.REQUEST_FF_2) && message
                .getCommand() == PEX_COMMAND)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        Number160 locationKey = message.getKey(0);
        Number160 domainKey = message.getKey(1);
        TrackerData tmp = message.getTrackerData(0);
        if (tmp != null && tmp.size() > 0 && locationKey != null && domainKey != null) {
            final PeerAddress referrer = message.getSender();
            for (Map.Entry<PeerAddress, Data> entry : tmp.getPeerAddresses().entrySet()) {
                PeerAddress trackerEntry = entry.getKey();
                peerBean().trackerStorage().putReferred(locationKey, domainKey, trackerEntry, referrer,
                        entry.getValue(),
                        message.getType() == Type.REQUEST_FF_1 ? ReferrerType.ACTIVE : ReferrerType.MESH);
                LOG.debug("Adding {} to the map. I'm {}", entry.getKey(), message.getRecipient());
            }
            /*
             * if (removedKeys != null) { for (Number160 key : removedKeys) {
             * peerBean().trackerStorage().removeReferred(locationKey, domainKey, key, referrer); } }
             */
        }
        if(message.isUdp()) {
            return message; 
        } else {
            return createResponseMessage(message, Type.OK);
        }
    }
}
