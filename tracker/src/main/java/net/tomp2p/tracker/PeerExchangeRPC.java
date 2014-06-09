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
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerExchangeRPC extends DispatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PeerExchangeRPC.class);

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
    
    private final PeerExchangeHandler peerExchange;
    
    public PeerExchangeRPC(final PeerBean peerBean, final ConnectionBean connectionBean, PeerExchangeHandler peerExchange) {
        super(peerBean, connectionBean);
        register(RPC.Commands.PEX.getNr());
        this.peerExchange = peerExchange;
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
    public FutureResponse peerExchange(final PeerAddress remotePeer, final Number320 key, 
    		final ChannelCreator channelCreator, TrackerData peers,
            final ConnectionConfiguration connectionConfiguration) {
        final Message message = createMessage(remotePeer, RPC.Commands.PEX.getNr(), Type.REQUEST_FF_1);

        //TrackerData peers;
        //peers = trackerStorage.peers(key);
        LOG.debug("we got stored meshPeers size: {}", peers);
        
        if (peers == null || peers.size() == 0) {
            //future is success as we did not do PEX, since its not necessary
            return new FutureResponse(null).response(null);
        }

        peers = UtilsTracker.limit(peers, TrackerRPC.MAX_MSG_SIZE_UDP);

        message.key(key.locationKey());
        message.key(key.domainKey());
        
		LOG.debug("sent ({}) to {} / {}", message.sender().peerId(), remotePeer.peerId(), peers.size());
		message.trackerData(peers);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse,
		        peerBean(), connectionBean(), connectionConfiguration);
		if (!connectionConfiguration.isForceTCP()) {
			return requestHandler.fireAndForgetUDP(channelCreator);
		} else {
			return requestHandler.sendTCP(channelCreator);
		}
    }

    @Override
    public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {
        if (!((message.type() == Type.REQUEST_FF_1) && message
                .command() == RPC.Commands.PEX.getNr())) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        final Number160 locationKey = message.key(0);
        final Number160 domainKey = message.key(1);
        Number320 key = new Number320(locationKey, domainKey);
        final TrackerData tmp = message.trackerData(0);
        peerExchange.put(key, tmp, message.sender());
       
        if(message.isUdp()) {
            responder.responseFireAndForget();
        } else {
        	responder.response(createResponseMessage(message, Type.OK));
        }
    }
}
