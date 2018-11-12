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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

import net.tomp2p.connection.ChannelSender;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerException;
import net.tomp2p.connection.PeerException.AbortCause;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.KeyCollection;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.network.KCP;
import net.tomp2p.peers.*;
import net.tomp2p.peers.Number256;
import net.tomp2p.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the neighbor requests and replies.
 * 
 * @author Thomas Bocek
 * 
 */

public class NeighborRPC extends DispatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(NeighborRPC.class);

    public static final int NEIGHBOR_SIZE = 30;
    public static final int NEIGHBOR_LIMIT = 1000;
    
    public NeighborRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
        this(peerBean, connectionBean, true);
    }

    /**
     * Setup the RPC and register for incoming messages.
     * 
     * @param peerBean
     *            The peer bean
     * @param connectionBean
     *            The connection bean
	 * @param register
	 *            Whether incoming messages should be registered
     */
    public NeighborRPC(final PeerBean peerBean, final ConnectionBean connectionBean, boolean register) {
        super(peerBean, connectionBean);
        if(register) {
            register(RPC.Commands.NEIGHBOR.getNr());
        }
    }

    /**
	 * Requests close neighbors from the remote peer. The remote peer may indicate if the data is present on
	 * that peer.
     * This is an RPC.
     * 
     * @param remotePeer
	 *            The remote peer to send this request to
     * @param searchValues
     *            The values to search for in the storage
     * 
     * @param type
     *            The type of the neighbor request:
     *            <ul>
     *            <li>REQUEST_1 for NEIGHBORS means check for put (no digest) for tracker and storage</li>
     *            <li>REQUEST_2 for NEIGHBORS means check for get (with digest) for storage</li>
     *            <li>REQUEST_3 for NEIGHBORS means check for get (with digest) for tracker</li>
     *            <li>REQUEST_4 for NEIGHBORS means check for put (with digest) for task</li>
     *            </ul>
     * @param configuration
	 *            The client-side connection configuration
     * @return The future response to keep track of future events
     */
    public Pair<FutureDone<Message>, KCP> closeNeighbors(final PeerAddress remotePeer, final SearchValues searchValues,
            final Type type, final ConnectionConfiguration configuration) {
        Message message = createMessage(remotePeer, RPC.Commands.NEIGHBOR.getNr(), type);
        if (!message.isRequest()) {
            throw new IllegalArgumentException("The type must be a request");
        }
        
        /*message.key(searchValues.locationKey());
        message.key(searchValues.domainKey() == null ? Number256.ZERO : searchValues.domainKey());
        
        /*if(searchValues.from() !=null && searchValues.to()!=null) {
        	Collection<Object> collection = new ArrayList<Object>(2);
        	//collection.add(searchValues.from());
        	//collection.add(searchValues.to());
        	//KeyCollection keyCollection = new KeyCollection(collection);
        	//message.keyCollection(keyCollection);
        } else*/ /*{
	        if (searchValues.contentKey() != null) {
        		message.key(searchValues.contentKey());
        	}
        
        	if (searchValues.keyBloomFilter() != null) {
        		message.bloomFilter(searchValues.keyBloomFilter());
        	}
        	if (searchValues.contentBloomFilter() != null) {
        		message.bloomFilter(searchValues.contentBloomFilter());
        	}
        }*/
        LOG.debug("Ask remote peer for neighbors with msg {}", message);
        return send(message, configuration);
    }

    private Pair<FutureDone<Message>, KCP> send(final Message message, final ConnectionConfiguration configuration) {
        final FutureResponse futureResponse = new FutureResponse(message);
        futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
            @Override
            public void operationComplete(FutureResponse future) throws Exception {
                if(future.isSuccess()) {
                    Message response = future.responseMessage();
                    if(response != null) {
                        /*NeighborSet ns = response.neighborsSet(0);
                        if(ns!=null) {
                            for(PeerAddress neighbor:ns.neighbors()) {
                                // Notify, that we found this peer. RTT is from the reporter and therefore only an estimate.
                                peerBean().notifyPeerFound(neighbor, response.sender(), futureResponse.getRoundTripTime().setEstimated());
                            }
                        }*/
                    }
                }
            }
        });
        
        return connectionBean().channelServer().sendUDP(message);
    }

    @Override
    public void handleResponse(Responder r, final Message message, final boolean sign, KCP kcp, ChannelSender sender) throws IOException {
        /*if (message.keyList().size() < 2) {
			throw new IllegalArgumentException("At least location and domain keys are needed.");
        }
        if (!(message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_2
                || message.type() == Type.REQUEST_3 || message.type() == Type.REQUEST_4)
                && (message.command() == RPC.Commands.NEIGHBOR.getNr())) {
			throw new IllegalArgumentException("Message content is wrong for this handler.");
        }
        Number256 locationKey = message.key(0);
        Number256 domainKey = message.key(1);
        
        List<PeerAddress> neighbors = getNeighbors(locationKey, NEIGHBOR_SIZE);
        if(neighbors == null) {
            //return empty neighbor set
            Message response = createResponseMessage(message, Type.NOT_FOUND);
            response.neighborsSet(new NeighborSet(-1, Collections.<PeerAddress>emptyList()));
            r.response(response);
            return;
        }
        
        // Create response message and set neighbors
        final Message responseMessage = createResponseMessage(message, Type.OK);
        
		LOG.debug("Found the following neighbors: {}.", neighbors);
        NeighborSet neighborSet = new NeighborSet(NEIGHBOR_LIMIT, neighbors);
        responseMessage.neighborsSet(neighborSet);
		// check for fast get:
		// -1 if no domain provided, so we cannot check content length
		// 0 for content not here
		// >0 content here
        // int contentLength = -1;
        Number256 contentKey = message.key(2);
        SimpleBloomFilter<Number256> keyBloomFilter = message.bloomFilter(0);
        SimpleBloomFilter<Number256> contentBloomFilter = message.bloomFilter(1);
        KeyCollection keyCollection = message.keyCollection(0);
        // it is important to set an integer if a value is present
        boolean isDigest = message.type() != Type.REQUEST_1;
        if (isDigest) {
            if (message.type() == Type.REQUEST_2) {
                final DigestInfo digestInfo;
                if (peerBean().digestStorage() == null) {
                	//no storage to search
                	digestInfo = new DigestInfo();
                } else {
					LOG.warn("Did not search for anything.");
                	digestInfo = new DigestInfo();
                }
                responseMessage.intValue(digestInfo.size());
                responseMessage.key(digestInfo.keyDigest());
                responseMessage.key(digestInfo.contentDigest());
            } else if (message.type() == Type.REQUEST_3) {
            	final DigestInfo digestInfo;
            	if(peerBean().digestTracker() == null) {
            		//no tracker to search
            		digestInfo = new DigestInfo();
            	} else {
            		digestInfo = peerBean().digestTracker().digest(locationKey, domainKey, contentKey);
            		if (digestInfo.size() == 0) {	
						LOG.debug("No entry found on peer {}.", message.recipient());
            		}
            	}
                responseMessage.intValue(digestInfo.size());
            } 
            else if (message.type() == Type.REQUEST_4) {
            	synchronized (peerBean().peerStatusListeners()) {
            		for (PeerStatusListener peerStatusListener : peerBean().peerStatusListeners()) {
    					peerStatusListener.peerFailed(message.sender(), new PeerException(AbortCause.SHUTDOWN, "shutdown"));
    				}
            	}
            }
              
        }
        r.response(responseMessage);*/
    }

    /**
     * TODO: explain why protected method here.
     */
    protected List<PeerAddress> getNeighbors(Number256 id, int atLeast) {
        NavigableSet<PeerStatistic> closePeers = peerBean().peerMap().closePeers(id, atLeast);

        ArrayList<PeerAddress> result = new ArrayList<PeerAddress>();
        for (PeerStatistic ps : closePeers) {
            result.add(ps.peerAddress());

        }
        return result;
    }

    /**
	 * The search values for fast get. You can either provide one content key. If you want to check for
	 * multiple keys,
     * use the content key bloom filter. You can also check for values with a bloom filter.
     * 
     * @author Thomas Bocek
     * 
     */

    public static class SearchValues {
        private final SimpleBloomFilter<Number256> keyBloomFilter;
        private final SimpleBloomFilter<Number256> contentBloomFilter;
        
        private final Number256 locationKey;
        private final Number256 domainKey;
        private final Number256 contentKey;

        /**
         * Searches for all content keys.
         * 
         * @param locationKey
         *            The location key
         * @param domainKey
         *            The domain key
         */
        public SearchValues(final Number256 locationKey, final Number256 domainKey) {
            this.locationKey = locationKey;
            this.domainKey = domainKey;
            this.keyBloomFilter = null;
            this.contentBloomFilter = null;
            this.contentKey = null;

        }

        /**
         * Searches for one content key.
         * 
         * @param locationKey
         *            The location key
         * @param domainKey
         *            The domain key
         * @param contentKey
		 *            For get() and remove() one can provide a content key and the remote peer indicates if
		 *            this key
         *            is on that peer.
         */
        public SearchValues(final Number256 locationKey, final Number256 domainKey, final Number256 contentKey) {
            this.locationKey = locationKey;
            this.domainKey = domainKey;
            this.keyBloomFilter = null;
            this.contentBloomFilter = null;
            this.contentKey = contentKey;

        }
        
        /**
         * Searches for multiple content keys. There may be false positives.
         *
         *
         * @return The location key
         */
        public Number256 locationKey() {
            return locationKey;
        }

        /**
         * @return The domain key
         */
        public Number256 domainKey() {
            return domainKey;
        }

        /**
         * @return The bloom filter for multiple content keys. May contain false positives.
         */
        public SimpleBloomFilter<Number256> keyBloomFilter() {
            return keyBloomFilter;
        }

        /**
         * @return The bloom filter for multiple content values. May contain false positives.
         */
        public SimpleBloomFilter<Number256> contentBloomFilter() {
            return contentBloomFilter;
        }

        /**
         * @return One content key for fast get
         */
        public Number256 contentKey() {
            return contentKey;
        }

    }
}
