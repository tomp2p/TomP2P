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

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.PeerException;
import net.tomp2p.connection.PeerException.AbortCause;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.KeyCollection;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.peers.PeerStatusListener;

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
     * @param channelCreator
     *            The channel creator that creates connections
     * @param configuration
	 *            The client-side connection configuration
     * @return The future response to keep track of future events
     */
    public FutureResponse closeNeighbors(final PeerAddress remotePeer, final SearchValues searchValues,
            final Type type, final ChannelCreator channelCreator, final ConnectionConfiguration configuration) {
        Message message = createMessage(remotePeer, RPC.Commands.NEIGHBOR.getNr(), type);
        if (!message.isRequest()) {
            throw new IllegalArgumentException("The type must be a request");
        }
        
        message.key(searchValues.locationKey());
        message.key(searchValues.domainKey() == null ? Number160.ZERO : searchValues.domainKey());
        
        if(searchValues.from() !=null && searchValues.to()!=null) {
        	Collection<Number640> collection = new ArrayList<Number640>(2);
        	collection.add(searchValues.from());
        	collection.add(searchValues.to());
        	KeyCollection keyCollection = new KeyCollection(collection);
        	message.keyCollection(keyCollection);
        } else {
	        if (searchValues.contentKey() != null) {
        		message.key(searchValues.contentKey());
        	}
        
        	if (searchValues.keyBloomFilter() != null) {
        		message.bloomFilter(searchValues.keyBloomFilter());
        	}
        	if (searchValues.contentBloomFilter() != null) {
        		message.bloomFilter(searchValues.contentBloomFilter());
        	}
        }
        return send(message, configuration, channelCreator);
    }

    private FutureResponse send(final Message message, final ConnectionConfiguration configuration, final ChannelCreator channelCreator) {
        final FutureResponse futureResponse = new FutureResponse(message);
        futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
            @Override
            public void operationComplete(FutureResponse future) throws Exception {
                if(future.isSuccess()) {
                    Message response = future.responseMessage();
                    if(response != null) {
                        NeighborSet ns = response.neighborsSet(0);
                        if(ns!=null) {
                            for(PeerAddress neighbor:ns.neighbors()) {
                                // Notify, that we found this peer. RTT is from the reporter and therefore only an estimate.
                                peerBean().notifyPeerFound(neighbor, response.sender(), null, futureResponse.getRoundTripTime().setEstimated());
                            }
                        }
                    }
                }
            }
        });
        RequestHandler<FutureResponse> request = new RequestHandler<FutureResponse>(futureResponse,
                peerBean(), connectionBean(), configuration);

        if (!configuration.isForceTCP()) {
            return request.sendUDP(channelCreator);
        } else {
            return request.sendTCP(channelCreator);
        }

    }

    @Override
    public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws IOException {
        if (message.keyList().size() < 2) {
			throw new IllegalArgumentException("At least location and domain keys are needed.");
        }
        if (!(message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_2
                || message.type() == Type.REQUEST_3 || message.type() == Type.REQUEST_4)
                && (message.command() == RPC.Commands.NEIGHBOR.getNr())) {
			throw new IllegalArgumentException("Message content is wrong for this handler.");
        }
        Number160 locationKey = message.key(0);
        Number160 domainKey = message.key(1);
        
        List<PeerAddress> neighbors = getNeighbors(locationKey, NEIGHBOR_SIZE);
        if(neighbors == null) {
            //return empty neighbor set
            Message response = createResponseMessage(message, Type.NOT_FOUND);
            response.neighborsSet(new NeighborSet(-1, Collections.<PeerAddress>emptyList()));
            responder.response(response);
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
        Number160 contentKey = message.key(2);
        SimpleBloomFilter<Number160> keyBloomFilter = message.bloomFilter(0);
        SimpleBloomFilter<Number160> contentBloomFilter = message.bloomFilter(1);
        KeyCollection keyCollection = message.keyCollection(0);
        // it is important to set an integer if a value is present
        boolean isDigest = message.type() != Type.REQUEST_1;
        if (isDigest) {
            if (message.type() == Type.REQUEST_2) {
                final DigestInfo digestInfo;
                if (peerBean().digestStorage() == null) {
                	//no storage to search
                	digestInfo = new DigestInfo();
                }
                else if (contentKey != null && locationKey!=null && domainKey!=null) {
                	Number320 locationAndDomainKey = new Number320(locationKey, domainKey);
                    Number640 from = new Number640(locationAndDomainKey, contentKey, Number160.ZERO);
                    Number640 to = new Number640(locationAndDomainKey, contentKey, Number160.MAX_VALUE);
                    digestInfo = peerBean().digestStorage().digest(from, to, -1, true);
                } else if ((keyBloomFilter != null || contentBloomFilter != null)  && locationKey!=null && domainKey!=null) {
                	Number320 locationAndDomainKey = new Number320(locationKey, domainKey);
                    digestInfo = peerBean().digestStorage().digest(locationAndDomainKey, keyBloomFilter,
                            contentBloomFilter, -1, true, true);
                } else if (keyCollection!=null && keyCollection.keys().size() == 2) {
                	Iterator<Number640> iterator = keyCollection.keys().iterator();
                	Number640 from = iterator.next();
                	Number640 to = iterator.next();
                	digestInfo = peerBean().digestStorage().digest(from, to, -1, true);
                } else if (locationKey!=null && domainKey!=null){
                	Number320 locationAndDomainKey = new Number320(locationKey, domainKey);
                    Number640 from = new Number640(locationAndDomainKey, Number160.ZERO, Number160.ZERO);
                    Number640 to = new Number640(locationAndDomainKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
                    digestInfo = peerBean().digestStorage().digest(from, to, -1, true);
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
        responder.response(responseMessage);
    }

    /**
     * TODO: explain why protected method here.
     */
    protected List<PeerAddress> getNeighbors(Number160 id, int atLeast) {
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
        private final SimpleBloomFilter<Number160> keyBloomFilter;
        private final SimpleBloomFilter<Number160> contentBloomFilter;
        
        private final Number160 locationKey;
        private final Number160 domainKey;
        private final Number160 contentKey;
        
        private final Number640 from;
        private final Number640 to;

        /**
         * Searches for all content keys.
         * 
         * @param locationKey
         *            The location key
         * @param domainKey
         *            The domain key
         */
        public SearchValues(final Number160 locationKey, final Number160 domainKey) {
            this.locationKey = locationKey;
            this.domainKey = domainKey;
            this.keyBloomFilter = null;
            this.contentBloomFilter = null;
            this.contentKey = null;
            this.from = null;
            this.to = null;
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
        public SearchValues(final Number160 locationKey, final Number160 domainKey, final Number160 contentKey) {
            this.locationKey = locationKey;
            this.domainKey = domainKey;
            this.keyBloomFilter = null;
            this.contentBloomFilter = null;
            this.contentKey = contentKey;
            this.from = null;
            this.to = null;
        }
        
        public SearchValues(Number160 locationKey, Number160 domainKey, Number640 from, Number640 to) {
            this.locationKey = locationKey;
            this.domainKey = domainKey;
            this.keyBloomFilter = null;
            this.contentBloomFilter = null;
            this.contentKey = null;
            this.from = from;
            this.to = to;
        }

        /**
         * Searches for multiple content keys. There may be false positives.
         * 
         * @param locationKey
         *            The location key
         * @param domainKey
         *            The domain key
         * @param keyBloomFilter
		 *            For get() and remove() one can provide a bloom filter of content keys and the remote
		 *            peer
         *            indicates if those keys are on that peer.
         */
        public SearchValues(final Number160 locationKey, final Number160 domainKey,
                final SimpleBloomFilter<Number160> keyBloomFilter) {
            this.locationKey = locationKey;
            this.domainKey = domainKey;
            this.keyBloomFilter = keyBloomFilter;
            this.contentBloomFilter = null;
            this.contentKey = null;
            this.from = null;
            this.to = null;
        }

        /**
         * Searches for content key and values with a bloom filter. There may be false positives.
         * 
         * @param locationKey
         *            The location key
         * @param domainKey
         *            The domain key
         * @param keyBloomFilter
		 *            For get() and remove() one can provide a bloom filter of content keys and the remote
		 *            peer
         *            indicates if those keys are on that peer.
         * @param contentBloomFilter
		 *            contentBloomFilter For get() and remove() one can provide a bloom filter of content
		 *            values and
         *            the remote peer indicates if those values are on that peer.
         */
        public SearchValues(final Number160 locationKey, final Number160 domainKey,
                final SimpleBloomFilter<Number160> keyBloomFilter,
                final SimpleBloomFilter<Number160> contentBloomFilter) {
            this.locationKey = locationKey;
            this.domainKey = domainKey;
            this.keyBloomFilter = keyBloomFilter;
            this.contentBloomFilter = contentBloomFilter;
            this.contentKey = null;
            this.from = null;
            this.to = null;
        }

		/**
         * @return The location key
         */
        public Number160 locationKey() {
            return locationKey;
        }

        /**
         * @return The domain key
         */
        public Number160 domainKey() {
            return domainKey;
        }

        /**
         * @return The bloom filter for multiple content keys. May contain false positives.
         */
        public SimpleBloomFilter<Number160> keyBloomFilter() {
            return keyBloomFilter;
        }

        /**
         * @return The bloom filter for multiple content values. May contain false positives.
         */
        public SimpleBloomFilter<Number160> contentBloomFilter() {
            return contentBloomFilter;
        }

        /**
         * @return One content key for fast get
         */
        public Number160 contentKey() {
            return contentKey;
        }
        
        public Number640 from() {
        	return from;
        }
        
        public Number640 to() {
        	return to;
        }
    }
}
