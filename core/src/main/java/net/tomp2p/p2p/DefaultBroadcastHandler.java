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

package net.tomp2p.p2p;

import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.ConcurrentCacheMap;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation for the broadcast. This is a random walk broadcast.
 * You will most likely hit 40-50% of the peers with this.
 * 
 * @author Thomas Bocek
 * 
 */
public class DefaultBroadcastHandler implements BroadcastHandler {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultBroadcastHandler.class);

	private static final Set<Number160> DEBUG_COUNTER = new HashSet<Number160>();

	private static final int NR = 10;

	private static final int MAX_HOP_COUNT = 5;

	private final Random rnd;

	private final ConcurrentCacheMap<Number160, Boolean> cache = new ConcurrentCacheMap<Number160, Boolean>();
	
	private volatile Peer peer;

	/**
	 * Constructor.
	 * 
	 * @param peer
	 *            The peer that sends the broadcast messages
	 * @param rnd
	 *            Random number, since its a random walk
	 */
	public DefaultBroadcastHandler(final Random rnd) {
		this.rnd = rnd;
	}
	
	public DefaultBroadcastHandler init(final Peer peer) {
		this.peer = peer;
		return this;
	}

	/**
	 * Used in JUnit tests only.
	 * 
	 * @return Return the number of peer in the debug set
	 */
	public int getBroadcastCounter() {
		synchronized (DEBUG_COUNTER) {
			return DEBUG_COUNTER.size();
		}
	}

	@Override
	public DefaultBroadcastHandler receive(final Message message) {
		if(peer == null) {
			throw new RuntimeException("Init never called. This should be done by the PeerBuilder");
		}
		final Number160 messageKey = message.key(0);
		final NavigableMap<Number640, Data> dataMap;
		if (message.dataMap(0) != null) {
			dataMap = message.dataMap(0).dataMap();
		} else {
			dataMap = null;
		}
		final int hopCount = message.intAt(0);
		final int bucketNr = message.intAt(1);
		LOG.debug("I {} received a message", peer.peerID());
		if (twiceSeen(messageKey)) {
			LOG.debug("already forwarded this message in {}", peer.peerID());
			return this;
		}
		LOG.debug("got broadcast map {} from {}", dataMap, peer.peerID());

		synchronized (DEBUG_COUNTER) {
			DEBUG_COUNTER.add(peer.peerID());
		}
		if (hopCount < MAX_HOP_COUNT) {
			if (hopCount == 0) {
				LOG.debug("zero hop");
				firstPeer(messageKey, dataMap, hopCount, message.isUdp());
			} else {
				LOG.debug("more hop");
				otherPeer(message.sender().peerId(), messageKey, dataMap, hopCount, message.isUdp(), bucketNr);
			}
		} else {
			LOG.debug("max hop reached in {}", peer.peerID());
		}
		LOG.debug("done");
		return this;
	}

	/**
	 * If a message is seen for the second time, then we don't want to send this
	 * message again. The cache has a size of 1024 entries and the objects have
	 * a default lifetime of 60s.
	 * 
	 * @param messageKey
	 *            The key of the message
	 * @return True if this message was send withing the last 60 seconds.
	 */
	private boolean twiceSeen(final Number160 messageKey) {
		Boolean isInCache = cache.putIfAbsent(messageKey, Boolean.TRUE);
		if (isInCache != null) {
			// ttl refresh
			cache.put(messageKey, Boolean.TRUE);
			return true;
		}
		return false;
	}

	/**
	 * The first peer is the initiator. This peer that wants to start the
	 * broadcast will send it to all its neighbors. Since this peer has an
	 * interest in sending, it should also work more than the other peers.
	 * 
	 * @param messageKey
	 *            The key of the message
	 * @param dataMap
	 *            The data map to send around
	 * @param hopCounter
	 *            The number of hops
	 * @param isUDP
	 *            Flag if message can be sent with UDP
	 */
	public void firstPeer(final Number160 messageKey, final NavigableMap<Number640, Data> dataMap,
	        final int hopCounter, final boolean isUDP) {
		if(peer == null) {
			throw new RuntimeException("Init never called. This should be done by the PeerBuilder");
		}
		final List<PeerAddress> list = peer.peerBean().peerMap().all();
		for (final PeerAddress peerAddress : list) {
			FutureChannelCreator frr = peer.connectionBean().reservation().create(isUDP ? 1 : 0, isUDP ? 0 : 1);
			frr.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
				@Override
				public void operationComplete(final FutureChannelCreator future) throws Exception {
					if (future.isSuccess()) {
						BroadcastBuilder broadcastBuilder = new BroadcastBuilder(peer, messageKey);
						broadcastBuilder.dataMap(dataMap);
						broadcastBuilder.hopCounter(hopCounter + 1);
						broadcastBuilder.udp(isUDP);
						FutureResponse futureResponse = peer.broadcastRPC().send(peerAddress, broadcastBuilder,
						        future.channelCreator(), broadcastBuilder, 0);
						LOG.debug("1st broadcast to {}", peerAddress);
						Utils.addReleaseListener(future.channelCreator(), futureResponse);
					} else {
						Utils.addReleaseListener(future.channelCreator());
					}
				}
			});
		}
	}

	/**
	 * This method is called on relaying peers. We select a random set and we
	 * send the message to those random peers.
	 * 
	 * @param messageKey
	 *            The key of the message
	 * @param dataMap
	 *            The data map to send around
	 * @param hopCounter
	 *            The number of hops
	 * @param isUDP
	 *            Flag if message can be sent with UDP
	 */
	public void otherPeer(Number160 sender, final Number160 messageKey, final NavigableMap<Number640, Data> dataMap,
	        final int hopCounter, final boolean isUDP, final int bucketNr) {
		if(peer == null) {
			throw new RuntimeException("Init never called. This should be done by the PeerBuilder");
		}
		LOG.debug("other");
		final List<PeerAddress> list = peer.peerBean().peerMap().all();
		final int max = Math.min(NR, list.size());
		FutureChannelCreator frr = peer.connectionBean().reservation().create(isUDP ? max : 0, isUDP ? 0 : max);
		frr.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(final FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					FutureResponse[] futures = new FutureResponse[max];
					for (int i = 0; i < max; i++) {
						PeerAddress randomAddress = list.remove(rnd.nextInt(list.size()));

						BroadcastBuilder broadcastBuilder = new BroadcastBuilder(peer, messageKey);
						broadcastBuilder.dataMap(dataMap);
						broadcastBuilder.hopCounter(hopCounter + 1);
						broadcastBuilder.udp(isUDP);

						futures[i] = peer.broadcastRPC().send(randomAddress, broadcastBuilder, future.channelCreator(),
						        broadcastBuilder, 0);
						LOG.debug("2nd broadcast to {} with hop {}", randomAddress, hopCounter + 1);
					}
					Utils.addReleaseListener(future.channelCreator(), futures);
				} else {
					Utils.addReleaseListener(future.channelCreator());
				}
			}
		});
	}
	
	Peer peer() {
	    return peer;
    }
}
