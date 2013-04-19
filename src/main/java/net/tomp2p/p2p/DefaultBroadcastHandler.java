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
import java.util.Map;
import java.util.Random;
import java.util.Set;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.ConcurrentCacheMap;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultBroadcastHandler implements BroadcastHandler {
	final private static Logger logger = LoggerFactory
			.getLogger(DefaultBroadcastHandler.class);

	final private Peer peer;

	final private Random rnd;

	final private static int NR = 10;

	final private static int MAX_HOP_COUNT = 7;

	final private ConcurrentCacheMap<Number160, Boolean> cache = new ConcurrentCacheMap<Number160, Boolean>();

	final private static Set<Number160> counter = new HashSet<Number160>();

	public DefaultBroadcastHandler(Peer peer, Random rnd) {
		this.peer = peer;
		this.rnd = rnd;
	}

	public int getBroadcastCounter() {
		synchronized (counter) {
			return counter.size();
		}
	}

	@Override
	public void receive(final Message message) {
		Number160 messageKey = message.getKey();
		Map<Number160, Data> dataMap = message.getDataMap();
		int hopCount = message.getInteger();
		if (twiceSeen(messageKey)) {
			return;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("got broadcast map " + dataMap);
		}
		synchronized (counter) {
			counter.add(peer.getPeerID());
		}
		if (hopCount < MAX_HOP_COUNT) {
			if (hopCount == 0) {
				firstPeer(messageKey, dataMap, hopCount, message.isUDP());
			} else {
				otherPeer(messageKey, dataMap, hopCount, message.isUDP());
			}
		}
	}

	private boolean twiceSeen(final Number160 messageKey) {
		Boolean isInCache = cache.putIfAbsent(messageKey, Boolean.TRUE);
		if (isInCache != null) {
			if (isInCache == true) {
				cache.put(messageKey, false);
			} else {
				return true;
			}
		}
		return false;
	}

	private void firstPeer(final Number160 messageKey,
			final Map<Number160, Data> dataMap, final int hopCounter,
			final boolean isUDP) {
		final List<PeerAddress> list = peer.getPeerBean().getPeerMap().getAll();
		for (final PeerAddress peerAddress : list) {
			peer.getConnectionBean().getConnectionReservation().reserve(1)
					.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
						@Override
						public void operationComplete(
								FutureChannelCreator future) throws Exception {
							FutureResponse futureResponse = peer
									.getBroadcastRPC().send(
											peerAddress,
											messageKey,
											dataMap,
											hopCounter + 1,
											future.getChannelCreator(),
											peer.getConnectionBean()
													.getConfiguration()
													.getIdleTCPMillis(), isUDP);
							if (logger.isDebugEnabled()) {
								logger.debug("broadcast to " + peerAddress);
							}
							Utils.addReleaseListener(futureResponse, peer
									.getConnectionBean()
									.getConnectionReservation(), future
									.getChannelCreator(), 1);
						}
					});
		}
	}

	private void otherPeer(final Number160 messageKey,
			final Map<Number160, Data> dataMap, final int hopCounter,
			final boolean isUDP) {
		final List<PeerAddress> list = peer.getPeerBean().getPeerMap().getAll();
		final int max = Math.min(NR, list.size());
		peer.getConnectionBean().getConnectionReservation().reserve(max)
				.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(FutureChannelCreator future)
							throws Exception {
						for (int i = 0; i < max; i++) {
							PeerAddress randomAddress = list.remove(rnd
									.nextInt(list.size()));
							FutureResponse futureResponse = peer
									.getBroadcastRPC().send(
											randomAddress,
											messageKey,
											dataMap,
											hopCounter + 1,
											future.getChannelCreator(),
											peer.getConnectionBean()
													.getConfiguration()
													.getIdleTCPMillis(), isUDP);
							if (logger.isDebugEnabled()) {
								logger.debug("broadcast to " + randomAddress);
							}
							Utils.addReleaseListener(futureResponse, peer
									.getConnectionBean()
									.getConnectionReservation(), future
									.getChannelCreator(), 1);
						}
					}
				});
	}
}