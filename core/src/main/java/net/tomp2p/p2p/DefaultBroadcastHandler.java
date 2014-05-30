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
 * 
 * @author Thomas Bocek
 * 
 */
public class DefaultBroadcastHandler implements BroadcastHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBroadcastHandler.class);

    private static final Set<Number160> DEBUG_COUNTER = new HashSet<Number160>();

    private static final int NR = 10;

    private static final int MAX_HOP_COUNT = 4;

    private final Peer peer;

    private final Random rnd;

    private final ConcurrentCacheMap<Number160, Boolean> cache = new ConcurrentCacheMap<Number160, Boolean>();

    /**
     * Constructor.
     * 
     * @param peer
     *            The peer that sends the broadcast messages
     * @param rnd
     *            Random number, since its a random walk
     */
    public DefaultBroadcastHandler(final Peer peer, final Random rnd) {
        this.peer = peer;
        this.rnd = rnd;
    }

    /**
     * Used in JUnit tests only.
     * 
     * @return Return the number of peer in the debug set
     */
    int getBroadcastCounter() {
        synchronized (DEBUG_COUNTER) {
            return DEBUG_COUNTER.size();
        }
    }

    @Override
    public void receive(final Message message) {
        final Number160 messageKey = message.getKey(0);
        final Map<Number640, Data> dataMap;
        if(message.getDataMap(0)!=null) {
             dataMap = message.getDataMap(0).dataMap();
        } else {
            dataMap = null;
        }
        final int hopCount = message.getInteger(0);
        if (twiceSeen(messageKey)) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("got broadcast map " + dataMap + " from " + peer.getPeerID());
        }
        synchronized (DEBUG_COUNTER) {
            DEBUG_COUNTER.add(peer.getPeerID());
        }
        if (hopCount < MAX_HOP_COUNT) {
            if (hopCount == 0) {
                firstPeer(messageKey, dataMap, hopCount, message.isUdp());
            } else {
                otherPeer(messageKey, dataMap, hopCount, message.isUdp());
            }
        }
    }

    /**
     * If a message is seen for the second time, then we don't want to send this message again. The cache has a size of
     * 1024 entries and the objects have a default lifetime of 60s.
     * 
     * @param messageKey
     *            The key of the message
     * @return True if this message was send withing the last 60 seconds.
     */
    private boolean twiceSeen(final Number160 messageKey) {
        Boolean isInCache = cache.putIfAbsent(messageKey, Boolean.TRUE);
        if (isInCache != null) {
            if (isInCache) {
                cache.put(messageKey, false);
            } else {
                return true;
            }
        }
        return false;
    }

    /**
     * The first peer is the initiator. This peer that wants to start the broadcast will send it to all its neighbors.
     * Since this peer has an interest in sending, it should also work more than the other peers.
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
    private void firstPeer(final Number160 messageKey, final Map<Number640, Data> dataMap, final int hopCounter,
            final boolean isUDP) {
        final List<PeerAddress> list = peer.getPeerBean().peerMap().getAll();
        for (final PeerAddress peerAddress : list) {
            FutureChannelCreator frr = peer.getConnectionBean().reservation().create(isUDP?1:0, isUDP?0:1);
                    frr.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                        @Override
                        public void operationComplete(final FutureChannelCreator future) throws Exception {
                            if (future.isSuccess()) {
                                BroadcastBuilder broadcastBuilder = new BroadcastBuilder(peer, messageKey);
                                broadcastBuilder.dataMap(dataMap);
                                broadcastBuilder.hopCounter(hopCounter + 1);
                                broadcastBuilder.setIsUDP(isUDP);
                                FutureResponse futureResponse = peer.getBroadcastRPC().send(peerAddress, broadcastBuilder, 
                                        future.channelCreator(), broadcastBuilder);
                                LOG.debug("1st broadcast to {}", peerAddress);
                                Utils.addReleaseListener(future.channelCreator(), futureResponse);
                            }
                        }
                    });
        }
    }

    /**
     * This method is called on relaying peers. We select a random set and we send the message to those random peers.
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
    private void otherPeer(final Number160 messageKey, final Map<Number640, Data> dataMap,
            final int hopCounter, final boolean isUDP) {
        LOG.debug("other");
        final List<PeerAddress> list = peer.getPeerBean().peerMap().getAll();
        final int max = Math.min(NR, list.size());
        FutureChannelCreator frr = peer.getConnectionBean().reservation()
                .create(isUDP ? max : 0, isUDP ? 0 : max);
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
                        broadcastBuilder.setIsUDP(isUDP);
                        
                        futures[i] = peer.getBroadcastRPC().send(randomAddress,
                                broadcastBuilder, future.channelCreator(), broadcastBuilder);
                        LOG.debug("2nd broadcast to {}", randomAddress);
                    }
                    Utils.addReleaseListener(future.channelCreator(), futures);
                }
            }
        });
    }
}
