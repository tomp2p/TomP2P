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

package net.tomp2p.replication;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStorageReplication implements ResponsibilityListener, Runnable {
    final private static Logger logger = LoggerFactory.getLogger(DefaultStorageReplication.class);

    private final StorageGeneric storage;

    private final StorageRPC storageRPC;

    private final Peer peer;

    private final Map<BaseFuture, Long> pendingFutures;

    private final boolean forceUDP;

    public DefaultStorageReplication(Peer peer, StorageGeneric storage, StorageRPC storageRPC,
            Map<BaseFuture, Long> pendingFutures, boolean forceUDP) {
        this.peer = peer;
        this.storage = storage;
        this.storageRPC = storageRPC;
        this.pendingFutures = pendingFutures;
        this.forceUDP = forceUDP;
    }

    @Override
    public void otherResponsible(final Number160 locationKey, final PeerAddress other) {
        if (logger.isDebugEnabled()) {
            logger.debug("[storage] Other peer " + other + " is responsible for " + locationKey + " I'm "
                    + storageRPC.getPeerAddress());
        }
        final Map<Number480, Data> dataMap = storage.subMap(locationKey);
        Number160 domainKeyOld = null;
        Map<Number160, Data> dataMapConverted = new HashMap<Number160, Data>();
        for (Map.Entry<Number480, Data> entry : dataMap.entrySet()) {
            final Number160 domainKey = entry.getKey().getDomainKey();
            final Number160 contentKey = entry.getKey().getContentKey();
            final Data data = entry.getValue();
            if (logger.isDebugEnabled()) {
                logger.debug("transfer from " + storageRPC.getPeerAddress() + " to " + other + " for key "
                        + locationKey);
            }

            if (domainKeyOld == null || domainKeyOld.equals(domainKey)) {
                dataMapConverted.put(contentKey, data);
            } else {
                final Map<Number160, Data> dataMapConverted1 = new HashMap<Number160, Data>(dataMapConverted);
                send(other, locationKey, domainKey, dataMapConverted1);
                dataMapConverted.clear();
            }
            domainKeyOld = domainKey;
        }
        if (!dataMapConverted.isEmpty()) {
            send(other, locationKey, domainKeyOld, dataMapConverted);
        }
    }

    private void send(final PeerAddress other, final Number160 locationKey, final Number160 domainKey,
            final Map<Number160, Data> dataMapConverted) {
        FutureChannelCreator futureChannelCreator = peer.getConnectionBean().getConnectionReservation().reserve(1);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    FutureResponse futureResponse = storageRPC.put(other, locationKey, domainKey, dataMapConverted,
                            false, false, false, future.getChannelCreator(), forceUDP, null);
                    Utils.addReleaseListener(futureResponse, peer.getConnectionBean().getConnectionReservation(),
                            future.getChannelCreator(), 1);
                    pendingFutures.put(futureResponse, Timings.currentTimeMillis());
                } else {
                    if (logger.isErrorEnabled()) {
                        logger.error("otherResponsible failed " + future.getFailedReason());
                    }
                }
            }
        });
    }

    @Override
    public void meResponsible(Number160 locationKey) {
        if (logger.isDebugEnabled())
            logger.debug("[storage] I (" + storageRPC.getPeerAddress() + ") now responsible for " + locationKey);
        // TODO: we could speed this up a little and trigger the maintenance
        // right away
    }

    @Override
    public void run() {
        // we get called every x seconds for content we are responsible for. So
        // we need to make sure that there are enough copies. The easy way is to
        // publish it again... The good way is to do a diff
        Collection<Number160> locationKeys = storage.findContentForResponsiblePeerID(peer.getPeerID());
        if (locationKeys == null) {
            return;
        }

        for (Number160 locationKey : locationKeys) {
            final Map<Number480, Data> dataMap = storage.subMap(locationKey);
            Number160 domainKeyOld = null;
            Map<Number160, Data> dataMapConverted = new HashMap<Number160, Data>();
            for (Map.Entry<Number480, Data> entry : dataMap.entrySet()) {
                final Number160 domainKey = entry.getKey().getDomainKey();
                final Number160 contentKey = entry.getKey().getContentKey();
                final Data data = entry.getValue();
                if (logger.isDebugEnabled()) {
                    logger.debug("[storage refresh] I (" + storageRPC.getPeerAddress() + ") restore " + locationKey);
                }
                if (domainKeyOld == null || domainKeyOld.equals(domainKey)) {
                    dataMapConverted.put(contentKey, data);
                } else {
                    final Map<Number160, Data> dataMapConverted1 = new HashMap<Number160, Data>(dataMapConverted);
                    pendingFutures.put(peer.put(locationKey).setDataMap(dataMapConverted1).setDomainKey(domainKey)
                            .setPutIfAbsent(true).start(), System.currentTimeMillis());
                    dataMapConverted.clear();
                    dataMapConverted.put(contentKey, data);
                }
                domainKeyOld = domainKey;
            }
            if (!dataMapConverted.isEmpty() && domainKeyOld != null) {
                pendingFutures.put(peer.put(locationKey).setDataMap(dataMapConverted).setDomainKey(domainKeyOld)
                        .setPutIfAbsent(true).start(), System.currentTimeMillis());
            }
        }
    }
}
