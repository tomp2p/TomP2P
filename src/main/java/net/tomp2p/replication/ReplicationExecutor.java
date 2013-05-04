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
import net.tomp2p.futures.FutureDHT;
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

/**
 * This implements the default indirect replication.
 * 
 * @author Thomas Bocek
 * 
 */
public class ReplicationExecutor implements ResponsibilityListener, Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationExecutor.class);

    private final StorageGeneric storage;

    private final StorageRPC storageRPC;

    private final Peer peer;

    private final Map<BaseFuture, Long> pendingFutures;

    private final Replication replicationStorage;
    // default replication for put and add is 6
    private static final int REPLICATION = 6;

    /**
     * Constructor for the default indirect replication.
     * 
     * @param peer
     *            The peer
     */
    public ReplicationExecutor(final Peer peer) {
        this.peer = peer;
        this.storage = peer.getPeerBean().getStorage();
        this.storageRPC = peer.getStoreRPC();
        this.pendingFutures = peer.getPendingFutures();
        this.replicationStorage = peer.getPeerBean().getReplicationStorage();
        replicationStorage.addResponsibilityListener(this);
        replicationStorage.setReplicationFactor(REPLICATION);
    }

    @Override
    public void otherResponsible(final Number160 locationKey, final PeerAddress other) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Other peer " + other + " is responsible for " + locationKey + " I'm "
                    + storageRPC.getPeerAddress());
        }
        final Map<Number480, Data> dataMap = storage.subMap(locationKey);
        Number160 domainKeyOld = null;
        Map<Number160, Data> dataMapConverted = new HashMap<Number160, Data>();
        for (Map.Entry<Number480, Data> entry : dataMap.entrySet()) {
            final Number160 domainKey = entry.getKey().getDomainKey();
            final Number160 contentKey = entry.getKey().getContentKey();
            final Data data = entry.getValue();
            if (LOG.isDebugEnabled()) {
                LOG.debug("transfer from " + storageRPC.getPeerAddress() + " to " + other + " for key " + locationKey);
            }

            if (domainKeyOld == null || domainKeyOld.equals(domainKey)) {
                dataMapConverted.put(contentKey, data);
            } else {
                final Map<Number160, Data> dataMapConverted1 = new HashMap<Number160, Data>(dataMapConverted);
                sendDirect(other, locationKey, domainKey, dataMapConverted1);
                dataMapConverted.clear();
            }
            domainKeyOld = domainKey;
        }
        if (!dataMapConverted.isEmpty()) {
            sendDirect(other, locationKey, domainKeyOld, dataMapConverted);
        }
    }

    @Override
    public void meResponsible(final Number160 locationKey) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("I (" + storageRPC.getPeerAddress() + ") now responsible for " + locationKey);
        }
        synchronizeData(locationKey);
    }

    @Override
    public void run() {
        // we get called every x seconds for content we are responsible for. So
        // we need to make sure that there are enough copies. The easy way is to
        // publish it again... The good way is to do a diff
        Collection<Number160> locationKeys = storage.findContentForResponsiblePeerID(peer.getPeerID());

        for (Number160 locationKey : locationKeys) {
            synchronizeData(locationKey);
        }
    }

    /**
     * Get the data that I'm responsible for and make sure that there are enough replicas.
     * 
     * @param locationKey
     *            The location key.
     */
    private void synchronizeData(final Number160 locationKey) {
        final Map<Number480, Data> dataMap = storage.subMap(locationKey);
        Number160 domainKeyOld = null;
        Map<Number160, Data> dataMapConverted = new HashMap<Number160, Data>();
        for (Map.Entry<Number480, Data> entry : dataMap.entrySet()) {
            final Number160 domainKey = entry.getKey().getDomainKey();
            final Number160 contentKey = entry.getKey().getContentKey();
            final Data data = entry.getValue();
            if (LOG.isDebugEnabled()) {
                LOG.debug("[storage refresh] I (" + storageRPC.getPeerAddress() + ") restore " + locationKey);
            }
            if (domainKeyOld == null || domainKeyOld.equals(domainKey)) {
                dataMapConverted.put(contentKey, data);
            } else {
                final Map<Number160, Data> dataMapConverted1 = new HashMap<Number160, Data>(dataMapConverted);
                pendingFutures.put(send(locationKey, domainKey, dataMapConverted1), System.currentTimeMillis());
                dataMapConverted.clear();
                dataMapConverted.put(contentKey, data);
            }
            domainKeyOld = domainKey;
        }
        if (!dataMapConverted.isEmpty() && domainKeyOld != null) {
            pendingFutures.put(send(locationKey, domainKeyOld, dataMapConverted), System.currentTimeMillis());
        }
    }

    /**
     * If my peer is responsible, I'll issue a put if absent to make sure all replicas are stored.
     * 
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param dataMapConverted
     *            The data to store
     * @return The future of the put
     */
    protected FutureDHT send(final Number160 locationKey, final Number160 domainKey,
            final Map<Number160, Data> dataMapConverted) {
        return peer.put(locationKey).setDataMap(dataMapConverted).setDomainKey(domainKey).setPutIfAbsent(true).start();
    }

    /**
     * If an other peer is responsible, we send this peer our data, so that the other peer can take care of this.
     * 
     * @param other
     *            The other peer
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param dataMapConverted
     *            The data to store
     */
    protected void sendDirect(final PeerAddress other, final Number160 locationKey, final Number160 domainKey,
            final Map<Number160, Data> dataMapConverted) {
        FutureChannelCreator futureChannelCreator = peer.getConnectionBean().getConnectionReservation().reserve(1);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    FutureResponse futureResponse = storageRPC.put(other, locationKey, domainKey, dataMapConverted,
                            false, false, false, future.getChannelCreator(), false, null);
                    Utils.addReleaseListener(futureResponse, peer.getConnectionBean().getConnectionReservation(),
                            future.getChannelCreator(), 1);
                    pendingFutures.put(futureResponse, Timings.currentTimeMillis());
                } else {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("otherResponsible failed " + future.getFailedReason());
                    }
                }
            }
        });
    }
}
