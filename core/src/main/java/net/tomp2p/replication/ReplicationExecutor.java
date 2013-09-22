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
import java.util.Timer;
import java.util.TimerTask;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FuturePut;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.builder.PutBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implements the default indirect replication.
 * 
 * @author Thomas Bocek
 * 
 */
public class ReplicationExecutor extends TimerTask implements ResponsibilityListener {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationExecutor.class);

    private final StorageGeneric storage;

    private final StorageRPC storageRPC;

    private final Peer peer;

    private final Replication replicationStorage;
    // default replication for put and add is 6
    private static final int REPLICATION = 6;
    
    private int intervalMillis = 60 * 1000;

    /**
     * Constructor for the default indirect replication.
     * 
     * @param peer
     *            The peer
     */
    public ReplicationExecutor(final Peer peer) {
        this.peer = peer;
        this.storage = peer.getPeerBean().storage();
        this.storageRPC = peer.getStoreRPC();
        this.replicationStorage = peer.getPeerBean().replicationStorage();
        replicationStorage.addResponsibilityListener(this);
        replicationStorage.setReplicationFactor(REPLICATION);
    }
    
    public void init(Peer peer, Timer timer) {
        timer.scheduleAtFixedRate(this, intervalMillis, intervalMillis);
    }

    @Override
    public void otherResponsible(final Number160 locationKey, final PeerAddress other) {
        LOG.debug("Other peer {} is responsible for {}. I'm {}", other, locationKey, storageRPC.peerBean()
                .serverPeerAddress());

        final Map<Number480, Data> dataMap = storage.subMap(locationKey);
        Number160 domainKeyOld = null;
        Map<Number160, Data> dataMapConverted = new HashMap<Number160, Data>();
        for (Map.Entry<Number480, Data> entry : dataMap.entrySet()) {
            final Number160 domainKey = entry.getKey().getDomainKey();
            final Number160 contentKey = entry.getKey().getContentKey();
            final Data data = entry.getValue();
            LOG.debug("transfer from {} to {} for key {}", storageRPC.peerBean().serverPeerAddress(), other,
                    locationKey);

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
        LOG.debug("I ({}) now responsible for {}", storageRPC.peerBean().serverPeerAddress(), locationKey);
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
            LOG.debug("[storage refresh] I ({}) restore {}", storageRPC.peerBean().serverPeerAddress(),
                    locationKey);
            if (domainKeyOld == null || domainKeyOld.equals(domainKey)) {
                dataMapConverted.put(contentKey, data);
            } else {
                final Map<Number160, Data> dataMapConverted1 = new HashMap<Number160, Data>(dataMapConverted);
                FuturePut futurePut = send(locationKey, domainKey, dataMapConverted1);
                peer.notifyAutomaticFutures(futurePut);
                dataMapConverted.clear();
                dataMapConverted.put(contentKey, data);
            }
            domainKeyOld = domainKey;
        }
        if (!dataMapConverted.isEmpty() && domainKeyOld != null) {
            FuturePut futurePut = send(locationKey, domainKeyOld, dataMapConverted);
            peer.notifyAutomaticFutures(futurePut);
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
    protected FuturePut send(final Number160 locationKey, final Number160 domainKey,
            final Map<Number160, Data> dataMapConverted) {
        return peer.put(locationKey).setDataMapContent(dataMapConverted).setDomainKey(domainKey)
                .setPutIfAbsent(true).start();
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
     * @param dataMapConvert
     *            The data to store
     */
    protected void sendDirect(final PeerAddress other, final Number160 locationKey,
            final Number160 domainKey, final Map<Number160, Data> dataMapConvert) {
        FutureChannelCreator futureChannelCreator = peer.getConnectionBean().reservation().create(0, 1);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    PutBuilder putBuilder = new PutBuilder(peer, locationKey);
                    putBuilder.setDomainKey(domainKey);
                    putBuilder.setDataMapContent(dataMapConvert);
                    FutureResponse futureResponse = storageRPC.put(other, putBuilder,
                            future.getChannelCreator());
                    Utils.addReleaseListener(future.getChannelCreator(), futureResponse);
                    peer.notifyAutomaticFutures(futureResponse);
                } else {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("otherResponsible failed " + future.getFailedReason());
                    }
                }
            }
        });
    }
    
    public int getIntervalMillis() {
        return intervalMillis;
    }

    public void setIntervalMillis(int intervalMillis) {
        this.intervalMillis = intervalMillis;
    }
}
