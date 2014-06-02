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

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.tomp2p.dht.StorageMemory;
import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.replication.Replication;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.Digest;
import net.tomp2p.storage.IdentityManagement;
import net.tomp2p.storage.Storage;
import net.tomp2p.utils.ConcurrentCacheMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The maintenance for the tracker is done by the client peer. Thus the peers on a tracker expire, but a client can send
 * a Bloom filter with peers, that he knows are offline. TrackerStorage stores the data in memory only. TODO: check
 * availability of secondary peers and periodically check if peers from the mesh are still online, right now we rely on
 * the PeerMap mechanism
 * 
 * @author draft
 */
public class TrackerStorage implements PeerStatusListener, Digest {
    private static final Logger logger = LoggerFactory.getLogger(TrackerStorage.class);

   // private static final Map<Number160, TrackerData> EMPTY_MAP = new HashMap<Number160, TrackerData>();

    private static final DigestInfo EMPTY_DIGEST_INFO = new DigestInfo(0);

    // once you call listen, changing this value has no effect unless a new
    // TrackerRPC is created. The value is chosen to fit into one single UDP
    // packet. This means that the attached data must be 0, otherwise you have
    // to used tcp. don't forget to add the header as well
    public static final int TRACKER_SIZE = 35;

    public static final int TRACKER_CACHE_SIZE = 1000;

    // K=location and domain, V=peerId and attachment
    private final ConcurrentMap<Number320, TrackerData> trackerDataActive;

    private final ConcurrentCacheMap<Number320, TrackerData> trackerDataMesh;

    private final ConcurrentCacheMap<Number320, TrackerData> trackerDataSecondary;

    // for timeouts we need to know which peer stores what data to remove it
    // from the primary and secondary tracker
    // K=peerId of the offline peer, V=location and domain
    private final ConcurrentCacheMap<Number160, Collection<Number320>> reverseTrackerDataMesh;

    private final ConcurrentCacheMap<Number160, Collection<Number320>> reverseTrackerDataSecondary;

    // K=peerId of the offline peer, V=reporter
    private final ConcurrentCacheMap<Number160, Collection<Number160>> peerOffline;

    private final IdentityManagement identityManagement;

    private final int trackerTimoutSeconds;

    private final Replication replication;

    private final Maintenance maintenance;

    // variable parameters
    private boolean fillPrimaryStorageFast = false;

    private int secondaryFactor = 5;

    private int primanyFactor = 1;

    private final Storage storageMemoryReplication = new StorageMemory();

    public enum ReferrerType {
        ACTIVE, MESH
    };

    public TrackerStorage(IdentityManagement identityManagement, int trackerTimoutSeconds,
            Replication replication, Maintenance maintenance) {
        this.trackerTimoutSeconds = trackerTimoutSeconds;
        this.identityManagement = identityManagement;
        this.replication = replication;
        this.maintenance = maintenance;
        trackerDataActive = new ConcurrentHashMap<Number320, TrackerData>();
        trackerDataMesh = new ConcurrentCacheMap<Number320, TrackerData>(trackerTimoutSeconds,
                TRACKER_CACHE_SIZE, true);
        trackerDataSecondary = new ConcurrentCacheMap<Number320, TrackerData>(trackerTimoutSeconds,
                TRACKER_CACHE_SIZE, true);
        //
        reverseTrackerDataMesh = new ConcurrentCacheMap<Number160, Collection<Number320>>(
                trackerTimoutSeconds, TRACKER_CACHE_SIZE, true);
        reverseTrackerDataSecondary = new ConcurrentCacheMap<Number160, Collection<Number320>>(
                trackerTimoutSeconds, TRACKER_CACHE_SIZE, true);

        // if everything is perfect, a factor of 2 is enough, to be on the safe
        // side factor 5 is used.
        peerOffline = new ConcurrentCacheMap<Number160, Collection<Number160>>(trackerTimoutSeconds * 5,
                TRACKER_CACHE_SIZE, false);
    }

    public TrackerData activePeers(Number160 locationKey, Number160 domainKey) {
        Number320 keys = new Number320(locationKey, domainKey);
        TrackerData data = trackerDataActive.get(keys);
        if (data == null) {
            return null;
        }
        // return a copy
        synchronized (data) {
            return new TrackerData(data.peerAddresses(), identityManagement.peerAddress());
        }

    }

    public TrackerData meshPeers(Number160 locationKey, Number160 domainKey) {
        Number320 keys = new Number320(locationKey, domainKey);
        TrackerData data = trackerDataMesh.get(keys);
        if (data == null) {
            return null;
        }
        // return a copy
        synchronized (data) {
            return new TrackerData(data.peerAddresses(), identityManagement.peerAddress());
        }

    }

    public TrackerData secondaryPeers(Number160 locationKey, Number160 domainKey) {
        Number320 keys = new Number320(locationKey, domainKey);
        TrackerData data = trackerDataSecondary.get(keys);
        if (data == null) {
            return null;
        }
        // return a copy
        synchronized (data) {
            return new TrackerData(data.peerAddresses(), identityManagement.peerAddress());
        }
    }

    public void addActive(Number160 locationKey, Number160 domainKey, PeerAddress remotePeer, Data attachement) {
        Number320 key = new Number320(locationKey, domainKey);

        TrackerData trackerData = trackerDataActive.get(key);
        if (trackerData == null) {
            trackerData = new TrackerData(new HashMap<PeerAddress, Data>(),
                    identityManagement.peerAddress());
            trackerDataActive.put(key, trackerData);
        }
        synchronized (trackerData) {
            trackerData.put(remotePeer, attachement);
        }
    }

    public boolean removeActive(Number160 locationKey, Number160 domainKey, Number160 remotePeerId) {
        Number320 key = new Number320(locationKey, domainKey);
        TrackerData data = trackerDataActive.get(key);
        if (data == null) {
            return false;
        }
        Map.Entry<PeerAddress, Data> retVal;
        // we don't expect many concurrent access for data
        synchronized (data) {
            retVal = data.remove(remotePeerId);
        }
        return retVal != null;
    }

    public boolean put(Number160 locationKey, Number160 domainKey, PeerAddress peerAddress,
            PublicKey publicKey, Data attachement) {
        if (logger.isDebugEnabled()) {
            logger.debug("try to store on tracker " + locationKey);
        }
        Number160 peerId = peerAddress.peerId();
        // check if this guy is offline
        if (isOffline(peerAddress)) {
            return false;
        }
        // check identity
        if (!identityManagement.checkIdentity(peerId, publicKey)) {
            return false;
        }

        // store the data
        if (canStorePrimary(locationKey, domainKey, false)) {
            // we have space in our primary tracker, store them there!
            Number320 key = new Number320(locationKey, domainKey);
            if (storeData(peerAddress, attachement, peerId, key, trackerDataMesh, reverseTrackerDataMesh,
                    getPrimanyFactor())) {
                if (replication != null) {
                    replication.updateAndNotifyResponsibilities(locationKey);
                }
                return true;
            }
        }
        // do not store in the secondary map, since this is used for PEX, for
        // unknown peers.
        return false;
    }

    private boolean isOffline(PeerAddress peerAddress) {
        // TODO: always trust myself, do a majority voting for others
        if (peerOffline.containsKey(peerAddress.peerId()))
            return true;
        return false;
    }

    public boolean putReferred(Number160 locationKey, Number160 domainKey, PeerAddress peerAddress,
            PeerAddress referrer, Data attachement, ReferrerType type) {
        Number160 peerId = peerAddress.peerId();
        // we cannot do public key check, because these data is referenced from
        // other peers and we don't know about the timeouts as well
        // store the data
        if (canStoreSecondary(locationKey, domainKey)) {
            // maybe we have space in our secondary tracker, store them there!
            Number320 key = new Number320(locationKey, domainKey);
            if (storeData(peerAddress, attachement, peerId, key, trackerDataSecondary,
                    reverseTrackerDataSecondary, getSecondaryFactor())) {
                if (ReferrerType.MESH == type) {
                    if (!isSecondaryTracker(locationKey, domainKey)) {
                        maintenance
                                .addTrackerMaintenance(peerAddress, referrer, locationKey, domainKey, this);
                    }
                }
                return true;
            }
        }
        return false;
    }

    public boolean moveFromSecondaryToMesh(PeerAddress peerAddress, PeerAddress referrer,
            Number160 locationKey, Number160 domainKey, PublicKey publicKey) {
        Number320 key = new Number320(locationKey, domainKey);
        TrackerData map = trackerDataSecondary.get(key);
        if (map == null) {
            return false;
        }
        synchronized (map) {
            Map.Entry<PeerAddress, Data> data = map.remove(peerAddress.peerId());
            if (data != null) {
                return put(locationKey, domainKey, data.getKey(), publicKey, data.getValue());
            }
        }
        return false;
    }

    private boolean storeData(PeerAddress peerAddress, Data attachement, Number160 peerId, Number320 key,
            ConcurrentMap<Number320, TrackerData> trackerData,
            ConcurrentMap<Number160, Collection<Number320>> reverseTrackerData, int factor) {
        TrackerData data = trackerData.get(key);
        if (data == null) {
            data = new TrackerData(new HashMap<PeerAddress, Data>(), null);
            trackerData.put(key, data);
        }

        synchronized (data) {
            if (data.size() > TRACKER_SIZE * factor) {
                return false;
            }
            data.put(peerAddress, attachement);
        }

        Collection<Number320> collection = reverseTrackerData.get(peerId);
        if (collection == null) {
            collection = new HashSet<Number320>();
        }
        synchronized (collection) {
            collection.add(key);
        }

        return true;
    }

    private boolean remove(Number160 peerId, ConcurrentMap<Number320, TrackerData> trackerData,
            ConcurrentMap<Number160, Collection<Number320>> reverseTrackerData) {
        boolean retVal = false;
        Collection<Number320> collection = reverseTrackerData.remove(peerId);
        if (collection == null) {
            return false;
        }
        synchronized (collection) {
            for (Number320 key : collection) {
                TrackerData data = trackerData.get(key);
                if (data == null) {
                    continue;
                }
                synchronized (data) {
                    if (data.remove(peerId) != null) {
                        retVal = true;
                    }
                    if (data.size() == 0) {
                        trackerData.remove(key);
                    }
                }
            }
        }
        return retVal;
    }

    private boolean canStorePrimary(Number160 locationKey, Number160 domainKey, boolean referred) {
        if (!referred || isFillPrimaryStorageFast()) {
            return sizePrimary(locationKey, domainKey) <= (TRACKER_SIZE * getPrimanyFactor());
        } else {
            return false;
        }
    }

    private boolean canStoreSecondary(Number160 locationKey, Number160 domainKey) {
        return sizeSecondary(locationKey, domainKey) <= (TRACKER_SIZE * getSecondaryFactor());
    }

    public int sizePrimary(Number160 locationKey, Number160 domainKey) {
        return size(locationKey, domainKey, trackerDataMesh);
    }

    public int sizeSecondary(Number160 locationKey, Number160 domainKey) {
        return size(locationKey, domainKey, trackerDataSecondary);
    }

    private int size(Number160 locationKey, Number160 domainKey,
            ConcurrentMap<Number320, TrackerData> trackerData) {
        Number320 key = new Number320(locationKey, domainKey);
        TrackerData data = trackerData.get(key);
        if (data == null) {
            return 0;
        } else {
            synchronized (data) {
                return data.size();
            }
        }
    }

    public void setSecondaryFactor(int secondaryFactor) {
        this.secondaryFactor = secondaryFactor;
    }

    public int getSecondaryFactor() {
        return secondaryFactor;
    }

    public void setPrimanyFactor(int primanyFactor) {
        this.primanyFactor = primanyFactor;
    }

    public int getPrimanyFactor() {
        return primanyFactor;
    }

    @Override
    public boolean peerFailed(final PeerAddress remotePeer, final FailReason reason) {
        peerOffline(remotePeer.peerId(), identityManagement.self());
        return true;
    }

    private void peerOffline(Number160 peerId, Number160 referrerId) {
        indicateOffline(peerId, referrerId);
        remove(peerId, trackerDataMesh, reverseTrackerDataMesh);
        remove(peerId, trackerDataSecondary, reverseTrackerDataSecondary);
    }

    private void indicateOffline(Number160 peerId, Number160 referrerId) {
        Collection<Number160> collection = new HashSet<Number160>();
        Collection<Number160> collection2 = peerOffline.putIfAbsent(peerId, collection);
        collection = collection2 == null ? collection : collection2;
        collection.add(referrerId);
    }

    @Override
    public boolean peerFound(final PeerAddress remotePeer, final PeerAddress referrer) {
        peerOffline.remove(remotePeer.peerId());
        return true;
    }

    private DigestInfo digest(Number160 locationKey, Number160 domainKey) {
        TrackerData data = trackerDataMesh.get(new Number320(locationKey, domainKey));
        if (data == null) {
            return EMPTY_DIGEST_INFO;
        }
        synchronized (data) {
            return new DigestInfo(data.size());
        }
    }

    // TODO: merge with digest
    // @Override
    public DigestInfo digest(Number160 locationKey, Number160 domainKey, Number160 contentKey) {
        if (contentKey == null) {
            return digest(locationKey, domainKey);
        }
        TrackerData data = trackerDataMesh.get(new Number320(locationKey, domainKey));
        if (data == null) {
            return EMPTY_DIGEST_INFO;
        }
        int counter = 0;
        synchronized (data) {
            if (data.containsKey(contentKey)) {
                counter++;
            }
        }
        return new DigestInfo(counter);
    }

    public void removeReferred(Number160 locationKey, Number160 domainKey, Number160 key, PeerAddress referrer) {
        indicateOffline(key, referrer.peerId());
    }

    public void setFillPrimaryStorageFast(boolean fillPrimaryStorageFast) {
        this.fillPrimaryStorageFast = fillPrimaryStorageFast;
    }

    public boolean isFillPrimaryStorageFast() {
        return fillPrimaryStorageFast;
    }

    public int getTrackerTimoutSeconds() {
        return trackerTimoutSeconds;
    }

    /**
     * A peer is a secondary tracker if the peers stores itself on the tracker as well. The primary trackers do not
     * behave like this.
     * 
     * @param locationKey
     * @param domainKey
     * @return
     */
    public boolean isSecondaryTracker(Number160 locationKey, Number160 domainKey) {
        Number320 keys = new Number320(locationKey, domainKey);
        TrackerData data = trackerDataMesh.get(keys);
        if (data == null)
            return false;
        // return a copy
        synchronized (data) {
            return data.containsKey(identityManagement.self());
        }
    }

    // TODO: seems a bit inefficient, but it works for the moment
    public Collection<Number160> responsibleDomains(Number160 locationKey) {
        Collection<Number160> retVal = new ArrayList<Number160>();
        for (Number320 number320 : trackerDataMesh.keySet()) {
            if (number320.locationKey().equals(locationKey)) {
                retVal.add(number320.domainKey());
            }
        }
        return retVal;
    }

    
    public Collection<Number160> findPeerIDsForResponsibleContent(Number160 locationKey) {
        return storageMemoryReplication.findPeerIDsForResponsibleContent(locationKey);
    }

    
    public Collection<Number160> findContentForResponsiblePeerID(Number160 peerID) {
        return storageMemoryReplication.findContentForResponsiblePeerID(peerID);
    }

    
    public boolean updateResponsibilities(Number160 locationKey, Number160 peerId) {
        return storageMemoryReplication.updateResponsibilities(locationKey, peerId);
    }

    
    public void removeResponsibility(Number160 locationKey) {
        storageMemoryReplication.removeResponsibility(locationKey);
    }

	public void removeResponsibility(Number160 locationKey, Number160 peerId) {
		storageMemoryReplication.removeResponsibility(locationKey, peerId);
	}

    @Override
    public DigestInfo digest(Number640 from, Number640 to) {
        throw new UnsupportedOperationException("Bloom filters and trackers are not supported");
    }

    @Override
    public DigestInfo digest(Number320 key, SimpleBloomFilter<Number160> keyBloomFilter,
            SimpleBloomFilter<Number160> contentBloomFilter) {
        throw new UnsupportedOperationException("Bloom filters and trackers are not supported");
    }
}
