package net.tomp2p.tracker;

import java.security.PublicKey;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.PeerException;
import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.DefaultMaintenance;
import net.tomp2p.peers.Maintainable;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapChangeListener;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.DigestTracker;
import net.tomp2p.utils.ConcurrentCacheMap;

public class TrackerStorage implements Maintainable, PeerMapChangeListener, PeerStatusListener, DigestTracker {
	// Core
	public static final int TRACKER_CACHE_SIZE = 1000;
	final private Map<Number320, TrackerData> dataMapUnverified;
	final private Map<Number320, TrackerData> dataMap;
	final private boolean verifyPeersOnTracker;
	private final int[] intervalSeconds;
	private final ConcurrentCacheMap<Number160, Boolean> peerOffline;
	private final PeerAddress self;
	private final int trackerTimoutSeconds;
	private final PeerMap peerMap;
	private final int replicationFactor;
	//comes later
	private PeerExchange peerExchange;

	public TrackerStorage(int trackerTimoutSeconds, final int[] intervalSeconds,
	        int replicationFactor, PeerMap peerMap, PeerAddress self, boolean verifyPeersOnTracker) {
		dataMapUnverified = new ConcurrentCacheMap<Number320, TrackerData>(trackerTimoutSeconds, TRACKER_CACHE_SIZE,
		        true);
		dataMap = new ConcurrentCacheMap<Number320, TrackerData>(trackerTimoutSeconds, TRACKER_CACHE_SIZE, true);
		peerOffline = new ConcurrentCacheMap<Number160, Boolean>(trackerTimoutSeconds * 5, TRACKER_CACHE_SIZE, false);
		this.trackerTimoutSeconds = trackerTimoutSeconds;
		this.intervalSeconds = intervalSeconds;
		this.self = self;
		this.peerMap = peerMap;
		this.replicationFactor = replicationFactor;
		this.verifyPeersOnTracker = verifyPeersOnTracker;
	}

	public boolean put(Number320 key, PeerAddress peerAddress, PublicKey publicKey, Data attachement) {
		if (peerOffline.containsKey(peerAddress.peerId())) {
			return false;
		}
		// security check
		Data oldDataUnverified = findOld(key, peerAddress, dataMapUnverified);
		boolean isUnverified = false;
		boolean isVerified = false;
		if(oldDataUnverified != null) {
			//security check
			if (oldDataUnverified.publicKey()!=null && !oldDataUnverified.publicKey().equals(publicKey)) {
				return false;
			}
			isUnverified = true;
		} else {
			Data oldData = findOld(key, peerAddress, dataMap);
			if(oldData != null) {
				//security check
				if (oldData.publicKey()!=null && !oldData.publicKey().equals(publicKey)) {
					return false;
				}
				isVerified = true;
			}
		}
		
		if(attachement == null) {
			attachement = new Data();
		}
		// now store
		attachement.publicKey(publicKey);
		final Map<Number320, TrackerData> dataMapToStore;
		if(isUnverified) {
			dataMapToStore = dataMapUnverified;
		} else if (isVerified) {
			dataMapToStore = dataMap;
		} else if(verifyPeersOnTracker) {
			dataMapToStore = dataMapUnverified;
		} else {
			dataMapToStore = dataMap;
		}
		return add(key, new PeerStatistic(peerAddress), dataMapToStore, attachement);
	}

	private Data findOld(Number320 key, PeerAddress peerAddress, Map<Number320, TrackerData> dataMap) {
		for (Map.Entry<Number320, TrackerData> entry : dataMap.entrySet()) {
			for (Map.Entry<PeerStatistic, Data> entry2 : entry.getValue().peerAddresses().entrySet()) {
				if (entry2.getKey().peerAddress().equals(peerAddress)) {
					return entry2.getValue();
				}
			}
		}
		return null;
	}
	
	public PeerExchange peerExchange() {
		return peerExchange;
	}
	
	public TrackerStorage peerExchange(PeerExchange peerExchange) {
		this.peerExchange = peerExchange;
		return this;
	}

	@Override
	public PeerStatistic nextForMaintenance(Collection<PeerAddress> notInterestedAddresses) {
		for (Map.Entry<Number320, TrackerData> entry : dataMapUnverified.entrySet()) {
			for (Map.Entry<PeerStatistic, Data> entry2 : entry.getValue().peerAddresses().entrySet()) {
				if (DefaultMaintenance.needMaintenance(entry2.getKey(), intervalSeconds)) {
					return entry2.getKey();
				}
			}
		}
		return null;
	}

	@Override
	public void peerInserted(PeerAddress remotePeer, boolean verified) {
		if (verified) {
			for (Map.Entry<Number320, TrackerData> entry : dataMap.entrySet()) {
				//if I have conetnt and I see a peer as a new responsible, push it.
				if(isInReplicationRange(entry.getKey().locationKey(), remotePeer, replicationFactor)) {
					//limit the pushing peer to those that are responsible
					if(isInReplicationRange(entry.getKey().locationKey(), self, replicationFactor)) {
						peerExchange.peerExchange(remotePeer, entry.getKey(), entry.getValue());
					}
				}
			}
		}
	}

	@Override
	public void peerRemoved(PeerAddress remotePeer, PeerStatistic storedPeerAddress) {
		// if a responsible peer is removed, and I see myself as a responsible, 
		// I should push my content to a random responsible
		for (Map.Entry<Number320, TrackerData> entry : dataMap.entrySet()) {
			//if I have conetnt and I see the removed peer as a responsible, push it.
			if(isInReplicationRange(entry.getKey().locationKey(), remotePeer, replicationFactor)) {
				//limit the pushing peer to those that are responsible
				if(isInReplicationRange(entry.getKey().locationKey(), self, replicationFactor)) {
					NavigableSet<PeerAddress> closePeers = peerMap.closePeers(entry.getKey().locationKey(), replicationFactor);
					PeerAddress newResponsible = closePeers.headSet(remotePeer).last();
					peerExchange.peerExchange(newResponsible, entry.getKey(), entry.getValue());
				}
			}
		}
	}

	@Override
	public void peerUpdated(PeerAddress peerAddress, PeerStatistic storedPeerAddress) {
		// nothing to do
	}

	private boolean isInReplicationRange(final Number160 locationKey, final PeerAddress peerAddress,
	        final int replicationFactor) {
		SortedSet<PeerAddress> tmp = peerMap.closePeers(locationKey, replicationFactor);
		tmp.add(self);
		return tmp.headSet(peerAddress).size() < replicationFactor;
	}

	private boolean add(Number320 key, PeerStatistic stat, Map<Number320, TrackerData> map, Data attachement) {
		TrackerData trackerData = map.get(key);
		if (trackerData == null) {
			trackerData = new TrackerData(new ConcurrentCacheMap<PeerStatistic, Data>(trackerTimoutSeconds,
			        TRACKER_CACHE_SIZE, true));
			map.put(key, trackerData);
		}
		if (trackerData.size() < TRACKER_CACHE_SIZE) {
			trackerData.put(stat, attachement);
			return true;
		} else {
			return false;
		}
	}

	private boolean remove(Number320 key, PeerStatistic stat, Map<Number320, TrackerData> map) {
		TrackerData trackerData = map.get(key);
		if (trackerData != null) {
			boolean retVal = trackerData.remove(stat.peerAddress().peerId()) != null;
			if(trackerData.peerAddresses().size() == 0) {
				map.remove(key);
			}
			return retVal;
		}
		return false;
	}

	public TrackerData peers(Number320 key) {
		return dataMap.get(key);
	}

	public Collection<Number320> keys() {
		return dataMap.keySet();
	}

	@Override
	public boolean peerFailed(PeerAddress remotePeer, PeerException reason) {
		peerOffline.put(remotePeer.peerId(), Boolean.TRUE);
		Number320 keyToRemove = null;
		PeerStatistic statToRemove = null;
		for (Map.Entry<Number320, TrackerData> entry : dataMapUnverified.entrySet()) {
			for (Map.Entry<PeerStatistic, Data> entry2 : entry.getValue().peerAddresses().entrySet()) {
				if (entry2.getKey().peerAddress().equals(remotePeer)) {
					keyToRemove = entry.getKey();
					statToRemove = entry2.getKey();
				}
			}
		}
		if(keyToRemove !=null) {
			remove(keyToRemove, statToRemove, dataMapUnverified);
		}
		//
		for (Map.Entry<Number320, TrackerData> entry : dataMap.entrySet()) {
			for (Map.Entry<PeerStatistic, Data> entry2 : entry.getValue().peerAddresses().entrySet()) {
				if (entry2.getKey().peerAddress().equals(remotePeer)) {
					keyToRemove = entry.getKey();
					statToRemove = entry2.getKey();
				}
			}
		}
		if(keyToRemove !=null) {
			remove(keyToRemove, statToRemove, dataMap);
		}
		return true;
	}

	@Override
	public boolean peerFound(PeerAddress remotePeer, PeerAddress referrer, PeerConnection peerConnection) {
		boolean firsthand = referrer == null;
		if (firsthand) {
			peerOffline.remove(remotePeer.peerId());
			Number320 keyToRemove = null;
			PeerStatistic statToRemove = null;
			for (Map.Entry<Number320, TrackerData> entry : dataMapUnverified.entrySet()) {
				for (Map.Entry<PeerStatistic, Data> entry2 : entry.getValue().peerAddresses().entrySet()) {
					PeerAddress tmp = entry2.getKey().peerAddress(); 
					if (tmp.equals(remotePeer)) {
						if (add(entry.getKey(), entry2.getKey(), dataMap, entry2.getValue())) {
							// only remove from unverified if we could store to
							// verified
							keyToRemove = entry.getKey();
							statToRemove = entry2.getKey();
							statToRemove.successfullyChecked();
							// TODO: here we can break to the if statement below
						}
					}
				}
			}
			if (keyToRemove != null && statToRemove != null) {
				remove(keyToRemove, statToRemove, dataMapUnverified);
			}
		}
		return true;
	}

	public int size() {
	    return dataMap.size();
    }

	public int sizeUnverified() {
		return dataMapUnverified.size();
    }

	@Override
    public DigestInfo digest(Number160 locationKey, Number160 domainKey, Number160 contentKey) {
		Number160 contentDigest = Number160.ZERO;
		int counter = 0;
		TrackerData trackerData = dataMap.get(new Number320(locationKey, domainKey));
		if(trackerData!=null) {
			if(contentKey!=null) {
				Map.Entry<PeerStatistic, Data> entry = trackerData.get(contentKey);
				if(entry!=null) {
					return new DigestInfo(Number160.ZERO, contentKey, 1);
				}
			} else {
				for(PeerStatistic peerStatatistic: trackerData.peerAddresses().keySet()) {
					contentDigest = contentDigest.xor(peerStatatistic.peerAddress().peerId());
					counter++;
				}
			}
		}
		return new DigestInfo(Number160.ZERO, contentKey, counter);
    }
}
