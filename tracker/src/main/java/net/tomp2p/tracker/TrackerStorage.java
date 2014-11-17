package net.tomp2p.tracker;

import java.security.PublicKey;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import net.tomp2p.utils.Pair;

public class TrackerStorage implements Maintainable, PeerMapChangeListener, PeerStatusListener, DigestTracker {
	// Core
	public static final int TRACKER_CACHE_SIZE = 1000;
	final private Map<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> dataMapUnverified;
	final private Map<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> dataMap;
	private final ConcurrentCacheMap<Number160, Boolean> peerOffline;
	
	final private boolean verifyPeersOnTracker;
	private final int[] intervalSeconds;
	private final PeerAddress self;
	private final int trackerTimoutSeconds;
	private final PeerMap peerMap;
	private final int replicationFactor;
	//comes later
	private PeerExchange peerExchange;

	public TrackerStorage(int trackerTimoutSeconds, final int[] intervalSeconds,
	        int replicationFactor, PeerMap peerMap, PeerAddress self, boolean verifyPeersOnTracker) {
		dataMapUnverified = new ConcurrentCacheMap<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>>(trackerTimoutSeconds, TRACKER_CACHE_SIZE,
		        true);
		dataMap = new ConcurrentCacheMap<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>>(trackerTimoutSeconds, TRACKER_CACHE_SIZE, true);
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
		Pair<PeerStatistic, Data> pair = findOld(key, peerAddress, dataMapUnverified);
		Data oldDataUnverified = pair != null ? pair.element1() : null;
		
		boolean isUnverified = false;
		boolean isVerified = false;
		if(oldDataUnverified != null) {
			//security check
			if (oldDataUnverified.publicKey()!=null && !oldDataUnverified.publicKey().equals(publicKey)) {
				return false;
			}
			isUnverified = true;
		} else {
			Pair<PeerStatistic, Data> pair2 = findOld(key, peerAddress, dataMap);
			Data oldData = pair2 != null? pair2.element1() : null;
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
		final Map<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> dataMapToStore;
		if(isUnverified) {
			dataMapToStore = dataMapUnverified;
		} else if (isVerified) {
			dataMapToStore = dataMap;
		} else if(verifyPeersOnTracker) {
			dataMapToStore = dataMapUnverified;
		} else {
			dataMapToStore = dataMap;
		}
		return add(key, peerAddress, dataMapToStore, attachement);
	}

	private Pair<PeerStatistic, Data> findOld(Number320 key, PeerAddress peerAddress, Map<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> dataMap) {
		
		final Map<PeerAddress, Pair<PeerStatistic, Data>> map = dataMap.get(key);
		if(map == null) {
			return null;
		}
		
		final Pair<PeerStatistic, Data> pair = map.get(peerAddress);
		if(pair == null) {
			return null;
		}
		
		return pair;
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
		for (Map<PeerAddress, Pair<PeerStatistic, Data>> map2 : dataMapUnverified.values()) {
			for (Pair<PeerStatistic, Data> pair : map2.values()) {
				if (DefaultMaintenance.needMaintenance(pair.element0(), intervalSeconds)) {
					return pair.element0();
				}
			}
		}
		return null;
	}

	@Override
	public void peerInserted(PeerAddress remotePeer, boolean verified) {
		if (verified) {
			for (Map.Entry<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> entry : dataMap.entrySet()) {
				//if I have conetnt and I see a peer as a new responsible, push it.
				if(isInReplicationRange(entry.getKey().locationKey(), remotePeer, replicationFactor)) {
					//limit the pushing peer to those that are responsible
					if(isInReplicationRange(entry.getKey().locationKey(), self, replicationFactor)) {
						TrackerData trackerData = new TrackerData(entry.getValue().values());
						peerExchange.peerExchange(remotePeer, entry.getKey(), trackerData);
					}
				}
			}
		}
	}

	@Override
	public void peerRemoved(PeerAddress remotePeer, PeerStatistic storedPeerAddress) {
		// if a responsible peer is removed, and I see myself as a responsible, 
		// I should push my content to a random responsible
		for (Map.Entry<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> entry : dataMap.entrySet()) {
			//if I have conetnt and I see the removed peer as a responsible, push it.
			if(isInReplicationRange(entry.getKey().locationKey(), remotePeer, replicationFactor)) {
				//limit the pushing peer to those that are responsible
				if(isInReplicationRange(entry.getKey().locationKey(), self, replicationFactor)) {
					NavigableSet<PeerAddress> closePeers = peerMap.closePeers(entry.getKey().locationKey(), replicationFactor);
					PeerAddress newResponsible = closePeers.headSet(remotePeer).last();
					TrackerData trackerData = new TrackerData(entry.getValue().values());
					peerExchange.peerExchange(newResponsible, entry.getKey(), trackerData);
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

	private boolean add(Number320 key, PeerAddress peerAddress, Map<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> map, Data attachement) {
		
		//check size
		Map<PeerAddress, Pair<PeerStatistic, Data>> map2 = map.get(key);
		if(map2!=null && map2.size() > TRACKER_CACHE_SIZE) {
			return false;
		}
		
		Pair<PeerStatistic, Data> trackerData = findOld(key, peerAddress, map);
		if (trackerData == null) {
			trackerData = new Pair<PeerStatistic, Data>(new PeerStatistic(peerAddress), attachement);
		}
		
		if(map2 == null) {
			map2 = new ConcurrentCacheMap<PeerAddress, Pair<PeerStatistic, Data>>(trackerTimoutSeconds, TRACKER_CACHE_SIZE, true);
			map.put(key, map2);
		}
		map2.put(peerAddress, trackerData);
		
		return true;
	}

	public Collection<Number320> keys() {
		return dataMap.keySet();
	}

	@Override
	public boolean peerFailed(PeerAddress remotePeer, PeerException reason) {
		peerOffline.put(remotePeer.peerId(), Boolean.TRUE);
		boolean removed = false;
		removed = removeFromMap(remotePeer, dataMapUnverified) != null;
		removed = removed || (removeFromMap(remotePeer, dataMap) != null);
		return removed;
	}

	private Map<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> removeFromMap(PeerAddress remotePeer, Map<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> map) {
		Map<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> removed = new HashMap<Number320, Map<PeerAddress,Pair<PeerStatistic,Data>>>();
	    for (Map.Entry<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> entry : map.entrySet()) {
	    	Pair<PeerStatistic, Data> oldPair = entry.getValue().remove(remotePeer);
	    	if(oldPair != null) {
	    		removed.put(entry.getKey(), entry.getValue());
	    	}
	    	
			if(entry.getValue().isEmpty()) {
				map.remove(entry.getKey());
				//someone added data in the meantime, but we don't care
			}
		}
	    return removed;
    }

	@Override
	public boolean peerFound(PeerAddress remotePeer, PeerAddress referrer, PeerConnection peerConnection) {
		boolean firsthand = referrer == null;
		if (firsthand) {
			peerOffline.remove(remotePeer.peerId());
			
			Map<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> removed = removeFromMap(remotePeer, dataMapUnverified);
			for (Map.Entry<Number320, Map<PeerAddress, Pair<PeerStatistic, Data>>> entry:removed.entrySet()) {
				for(Pair<PeerStatistic, Data> pair:entry.getValue().values()) {
					add(entry.getKey(), pair.element0().peerAddress(), dataMap, pair.element1());
				}
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
		Map<PeerAddress, Pair<PeerStatistic, Data>> trackerData = dataMap.get(new Number320(locationKey, domainKey));
		if(trackerData!=null) {
			if(contentKey!=null) {
				PeerAddress tmpAddress = new PeerAddress(contentKey);
				Pair<PeerStatistic, Data> pair = trackerData.get(tmpAddress);
				if(pair != null) {
					contentDigest = pair.element1().hash();
					counter = 1;
				}
			} else {
				for(Map.Entry<PeerAddress, Pair<PeerStatistic, Data>> entry: trackerData.entrySet()) {
					contentDigest = contentDigest.xor(entry.getValue().element1().hash());
					counter++;
				}
			}
		}
		return new DigestInfo(Number160.ZERO, contentKey, counter);
    }

	public Map<PeerAddress, Pair<PeerStatistic, Data>> peers(Number320 number320) {
		Map<PeerAddress, Pair<PeerStatistic, Data>> retVal = dataMap.get(number320);
		if(retVal == null) {
			return Collections.emptyMap();
		}
		return retVal;
    }
	
	public TrackerData trackerData(Number320 number320) {
		return new TrackerData(peers(number320).values());
	}
}
