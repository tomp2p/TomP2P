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
package net.tomp2p.storage;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.tomp2p.p2p.IdentityManagement;
import net.tomp2p.p2p.Maintenance;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.replication.Replication;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.utils.ExpiringMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The maintenance for the tracker is done by the client peer. Thus the peers on
 * a tracker expire, but a client can send a Bloom filter with peers, that he
 * knows are offline. TrackerStorage stores the data in memory only.
 * 
 * TODO: check availability of secondary peers and periodically check if peers
 * from the mesh are still online, right now we rely on the PeerMap mechanism
 * 
 * @author draft
 * 
 */
public class TrackerStorage implements PeerStatusListener, Digest
{
	final private static Logger logger = LoggerFactory.getLogger(TrackerStorage.class);
	final private static Map<Number160, TrackerData> EMPTY_MAP = new HashMap<Number160, TrackerData>();
	final private static DigestInfo EMPTY_DIGEST_INFO = new DigestInfo(Number160.ZERO, 0);
	// once you call listen, changing this value has no effect unless a new
	// TrackerRPC is created. The value is chosen to fit into one single UDP
	// packet. This means that the attached data must be 0, otherwise you have
	// to used tcp. don't forget to add the header as well
	final public static int TRACKER_SIZE = 35;
	// K=location and domain, V=peerId and attachment
	final private ConcurrentMap<Number320, Map<Number160, TrackerData>> trackerDataActive;
	final private ExpiringMap<Number320, Map<Number160, TrackerData>> trackerDataMesh;
	final private ExpiringMap<Number320, Map<Number160, TrackerData>> trackerDataSecondary;
	// for timeouts we need to know which peer stores what data to remove it
	// from the primary and secondary tracker
	// K=peerId of the offline peer, V=location and domain
	final private ExpiringMap<Number160, Collection<Number320>> reverseTrackerDataMesh;
	final private ExpiringMap<Number160, Collection<Number320>> reverseTrackerDataSecondary;
	// K=peerId of the offline peer, V=reporter
	final private ExpiringMap<Number160, Collection<Number160>> peerOffline;
	final private IdentityManagement identityManagement;
	final private int trackerTimoutSeconds;
	final private Replication replication;
	final private Maintenance maintenance;
	// variable parameters
	private boolean fillPrimaryStorageFast = false;
	private int secondaryFactor = 5;
	private int primanyFactor = 1;

	public enum ReferrerType
	{
		ACTIVE, MESH
	};

	public TrackerStorage(IdentityManagement identityManagement, int trackerTimoutSeconds, Replication replication,
			Maintenance maintenance)
	{
		this.trackerTimoutSeconds = trackerTimoutSeconds;
		this.identityManagement = identityManagement;
		this.replication = replication;
		this.maintenance = maintenance;
		trackerDataActive = new ConcurrentHashMap<Number320, Map<Number160,TrackerData>>();
		trackerDataMesh = new ExpiringMap<Number320, Map<Number160,TrackerData>>(trackerTimoutSeconds);
		trackerDataMesh.getExpirer().startExpiring();
		trackerDataSecondary = new ExpiringMap<Number320, Map<Number160,TrackerData>>(trackerTimoutSeconds);
		trackerDataSecondary.getExpirer().startExpiring();
		//
		reverseTrackerDataMesh = new ExpiringMap<Number160, Collection<Number320>>(trackerTimoutSeconds);
		reverseTrackerDataMesh.getExpirer().startExpiring();
		reverseTrackerDataSecondary = new ExpiringMap<Number160, Collection<Number320>>(trackerTimoutSeconds);
		reverseTrackerDataSecondary.getExpirer().startExpiring();
				
		// if everything is perfect, a factor of 2 is enough, to be on the safe
		// side factor 5 is used.
		peerOffline = new ExpiringMap<Number160, Collection<Number160>>(trackerTimoutSeconds * 5);
		peerOffline.getExpirer().startExpiring();
	}
	
	public void shutdown()
	{
		trackerDataMesh.getExpirer().stopExpiring();
		trackerDataSecondary.getExpirer().stopExpiring();
		reverseTrackerDataMesh.getExpirer().stopExpiring();
		reverseTrackerDataSecondary.getExpirer().stopExpiring();
		peerOffline.getExpirer().stopExpiring();
	}

	public Map<Number160, TrackerData> activePeers(Number160 locationKey, Number160 domainKey)
	{
		Number320 keys = new Number320(locationKey, domainKey);
		Map<Number160, TrackerData> data = trackerDataActive.get(keys);
		if (data == null)
			return EMPTY_MAP;
		// return a copy
		synchronized (data)
		{
			return new HashMap<Number160, TrackerData>(data);
		}

	}

	public Map<Number160, TrackerData> meshPeers(Number160 locationKey, Number160 domainKey)
	{
		Number320 keys = new Number320(locationKey, domainKey);
		Map<Number160, TrackerData> data = trackerDataMesh.get(keys);
		if (data == null)
			return EMPTY_MAP;
		// return a copy
		synchronized (data)
		{
			return new HashMap<Number160, TrackerData>(data);
		}

	}

	public Map<Number160, TrackerData> secondaryPeers(Number160 locationKey, Number160 domainKey)
	{
		Number320 keys = new Number320(locationKey, domainKey);
		Map<Number160, TrackerData> data = trackerDataSecondary.get(keys);
		if (data == null)
			return EMPTY_MAP;
		// return a copy
		synchronized (data)
		{
			return new HashMap<Number160, TrackerData>(data);
		}
	}

	public void addActive(Number160 locationKey, Number160 domainKey, PeerAddress remotePeer, byte[] attachement,
			int offset, int length)
	{
		Number320 key = new Number320(locationKey, domainKey);
		Map<Number160, TrackerData> data = new HashMap<Number160, TrackerData>();
		Map<Number160, TrackerData> data2 = trackerDataActive.putIfAbsent(key, data);
		data = data2 == null ? data : data2;
		// we don't expect many concurrent access for data
		synchronized (data)
		{
			data.put(remotePeer.getID(), new TrackerData(remotePeer, identityManagement.getPeerAddress(), attachement,
					offset, length));
		}
	}

	public boolean removeActive(Number160 locationKey, Number160 domainKey, Number160 remotePeerId)
	{
		Number320 key = new Number320(locationKey, domainKey);
		Map<Number160, TrackerData> data = trackerDataActive.get(key);
		if (data == null)
			return false;
		TrackerData retVal;
		// we don't expect many concurrent access for data
		synchronized (data)
		{
			retVal = data.remove(remotePeerId);
			if (data.size() == 0)
			{
				trackerDataActive.remove(key);
			}
		}
		return retVal != null;
	}

	public boolean put(Number160 locationKey, Number160 domainKey, PeerAddress peerAddress, PublicKey publicKey,
			byte[] attachement)
	{
		if (attachement == null)
		{
			return put(locationKey, domainKey, peerAddress, publicKey, null, 0, 0);
		}
		else
		{
			return put(locationKey, domainKey, peerAddress, publicKey, attachement, 0, attachement.length);
		}
	}

	public boolean put(Number160 locationKey, Number160 domainKey, PeerAddress peerAddress, PublicKey publicKey,
			byte[] attachement, int offset, int length)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("try to store on tracker " + locationKey);
		}
		Number160 peerId = peerAddress.getID();
		// check if this guy is offline
		if (isOffline(peerAddress))
			return false;
		// check identity
		if (!identityManagement.checkIdentity(peerId, publicKey))
			return false;

		// store the data
		if (canStorePrimary(locationKey, domainKey, false))
		{
			// we have space in our primary tracker, store them there!
			Number320 key = new Number320(locationKey, domainKey);
			if (storeData(peerAddress, attachement, offset, length, peerId, key, trackerDataMesh,
					reverseTrackerDataMesh, getPrimanyFactor()))
			{
				replication.checkResponsibility(locationKey);
				return true;
			}
		}
		// do not store in the secondary map, since this is used for PEX, for
		// unknown peers.
		return false;
	}

	private boolean isOffline(PeerAddress peerAddress)
	{
		// TODO: always trust myself, do a majority voting for others
		if (peerOffline.containsKey(peerAddress.getID()))
			return true;
		return false;
	}

	public boolean putReferred(Number160 locationKey, Number160 domainKey, PeerAddress peerAddress,
			PeerAddress referrer, byte[] attachement, int offset, int length, ReferrerType type)
	{
		Number160 peerId = peerAddress.getID();
		// we cannot do public key check, because these data is referenced from
		// other peers and we don't know about the timeouts as well
		// store the data
		if (canStoreSecondary(locationKey, domainKey))
		{
			// maybe we have space in our secondary tracker, store them there!
			Number320 key = new Number320(locationKey, domainKey);
			if (storeData(peerAddress, attachement, offset, length, peerId, key, trackerDataSecondary,
					reverseTrackerDataSecondary, getSecondaryFactor()))
			{
				if(ReferrerType.MESH == type) {
					if(!isSecondaryTracker(locationKey, domainKey)) {
						maintenance.addTrackerMaintenance(peerAddress, referrer, locationKey, domainKey, this);
					}
				}
				return true;
			}
		}
		return false;
	}

	public boolean moveFromSecondaryToMesh(PeerAddress peerAddress, PeerAddress referrer, Number160 locationKey, Number160 domainKey, PublicKey publicKey)
	{
		Number320 key = new Number320(locationKey, domainKey);
		Map<Number160, TrackerData> map = trackerDataSecondary.get(key);
		if (map == null)
			return false;
		synchronized (map)
		{
			TrackerData data = map.remove(peerAddress.getID());
			if(data!=null)
			{
				if(!put(locationKey, domainKey, data.getPeerAddress(), publicKey, data.getAttachement(), data.getOffset(),
						data.getLength())) {
					map.put(peerAddress.getID(), data);
				}
			}
		}

		return true;
	}

	private boolean storeData(PeerAddress peerAddress, byte[] attachement, int offset, int length, Number160 peerId,
			Number320 key, ConcurrentMap<Number320, Map<Number160, TrackerData>> trackerData,
			ConcurrentMap<Number160, Collection<Number320>> reverseTrackerData, int factor)
	{
		Map<Number160, TrackerData> data = new HashMap<Number160, TrackerData>();
		Map<Number160, TrackerData> data2 = trackerData.putIfAbsent(key, data);
		data = data2 == null ? data : data2;
		// we don't expect much concurrency with data and data2 so we use
		// locking
		synchronized (data)
		{
			if (data.size() > TRACKER_SIZE * factor)
				return false;
			data.put(peerId, new TrackerData(peerAddress, null, attachement, offset, length));
		}
		// now store the reverse data to find all the data one peer stored
		Collection<Number320> collection = new HashSet<Number320>();
		Collection<Number320> collection2 = reverseTrackerData.putIfAbsent(peerId, collection);
		collection = collection2 == null ? collection : collection2;
		// we don't expect much concurrency with collection and collection2 so
		// we use locking
		synchronized (collection)
		{
			collection.add(key);
		}
		return true;
	}

	private boolean canStorePrimary(Number160 locationKey, Number160 domainKey, boolean referred)
	{
		if (!referred || isFillPrimaryStorageFast())
		{
			return sizePrimary(locationKey, domainKey) <= (TRACKER_SIZE * getPrimanyFactor());
		}
		else
		{
			return false;
		}
	}

	private boolean canStoreSecondary(Number160 locationKey, Number160 domainKey)
	{
		return sizeSecondary(locationKey, domainKey) <= (TRACKER_SIZE * getSecondaryFactor());
	}

	public int sizePrimary(Number160 locationKey, Number160 domainKey)
	{
		return size(locationKey, domainKey, trackerDataMesh);
	}

	public int sizeSecondary(Number160 locationKey, Number160 domainKey)
	{
		return size(locationKey, domainKey, trackerDataSecondary);
	}

	private int size(Number160 locationKey, Number160 domainKey,
			ConcurrentMap<Number320, Map<Number160, TrackerData>> trackerData)
	{
		Number320 key = new Number320(locationKey, domainKey);
		Map<Number160, TrackerData> data = trackerData.get(key);
		if (data == null)
		{
			return 0;
		}
		else
		{
			synchronized (data)
			{
				return data.size();
			}
		}
	}

	public void setSecondaryFactor(int secondaryFactor)
	{
		this.secondaryFactor = secondaryFactor;
	}

	public int getSecondaryFactor()
	{
		return secondaryFactor;
	}

	public void setPrimanyFactor(int primanyFactor)
	{
		this.primanyFactor = primanyFactor;
	}

	public int getPrimanyFactor()
	{
		return primanyFactor;
	}

	@Override
	public void peerOffline(PeerAddress peerAddress, Reason reason)
	{
		if (reason == Reason.NOT_REACHABLE)
			peerOffline(peerAddress.getID(), identityManagement.getSelf());
	}

	private void peerOffline(Number160 peerId, Number160 referrerId)
	{
		indicateOffline(peerId, referrerId);
		remove(peerId, trackerDataMesh, reverseTrackerDataMesh);
		remove(peerId, trackerDataSecondary, reverseTrackerDataSecondary);
	}

	private void indicateOffline(Number160 peerId, Number160 referrerId)
	{
		Collection<Number160> collection = new HashSet<Number160>();
		Collection<Number160> collection2 = peerOffline.putIfAbsent(peerId, collection);
		collection = collection2 == null ? collection : collection2;
		collection.add(referrerId);
	}

	private boolean remove(Number160 peerId, ConcurrentMap<Number320, Map<Number160, TrackerData>> trackerData,
			ConcurrentMap<Number160, Collection<Number320>> reverseTrackerData)
	{
		boolean retVal = false;
		Collection<Number320> collection = reverseTrackerData.remove(peerId);
		if (collection == null)
			return false;
		synchronized (collection)
		{
			for (Number320 key : collection)
			{
				Map<Number160, TrackerData> data = trackerData.get(key);
				if (data == null)
					continue;
				synchronized (data)
				{
					if (data.remove(peerId) != null)
						retVal = true;
					if (data.size() == 0)
						trackerData.remove(key);
				}
			}
		}
		return retVal;
	}

	@Override
	public void peerFail(PeerAddress peerAddress, boolean force)
	{
		// not interested in this one
	}

	@Override
	public void peerOnline(PeerAddress peerAddress)
	{
		peerOffline.remove(peerAddress.getID());
	}

	@Override
	public DigestInfo digest(Number320 key)
	{
		Map<Number160, TrackerData> data = trackerDataMesh.get(key);
		if (data == null)
		{
			return EMPTY_DIGEST_INFO;
		}
		synchronized (data)
		{
			DigestInfo digestInfo = new DigestInfo();
			for (Number160 tmpKey : data.keySet())
			{
				digestInfo.getKeyDigests().add(tmpKey);	
			}
			return digestInfo;
		}
	}

	@Override
	public DigestInfo digest(Number320 key, Collection<Number160> contentKeys)
	{
		if (contentKeys == null)
		{
			return digest(key);
		}
		Map<Number160, TrackerData> data = trackerDataMesh.get(key);
		if (data == null)
			return EMPTY_DIGEST_INFO;
		synchronized (data)
		{
			DigestInfo digestInfo = new DigestInfo();
			for (Number160 tmpKey : contentKeys)
			{
				if (data.containsKey(tmpKey))
				{
					digestInfo.getKeyDigests().add(tmpKey);
				}
			}
			return digestInfo;
		}
	}

	public void removeReferred(Number160 locationKey, Number160 domainKey, Number160 key, PeerAddress referrer)
	{
		indicateOffline(key, referrer.getID());
	}

	public void setFillPrimaryStorageFast(boolean fillPrimaryStorageFast)
	{
		this.fillPrimaryStorageFast = fillPrimaryStorageFast;
	}

	public boolean isFillPrimaryStorageFast()
	{
		return fillPrimaryStorageFast;
	}

	public int getTrackerTimoutSeconds()
	{
		return trackerTimoutSeconds;
	}

	/**
	 * A peer is a secondary tracker if the peers stores itself on the tracker
	 * as well. The primary trackers do not behave like this.
	 * 
	 * @param locationKey
	 * @param domainKey
	 * @return
	 */
	public boolean isSecondaryTracker(Number160 locationKey, Number160 domainKey)
	{
		Number320 keys = new Number320(locationKey, domainKey);
		Map<Number160, TrackerData> data = trackerDataMesh.get(keys);
		if (data == null)
			return false;
		// return a copy
		synchronized (data)
		{
			return data.containsKey(identityManagement.getSelf());
		}
	}

	// TODO: seems a bit inefficient, but it works for the moment
	public Collection<Number160> responsibleDomains(Number160 locationKey)
	{
		Collection<Number160> retVal = new ArrayList<Number160>();
		for (Number320 number320 : trackerDataMesh.keySet())
		{
			if (number320.getLocationKey().equals(locationKey))
			{
				retVal.add(number320.getDomainKey());
			}
		}
		return retVal;
	}
}
