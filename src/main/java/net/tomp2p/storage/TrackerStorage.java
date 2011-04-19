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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.TrackerDataResult;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The maintenance for the tracker is done by the client peer. Thus the peers on
 * a tracker expire, but a client can send a Bloom filter with peers, that he
 * knows are offline.
 * 
 * @author draft
 * 
 */
public class TrackerStorage implements PeerStatusListener, Digest
{
	final private static Logger logger = LoggerFactory.getLogger(TrackerStorage.class);
	// once you call listen, changing this value has no effect unless a new
	// TrackerRPC is created. The value is chosen to fit into one single UDP
	// packet. This means that the attached data must be 0, otherwise you have
	// to used tcp. don't forget to add the header as well
	final public static int TRACKER_SIZE = 35;
	final private Set<Number320> secondaryTracker = new HashSet<Number320>();
	final private Map<Number320, SortedMap<Number160, TrackerData>> trackerData = new HashMap<Number320, SortedMap<Number160, TrackerData>>();
	final private Map<Number160, Set<Number320>> trackerDataPerPeerPrimary = new HashMap<Number160, Set<Number320>>();
	final private Map<Number160, Set<Number320>> trackerDataPerPeerSecondary = new HashMap<Number160, Set<Number320>>();
	final private Map<Number160, Long> trackerPeerTimeout = new LinkedHashMap<Number160, Long>();
	final private Map<Number160, PublicKey> peerIdentity = new HashMap<Number160, PublicKey>();
	final private Set<Number160> peerOffline = new HashSet<Number160>();
	final private Set<Number160> peerOfflinePrimary = new HashSet<Number160>();
	final private Object lock = new Object();
	private boolean fillPrimaryStorageFast=false;
	private boolean primaryStorageOnly=false;
	private int secondaryFactor = 5;
	private int primanyFactor = 1;

	final private Random rnd;
	private volatile int trackerTimoutSeconds = Integer.MAX_VALUE;

	public TrackerStorage(long seed)
	{
		rnd = new Random(seed);
	}

	public boolean put(Number160 locationKey, Number160 domainKey, PeerAddress peerAddress, PublicKey publicKey,
			byte[] attachement)
	{
		if (attachement == null)
			return put(locationKey, domainKey, peerAddress, publicKey, attachement, 0, 0);
		else
			return put(locationKey, domainKey, peerAddress, publicKey, attachement, 0, attachement.length);
	}
	
	public Collection<Number160> knownPeers(Number160 locationKey, Number160 domainKey)
	{
		Number320 keys = new Number320(locationKey, domainKey);
		synchronized (lock)
		{
			Map<Number160, TrackerData> data = trackerData.get(keys);
			return new ArrayList<Number160>(data.keySet());
		}
	}

	public boolean put(Number160 locationKey, Number160 domainKey, PeerAddress peerAddress, PublicKey publicKey,
			byte[] attachement, int offset, int length)
	{
		Number160 peerId = peerAddress.getID();
		synchronized (lock)
		{
			// check if this guy is offline
			if (peerOffline.contains(peerAddress.getID()))
				return false;
			// check permission
			PublicKey storedIdentity = peerIdentity.get(peerId);
			if (storedIdentity != null)
			{
				if (!storedIdentity.equals(publicKey))
					return false;
			}
			else if (publicKey != null)
				peerIdentity.put(peerId, publicKey);
			// set/update timeout
			trackerPeerTimeout.remove(peerId);
			trackerPeerTimeout.put(peerId, System.currentTimeMillis() + (getTrackerTimoutSeconds() * 1000));
			// store the data
			if (!isSecondaryTracker(locationKey, domainKey) && primaryStorageOnly)
			{
				// we have space in our primary tracker, store them there!
				Number320 key = new Number320(locationKey, domainKey);
				storeData(peerAddress, attachement, offset, length, peerId, key, trackerDataPerPeerPrimary);
			}
			else if (size(locationKey, domainKey) < TRACKER_SIZE * getPrimanyFactor())
			{
				// we have space in our primary tracker, store them there!
				Number320 key = new Number320(locationKey, domainKey);
				storeData(peerAddress, attachement, offset, length, peerId, key, trackerDataPerPeerPrimary);
			}
			else
			{
				// no space, store in the extended cache
				Number320 keyAlt = new Number320(locationKey, domainKey.xor(Number160.MAX_VALUE));
				storeData(peerAddress, attachement, offset, length, peerId, keyAlt, trackerDataPerPeerSecondary);
			}
			return true;
		}
	}

	public boolean putReferred(Number160 locationKey, Number160 domainKey, PeerAddress peerAddress,
			PeerAddress referrer, byte[] attachement, int offset, int legth)
	{
		Number160 peerId = peerAddress.getID();
		synchronized (lock)
		{
			// check if this guy is offline
			if (peerOffline.contains(peerAddress.getID()))
				return false;
			PublicKey storedIdentity = peerIdentity.get(peerId);
			// we have it already stored first hand, ignore this entry!
			if (storedIdentity != null)
				return false;
			// set/update timeout
			trackerPeerTimeout.remove(peerId);
			trackerPeerTimeout.put(peerId, System.currentTimeMillis() + (getTrackerTimoutSeconds() * 1000));
			// store the data
			if (size(locationKey, domainKey) < (TRACKER_SIZE * getPrimanyFactor()) && isFillPrimaryStorageFast())
			{
				// we have space in our primary tracker, store them there!
				Number320 key = new Number320(locationKey, domainKey);
				storeData(peerAddress, attachement, offset, legth, peerId, key, trackerDataPerPeerPrimary);
			}
			else
			{
				// no space, store in the extended cache
				Number320 keyAlt = new Number320(locationKey, domainKey.xor(Number160.MAX_VALUE));
				storeData(peerAddress, attachement, offset, legth, peerId, keyAlt, trackerDataPerPeerSecondary);
			}
		}
		return true;
	}

	private void storeData(PeerAddress peerAddress, byte[] attachement, int offset, int length, Number160 peerId,
			Number320 key, Map<Number160, Set<Number320>> trackerDataPerPeer)
	{
		SortedMap<Number160, TrackerData> data = trackerData.get(key);
		if (data == null)
		{
			data = new TreeMap<Number160, TrackerData>();
			trackerData.put(key, data);
		}
		data.put(peerId, new TrackerData(peerAddress, null, attachement, offset, length));
		// store the map peer - content
		Set<Number320> keys = trackerDataPerPeer.get(key);
		if (keys == null)
		{
			keys = new HashSet<Number320>();
			trackerDataPerPeer.put(peerId, keys);
		}
		keys.add(key);
	}

	public int size(Number160 locationKey, Number160 domainKey)
	{
		Number320 key = new Number320(locationKey, domainKey);
		synchronized (lock)
		{
			Map<Number160, TrackerData> data = trackerData.get(key);
			if (data == null)
				return 0;
			else
				return data.size();
		}
	}

	public TrackerDataResult get(Number160 locationKey, Number160 domainKey)
	{
		Number320 keys = new Number320(locationKey, domainKey);
		return get(keys);
	}

	public TrackerDataResult get(Number320 keys)
	{
		synchronized (lock)
		{
			SortedMap<Number160, TrackerData> tmp = trackerData.get(keys);
			return new TrackerDataResult(tmp, false);
		}
	}

	public TrackerDataResult getSelection(Number160 locationKey, Number160 domainKey, int maxTrackers,
			Set<Number160> knownPeers)
	{
		Number320 keys = new Number320(locationKey, domainKey);
		SortedMap<Number160, TrackerData> dataCopy;
		synchronized (lock)
		{
			Map<Number160, TrackerData> data = trackerData.get(keys);
			if (data != null)
				dataCopy = new TreeMap<Number160, TrackerData>(data);
			else
				dataCopy = null;
		}

		if (knownPeers != null && dataCopy != null)
		{
			for (Iterator<Number160> i = dataCopy.keySet().iterator(); i.hasNext();)
			{
				Number160 number160 = i.next();
				if (knownPeers.contains(number160))
				{
					if (logger.isDebugEnabled())
						logger.debug("We won't deliver " + number160 + " as the peer indicates that its already known.");
					// System.err.println("We won't deliver " +
					// number480.getContentKey()
					// + " as the peer indicates that its already known.");
					i.remove();
				}
			}
		}
		Map<Number160, TrackerData> dataResult = new HashMap<Number160, TrackerData>();
		if (dataCopy == null)
			return new TrackerDataResult(dataResult, false);
		int size = dataCopy.size();
		for (int i = 0; i < maxTrackers && i < size; i++)
		{
			Entry<Number160, TrackerData> key = Utils.<Number160, TrackerData>pollRandomKey(dataCopy, rnd);
			/*if(dataResult.containsKey(key.getKey()))
				System.out.println("Wtf?? "+dataCopy);
			System.out.println("add "+key.getKey());
			System.out.println("all "+dataCopy);*/
			dataResult.put(key.getKey(), key.getValue());
		}
		/*if(size(locationKey, domainKey)==35 && dataResult.size()<35)
		{
			System.out.println("Wtf?");
			getSelection(locationKey, domainKey, maxTrackers, knownPeers);
		}*/
		return new TrackerDataResult(dataResult, !dataCopy.isEmpty());
	}

	public Collection<PeerAddress> getSelectionSecondary(Number160 locationKey, Number160 domainKey, int maxTrackers,
			Set<Number160> knownPeers)
	{
		TrackerDataResult data = getSelection(locationKey, domainKey.xor(Number160.MAX_VALUE), maxTrackers, knownPeers);
		List<PeerAddress> retVal = new ArrayList<PeerAddress>();
		for (TrackerData dat : data.getPeerDataMap().values())
			retVal.add(dat.getPeerAddress());
		return retVal;
	}

	public int getTrackerTimoutSeconds()
	{
		return trackerTimoutSeconds;
	}

	public void setTrackerTimoutSeconds(int trackerTimoutSeconds)
	{
		this.trackerTimoutSeconds = trackerTimoutSeconds;
	}

	public void addAsSecondaryTracker(Number160 locationKey, Number160 domainKey)
	{
		secondaryTracker.add(new Number320(locationKey, domainKey));
	}

	public boolean isSecondaryTracker(Number160 locationKey, Number160 domainKey)
	{
		return secondaryTracker.contains(new Number320(locationKey, domainKey));
	}

	public int getTrackerStoreSize(Number160 locationKey, Number160 domainKey)
	{
		if (isSecondaryTracker(locationKey, domainKey))
			return TRACKER_SIZE * getSecondaryFactor();
		else if(isPrimaryStorageOnly())
			return Integer.MAX_VALUE;
		else
			return TRACKER_SIZE * getPrimanyFactor();
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

	public Set<Number160> getAndClearOfflinePrimary()
	{
		Set<Number160> retVal;
		synchronized (lock)
		{
			retVal = new HashSet<Number160>(peerOfflinePrimary);
			peerOfflinePrimary.clear();
		}
		return retVal;
	}

	@Override
	public void peerOffline(PeerAddress peerAddress, Reason reason)
	{
		if(reason == Reason.NOT_REACHABLE)
			peerOffline(peerAddress.getID());
	}

	private void peerOffline(Number160 peerId)
	{
		synchronized (lock)
		{
			peerOffline.add(peerId);
			if (removeOffline(peerId, trackerDataPerPeerPrimary))
			{
				peerOfflinePrimary.add(peerId);
				System.err.println("removed "+peerId);
			}
			removeOffline(peerId, trackerDataPerPeerSecondary);
			trackerPeerTimeout.remove(peerId);
			trackerDataPerPeerPrimary.remove(peerId);
			trackerDataPerPeerSecondary.remove(peerId);
		}
	}

	private boolean removeOffline(Number160 peerId, Map<Number160, Set<Number320>> trackerDataPerPeer)
	{
		boolean retVal = false;
		Set<Number320> keys = trackerDataPerPeer.get(peerId);
		if (keys == null)
			return false;
		for (Number320 key : keys)
		{
			Map<Number160, TrackerData> data = trackerData.get(key);
			if (data == null)
				continue;
			if (data.remove(peerId) != null)
				retVal = true;
			if (data.size() == 0)
				trackerData.remove(key);
		}
		return retVal;
	}

	@Override
	public void peerFail(PeerAddress peerAddress)
	{
		// not interested in this one
	}

	@Override
	public void peerOnline(PeerAddress peerAddress)
	{
		synchronized (lock)
		{
			if(true)
				return;
			// switch from secondary storage to primary storage
			peerOffline.remove(peerAddress.getID());
			Set<Number320> keys = trackerDataPerPeerSecondary.remove(peerAddress.getID());
			if (keys == null)
				return;
			for (Number320 key : keys)
			{
				Map<Number160, TrackerData> data = trackerData.get(key);
				if (data == null)
					continue;
				TrackerData localTrackerData = data.remove(peerAddress.getID());
				if (localTrackerData == null)
					continue;
				if(data.size()==0)
					this.trackerData.remove(key);
				put(key.getLocationKey(), key.getDomainKey().xor(Number160.MAX_VALUE), peerAddress, peerIdentity.get(peerAddress.getID()),
						localTrackerData.getAttachement(), localTrackerData.getOffset(), localTrackerData.getLength());
			}
		}
	}

	@Override
	public DigestInfo digest(Number320 key)
	{
		synchronized (lock)
		{
			Map<Number160, TrackerData> data = trackerData.get(key);
			Number160 hash = Number160.ZERO;
			if (data == null)
				return new DigestInfo(hash, 0);
			for (Number160 tmpKey : data.keySet())
				hash = hash.xor(tmpKey);
			return new DigestInfo(hash, data.size());
		}
	}

	@Override
	public DigestInfo digest(Number320 key, Collection<Number160> contentKeys)
	{
		if (contentKeys == null)
			return digest(key);
		synchronized (lock)
		{
			Map<Number160, TrackerData> data = trackerData.get(key);
			Number160 hash = Number160.ZERO;
			int size = 0;
			if (data == null)
				return new DigestInfo(hash, 0);
			for (Number160 tmpKey : contentKeys)
			{
				if (data.containsKey(tmpKey))
				{
					hash = hash.xor(tmpKey);
					size++;
				}
			}
			return new DigestInfo(hash, size);
		}
	}

	@Override
	public DigestInfo digest(Number320 key, Number160 fromKey, Number160 toKey)
	{
		if (fromKey == null)
			fromKey = Number160.ZERO;
		if (toKey == null)
			toKey = Number160.MAX_VALUE;
		synchronized (lock)
		{
			SortedMap<Number160, TrackerData> data = trackerData.get(key);
			Number160 hash = Number160.ZERO;
			if (data == null)
				return new DigestInfo(hash, 0);
			SortedMap<Number160, TrackerData> dataSub = data.subMap(fromKey, toKey);
			for (Number160 tmpKey : dataSub.keySet())
				hash = hash.xor(tmpKey);
			return new DigestInfo(hash, dataSub.size());
		}
	}

	// Timer for tracker data expiration, not required for the simulation

	public void removeReferred(Number160 locationKey, Number160 domainKey, Number160 key, PeerAddress referrer)
	{
		// TODO: remove this, only for testing and simulation. We assume every
		// peer is good!!!!
		peerOffline(key);
	}

	public void setFillPrimaryStorageFast(boolean fillPrimaryStorageFast)
	{
		this.fillPrimaryStorageFast = fillPrimaryStorageFast;
	}

	public boolean isFillPrimaryStorageFast()
	{
		return fillPrimaryStorageFast;
	}

	public void setPrimaryStorageOnly(boolean primaryStorageOnly)
	{
		this.primaryStorageOnly = primaryStorageOnly;
	}

	public boolean isPrimaryStorageOnly()
	{
		return primaryStorageOnly;
	}
}
