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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.rpc.TrackerData;

/**
 * The maintenance for the tracker is done by the client peer. Thus the peers on
 * a tracker expire, but a client can send a Bloom filter with peers, that he
 * knows are offline.
 * 
 * @author draft
 * 
 */
public class TrackerStorage extends StorageMemory
{
	final private static Logger logger = LoggerFactory.getLogger(TrackerStorage.class);
	// once you call listen, changing this value has no effect unless a new
	// TrackerRPC is created. The value is chosen to fit into one single UDP
	// packet. This means that the attached data must be 0, otherwise you have
	// to used tcp. don't forget to add the header as well
	final private int trackerSize = 20;
	private int secondaryFactor = 5;
	private int primanyFactor = 1;
	final private Set<Number320> secondaryTracker=new HashSet<Number320>();
	private int trackerStoreSize = trackerSize;
	
	final private Random rnd;
	private volatile int trackerTimoutSeconds = Integer.MAX_VALUE;

	public TrackerStorage(long seed)
	{
		rnd = new Random(seed);
	}

	public int getTrackerSize()
	{
		return trackerSize;
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

	public boolean put(Number160 locationKey, Number160 domainKey, PublicKey publicKey, Data data)
	{
		// fixed ttl, not set by the user
		data.setTTLSeconds(getTrackerTimoutSeconds());
		Number480 number480 = new Number480(locationKey, domainKey, data.getPeerAddress().getID());
		boolean retVal = put(number480, data, publicKey, false, publicKey != null);
		//System.err.println("size22:"+get(new Number320(locationKey, domainKey)).size());
		return retVal;
	}

	public boolean putReferred(Number160 locationKey, Number160 domainKey, Data data)
	{
		data.setTTLSeconds(getTrackerTimoutSeconds());
		Number480 number480 = new Number480(locationKey, domainKey.xor(Number160.MAX_VALUE), data.getPeerAddress().getID());
		//Number480 number480 = new Number480(locationKey, domainKey, data.getPeerAddress().getID());
		return put(number480, data, null, false, false);
	}

	public int size(Number160 locationKey, Number160 domainKey)
	{
		DigestInfo digest = digest(new Number320(locationKey, domainKey));
		return digest.getSize();
	}

	public TrackerData getSelection(Number160 locationKey, Number160 domainKey, int nr,
			SimpleBloomFilter<Number160> knownPeers)
	{
		// we have a copy here, so no need to sync
		Number320 number320 = new Number320(locationKey, domainKey);
		List<Number480> keys = getKeys(number320);
		// the peer is not interested in those peers! filter them
		if (knownPeers != null)
		{
			for (Iterator<Number480> i = keys.iterator(); i.hasNext();)
			{
				Number480 number480 = i.next();
				if (knownPeers.contains(number480.getContentKey()))
				{
					if (logger.isDebugEnabled())
						logger.debug("We won't deliver " + number480.getContentKey()
								+ " as the peer indicates that its already known.");
					i.remove();
				}
			}
		}
		int size = keys.size();
		//TODO: do I need a treemap here?
		SortedMap<Number480, Data> result = new TreeMap<Number480, Data>();
		for (int i = 0; i < nr && i < size; i++)
		{
			Number480 rndKey = keys.remove(rnd.nextInt(size - i));
			result.put(rndKey, get(rndKey));
		}
		return new TrackerData(result, !keys.isEmpty());
	}
	
	public SortedSet<PeerAddress> getSelectionSecondary(Number160 locationKey, Number160 domainKey, int nr)
	{
		TrackerData data= getSelection(locationKey, domainKey.xor(Number160.MAX_VALUE), nr, null);
		SortedSet<PeerAddress> retVal=new TreeSet<PeerAddress>();
		for(Data dat:data.getPeerDataMap().values())
		{
			retVal.add(dat.getPeerAddress());
		}
		return retVal;
	}
	
	public boolean isSecondaryTracker(Number160 locationKey, Number160 domainKey)
	{
		return secondaryTracker.contains(new Number320(locationKey, domainKey));
	}

	public int getTrackerStoreSize(Number160 locationKey, Number160 domainKey)
	{
		if(isSecondaryTracker(locationKey, domainKey))
			return trackerStoreSize * getSecondaryFactor();
		else
			return trackerStoreSize * getPrimanyFactor();
	}

	public void setTrackerStoreSize(int trackerStoreSize)
	{
		this.trackerStoreSize = trackerStoreSize;
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
}
