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
import java.io.IOException;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;

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
	// once you call listen, changing this value has no effect unless a new
	// TrackerRPC is created. The value is chosen to be 1+x*(45+0)<udpLength=
	// -> x=29. This means that the attached data must be 0, otherwise you have
	// to used tcp. don't forget to add the header as well
	final private int trackerSize = 29;
	private volatile int trackerTimoutSeconds = Integer.MAX_VALUE;

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

	public Map<PeerAddress, Data> get(Number160 locationKey, Number160 domainKey)
			throws ClassNotFoundException, IOException
	{
		Map<PeerAddress, Data> result = new HashMap<PeerAddress, Data>();
		SortedMap<Number480, Data> tmp = get(new Number320(locationKey, domainKey));
		for (Data data : tmp.values())
		{
			// TODO: deserializing and serializing is a waste, cache the object
			// in Data
			TrackerData trackerData = (TrackerData) data.getObject();
			result.put(trackerData.getPeerAddress(), trackerData.getAttachement());
		}
		return result;
	}

	public boolean put(Number160 locationKey, Number160 domainKey, PeerAddress senderAddress,
			PublicKey publicKey, Data attachement) throws IOException
	{
		Data data = new Data(new TrackerData(senderAddress, attachement));
		// fixed ttl, not set by the user
		data.setTTLSeconds(getTrackerTimoutSeconds());
		return put(new Number480(locationKey, domainKey, senderAddress.getID()), data, publicKey,
				false, publicKey != null);
	}

	public int size(Number160 locationKey, Number160 domainKey)
	{
		DigestInfo digest = digest(new Number320(locationKey, domainKey));
		return digest.getSize();
	}
}
