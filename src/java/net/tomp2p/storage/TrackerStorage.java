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

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
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
	// TrackerRPC is created. The value is chosen to fit into one single UDP
	// packet. This means that the attached data must be 0, otherwise you have
	// to used tcp. don't forget to add the header as well
	final private int trackerSize = 20;
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

	public boolean put(Number160 locationKey, Number160 domainKey, PublicKey publicKey, Data data)
			throws IOException
	{
		// fixed ttl, not set by the user
		data.setTTLSeconds(getTrackerTimoutSeconds());
		Number480 number480 = new Number480(locationKey, domainKey, data.getPeerAddress().getID());
		return put(number480, data, publicKey, false, publicKey != null);
	}

	public int size(Number160 locationKey, Number160 domainKey)
	{
		DigestInfo digest = digest(new Number320(locationKey, domainKey));
		return digest.getSize();
	}
}
