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
package net.tomp2p.p2p;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.jboss.netty.buffer.ChannelBuffer;


public class VotingSchemeDHT implements EvaluatingSchemeDHT
{
	@Override
	public Collection<Number160> evaluate(Map<PeerAddress, Collection<Number160>> rawKeys)
	{
		if (rawKeys == null)
			throw new IllegalArgumentException("cannot evaluate, as no result provided");
		Map<Number160, Integer> counter = new HashMap<Number160, Integer>();
		Set<Number160> result = new HashSet<Number160>();
		int size = rawKeys.size();
		int majority = (size + 1) / 2;
		for (PeerAddress address : rawKeys.keySet())
		{
			Collection<Number160> keys = rawKeys.get(address);
			for (Number160 key : keys)
			{
				int c = 1;
				Integer count = counter.get(key);
				if (count != null)
					c = count + 1;
				counter.put(key, c);
				if (c >= majority)
					result.add(key);
			}
		}
		return result;
	}

	@Override
	public Map<Number160, Data> evaluate(Map<PeerAddress, Map<Number160, Data>> rawData)
	{
		if (rawData == null)
			throw new IllegalArgumentException("cannot evaluate, as no result provided");
		Map<Number160, Integer> counter = new HashMap<Number160, Integer>();
		Map<Number160, Data> result = new HashMap<Number160, Data>();
		int size = rawData.size();
		int majority = (size + 1) / 2;
		for (PeerAddress address : rawData.keySet())
		{
			Map<Number160, Data> data = rawData.get(address);
			for (Number160 contentKey : data.keySet())
			{
				Data dat = data.get(contentKey);
				Number160 hash = dat.getHash().xor(contentKey);
				int c = 1;
				Integer count = counter.get(hash);
				if (count != null)
					c = count + 1;
				counter.put(hash, c);
				if (c >= majority)
					result.put(contentKey, dat);
			}
		}
		return result;
	}

	@Override
	public Object evaluate(Map<PeerAddress, Object> rawKeys)
	{
		return evaluate0(rawKeys);
	}

	@Override
	public ChannelBuffer evaluate(Map<PeerAddress, ChannelBuffer> rawKeys)
	{
		return evaluate0(rawKeys);
	}

	private static <K> K evaluate0(Map<PeerAddress, K> raw)
	{
		if (raw == null)
			throw new IllegalArgumentException("cannot evaluate, as no result provided");
		Map<K, Integer> counter = new HashMap<K, Integer>();
		K best = null;
		int count = 0;
		for (PeerAddress address : raw.keySet())
		{
			K k = raw.get(address);
			if (k != null)
			{
				Integer c = counter.get(k);
				if (c == null)
					c = 0;
				c++;
				counter.put(k, c);
				if (c > count)
				{
					best = k;
					count = c;
				}
			}
		}
		return best;
	}
}
