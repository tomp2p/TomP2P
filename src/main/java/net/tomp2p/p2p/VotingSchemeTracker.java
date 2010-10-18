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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;


public class VotingSchemeTracker implements EvaluatingSchemeTracker
{
	@Override
	public Map<PeerAddress, Data> evaluateSingle(Map<PeerAddress, Map<PeerAddress, Data>> rawData)
	{
		if (rawData == null)
			throw new IllegalArgumentException("cannot evaluate, as no result provided");
		Map<Number160, Integer> counter = new HashMap<Number160, Integer>();
		Map<PeerAddress, Data> result = new HashMap<PeerAddress, Data>();
		for (Entry<PeerAddress, Map<PeerAddress, Data>> entry1 : rawData.entrySet())
		{
			for (Entry<PeerAddress, Data> entry2 : entry1.getValue().entrySet())
			{
				int majority = (entry1.getValue().size() + 1) / 2;
				Data dat = entry2.getValue();
				int c = 1;
				Integer count = counter.get(dat.getHash());
				if (count != null)
					c = count + 1;
				counter.put(dat.getHash(), c);
				if (c >= majority)
					result.put(entry1.getKey(), dat);
			}
		}
		return result;
	}

	@Override
	public Map<PeerAddress, Set<Data>> evaluate(Map<PeerAddress, Map<PeerAddress, Data>> rawData)
	{
		if (rawData == null)
			throw new IllegalArgumentException("cannot evaluate, as no result provided");
		Map<PeerAddress, Set<Data>> result = new HashMap<PeerAddress, Set<Data>>();
		for (Entry<PeerAddress, Map<PeerAddress, Data>> tmp : rawData.entrySet())
		{
			Set<Data> set = result.get(tmp.getKey());
			if (set == null)
			{
				set = new HashSet<Data>();
				result.put(tmp.getKey(), set);
			}
			set.addAll(tmp.getValue().values());
		}
		return result;
	}
}
