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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import net.tomp2p.peers.Number160;

import com.google.common.collect.MapMaker;

public class ResponsibilityMemory implements Responsibility
{
	// maps content (locationKey) to peerid
	final private ConcurrentMap<Number160, Number160> responsibilityMap;
	// maps peerid to content (locationKey)
	final private ConcurrentMap<Number160, Set<Number160>> responsibilityMapRev;

	public ResponsibilityMemory()
	{
		responsibilityMap = new MapMaker().makeMap();
		responsibilityMapRev = new MapMaker().makeMap();
	}

	/** {@inheritDoc} */
	@Override
	public Number160 findPeerIDForResponsibleContent(Number160 locationKey)
	{
		return responsibilityMap.get(locationKey);
	}

	/** {@inheritDoc} */
	@Override
	public Collection<Number160> findContentForResponsiblePeerID(Number160 peerID)
	{
		Collection<Number160> result = responsibilityMapRev.get(peerID);
		if (result == null)
		{
			return Collections.<Number160> emptyList();
		}
		else
		{
			synchronized (result)
			{
				return new ArrayList<Number160>(result);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean updateResponsibilities(Number160 locationKey, Number160 peerId)
	{
		boolean isNew = true;
		Number160 old = responsibilityMap.put(locationKey, peerId);
		if (old != null)
		{
			isNew = !old.equals(peerId);
			// clean up reverse map
			Set<Number160> tmp = responsibilityMapRev.get(old);
			if (tmp != null)
			{
				boolean remove = false;
				synchronized (tmp)
				{
					tmp.remove(locationKey);
					if (tmp.isEmpty())
						remove = true;
				}
				if (remove)
					responsibilityMapRev.remove(old);
			}
		}
		// add to the reverse map
		Set<Number160> tmp1 = new HashSet<Number160>();
		Set<Number160> tmp2 = responsibilityMapRev.putIfAbsent(peerId, tmp1);
		tmp1 = tmp2 == null ? tmp1 : tmp2;
		synchronized (tmp1)
		{
			tmp1.add(locationKey);
		}
		return isNew;
	}

	/** {@inheritDoc} */
	@Override
	public void removeResponsibility(Number160 locationKey)
	{
		Number160 tmp = responsibilityMap.remove(locationKey);
		if (tmp != null)
		{
			Set<Number160> tmp2 = responsibilityMapRev.get(tmp);
			if (tmp2 != null)
			{
				boolean remove = false;
				synchronized (tmp2)
				{
					tmp2.remove(locationKey);
					if (tmp2.isEmpty())
					{
						remove = true;
					}
				}
				// have this outside of the sync
				if (remove)
				{
					responsibilityMapRev.remove(tmp);
				}
			}
		}
	}
}
