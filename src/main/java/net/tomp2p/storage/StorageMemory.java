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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.rpc.DigestInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageMemory extends Storage
{
	final private static Logger logger = LoggerFactory.getLogger(StorageMemory.class);
	// these data need to be consistent
	final private Object lock = new Object();
	final protected SortedMap<Number480, Data> dataMap = new TreeMap<Number480, Data>();
	final protected Set<Number480> dataDirectReplication = new HashSet<Number480>();
	final private Map<Number320, PublicKey> protectedMap = new HashMap<Number320, PublicKey>();
	//final protected Map<Number160, Number160> responsibilityMap = new HashMap<Number160, Number160>();
	//final protected Map<Number160, Set<Number160>> responsibilityMapRev = new HashMap<Number160, Set<Number160>>();
	final private Map<Number480, Long> timeoutMap = new HashMap<Number480, Long>();
	final private SortedMap<Long, Set<Number480>> timeoutMapRev = new TreeMap<Long, Set<Number480>>();
	final private Responsibility responsibilityMemory = new ResponsibilityMemory();

	@Override
	public void close()
	{
		dataMap.clear();
		dataDirectReplication.clear();
		protectedMap.clear();
		timeoutMap.clear();
		timeoutMapRev.clear();
	}

	@Override
	public boolean put(Number480 key, Data newData, PublicKey publicKey, boolean putIfAbsent, boolean domainProtection)
	{
		synchronized (lock)
		{
			checkTimeout();
			if (!securityDomainCheck(key, publicKey, domainProtection))
				return false;
			boolean contains = dataMap.containsKey(key);
			if (putIfAbsent && contains)
				return false;
			if (contains)
			{
				Data oldData = dataMap.get(key);
				boolean protectEntry = newData.isProtectedEntry();
				if (!canUpdateEntry(key, oldData, newData, protectEntry))
					return false;
			}
			dataMap.put(key, newData);
			long exp = newData.getExpirationMillis();
			// handle timeout
			addTimeout(key, exp);
		}
		return true;
	}

	private void addTimeout(Number480 key, long exp)
	{
		Long old = timeoutMap.put(key, exp);
		if (old != null)
		{
			Set<Number480> tmp = timeoutMapRev.get(old);
			if (tmp != null)
			{
				tmp.remove(key);
			}
		}
		Set<Number480> tmp = timeoutMapRev.get(exp);
		if (tmp == null)
		{
			tmp = new HashSet<Number480>();
			timeoutMapRev.put(exp, tmp);
		}
		tmp.add(key);
	}

	private void removeTimeout(Number480 key)
	{
		Long tmp = timeoutMap.remove(key);
		if (tmp != null)
		{
			Set<Number480> tmp2 = timeoutMapRev.get(tmp);
			if (tmp2 != null)
			{
				tmp2.remove(key);
				if (tmp2.isEmpty())
					timeoutMapRev.remove(tmp);
			}
		}
	}

	private boolean securityDomainCheck(Number480 key, PublicKey publicKey, boolean domainProtection)
	{
		Number320 partKey = new Number320(key.getLocationKey(), key.getDomainKey());
		boolean domainProtectedByOthers = isDomainProtectedByOthers(partKey, publicKey);
		if (!domainProtection && !domainProtectedByOthers)
			return true;
		else if (domainProtection)
		{
			if (!domainProtectedByOthers
					|| (getProtectionDomainMode() == ProtectionMode.MASTER_PUBLIC_KEY && foreceOverrideDomain(
							key.getDomainKey(), publicKey)))
			{
				if (canProtectDomain(partKey, publicKey))
					return protectDomain(partKey, publicKey);
			}
		}
		return false;
	}

	private boolean isDomainProtectedByOthers(Number320 partKey, PublicKey publicKey)
	{
		PublicKey other = protectedMap.get(partKey);
		if (other == null)
			return false;
		return !publicKey.equals(other);
	}

	private boolean protectDomain(Number320 partKey, PublicKey publicKey)
	{
		// if (!protectedMap.containsKey(partKey))
		// {
		if (getProtectionEntryInDomain() == ProtectionEntryInDomain.ENTRY_REMOVE_IF_DOMAIN_CLAIMED)
			remove(partKey.min(), partKey.max(), publicKey);
		protectedMap.put(partKey, publicKey);
		return true;
		// }
		// else
		// or else check if already protected
		// return protectedMap.get(partKey).equals(publicKey);
	}

	@Override
	public Data get(Number480 key)
	{
		synchronized (lock)
		{
			checkTimeout();
			return dataMap.get(key);
		}
	}

	public List<Number480> getKeys(Number320 key)
	{
		return getKeys(key.min(), key.max());
	}

	public List<Number480> getKeys(Number480 fromKey, Number480 toKey)
	{
		synchronized (lock)
		{
			checkTimeout();
			if (fromKey == null && toKey == null)
				return null;
			else if (toKey == null)
				// make copy, otherwise we see concurrent modification
				// excteption
				return new ArrayList<Number480>(dataMap.tailMap(fromKey).keySet());
			else if (fromKey == null)
				// make copy, otherwise we see concurrent modification
				// excteption
				return new ArrayList<Number480>(dataMap.headMap(toKey).keySet());
			else
				// make copy, otherwise we see concurrent modification
				// excteption
				return new ArrayList<Number480>(dataMap.subMap(fromKey, toKey).keySet());
		}
	}

	@Override
	public SortedMap<Number480, Data> get(Number480 fromKey, Number480 toKey)
	{
		synchronized (lock)
		{
			checkTimeout();
			if (fromKey == null && toKey == null)
				return null;
			else if (toKey == null)
				// make copy, otherwise we see concurrent modification
				// excteption
				return new TreeMap<Number480, Data>(dataMap.tailMap(fromKey));
			else if (fromKey == null)
				// make copy, otherwise we see concurrent modification
				// excteption
				return new TreeMap<Number480, Data>(dataMap.headMap(toKey));
			else
				// make copy, otherwise we see concurrent modification
				// excteption
				return new TreeMap<Number480, Data>(dataMap.subMap(fromKey, toKey));
		}
	}

	@Override
	public Data remove(Number480 key, PublicKey publicKey)
	{
		synchronized (lock)
		{
			checkTimeout();
			return remove(key, publicKey, false);
		}
	}

	private Data remove(Number480 key, PublicKey publicKey, boolean force)
	{
		if (!force && isDomainProtectedByOthers(new Number320(key.getLocationKey(), key.getDomainKey()), publicKey))
			return null;
		Data data = dataMap.get(key);
		if (data != null)
		{
			if (force || data.getPublicKey() == null || data.getPublicKey().equals(publicKey))
			{
				removeTimeout(key);
				removeResponsibility(key.getLocationKey());
				return dataMap.remove(key);
			}
		}
		return null;
	}

	@Override
	public SortedMap<Number480, Data> remove(Number480 fromKey, Number480 toKey, PublicKey publicKey)
	{
		synchronized (lock)
		{
			checkTimeout();
			// we remove only if locationkey and domain key are the same
			if (!fromKey.getLocationKey().equals(toKey.getLocationKey())
					|| !fromKey.getDomainKey().equals(toKey.getDomainKey()))
				return null;
			boolean domainProtectedByOthers = isDomainProtectedByOthers(
					new Number320(fromKey.getLocationKey(), fromKey.getDomainKey()), publicKey);
			boolean cont = (!domainProtectedByOthers || (getProtectionDomainMode() == ProtectionMode.MASTER_PUBLIC_KEY && foreceOverrideDomain(
					fromKey.getDomainKey(), publicKey)));
			if (!cont)
				return null;
			SortedMap<Number480, Data> tmp = dataMap.subMap(fromKey, toKey);
			Collection<Number480> keys = new ArrayList<Number480>(tmp.keySet());
			SortedMap<Number480, Data> result = new TreeMap<Number480, Data>();
			for (Number480 key : keys)
			{
				Data data = dataMap.get(key);
				if (data.getPublicKey() == null || data.getPublicKey().equals(publicKey))
				{
					removeTimeout(key);
					removeResponsibility(key.getLocationKey());
					result.put(key, dataMap.remove(key));
				}
			}
			return result;
		}
	}

	@Override
	public boolean contains(Number480 key)
	{
		synchronized (lock)
		{
			checkTimeout();
			return dataMap.containsKey(key);
		}
	}

	@Override
	public DigestInfo digest(Number320 key)
	{
		synchronized (lock)
		{
			checkTimeout();
			SortedMap<Number480, Data> tmp = get(key);
			Number160 hash = Number160.ZERO;
			for (Number480 key2 : tmp.keySet())
				hash = hash.xor(key2.getContentKey());
			return new DigestInfo(hash, tmp.size());
		}
	}

	@Override
	public DigestInfo digest(Number320 key, Collection<Number160> contentKeys)
	{
		if (contentKeys == null)
			return digest(key);
		synchronized (lock)
		{
			checkTimeout();
			SortedMap<Number480, Data> tmp = get(key);
			Number160 hash = Number160.ZERO;
			int size = 0;
			for (Number480 key2 : tmp.keySet())
			{
				if (contentKeys.contains(key2.getContentKey()))
				{
					hash = hash.xor(key2.getContentKey());
					size++;
				}
			}
			return new DigestInfo(hash, size);
		}
	}

	@Override
	public void iterateAndRun(Number160 locationKey, StorageRunner runner)
	{
		Number480 min = new Number480(locationKey, Number160.ZERO, Number160.ZERO);
		Number480 max = new Number480(locationKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
		synchronized (lock)
		{
			checkTimeout();
			for (Map.Entry<Number480, Data> entry : dataMap.subMap(min, max).entrySet())
			{
				runner.call(entry.getKey().getLocationKey(), entry.getKey().getDomainKey(), entry.getKey()
						.getContentKey(), entry.getValue());
			}
		}
	}

	@Override
	public Collection<Number160> findContentForResponsiblePeerID(Number160 peerID)
	{
		return responsibilityMemory.findContentForResponsiblePeerID(peerID);
	}

	@Override
	public Number160 findPeerIDForResponsibleContent(Number160 locationKey)
	{
		return responsibilityMemory.findPeerIDForResponsibleContent(locationKey);
	}

	@Override
	public boolean updateResponsibilities(Number160 locationKey, Number160 peerId)
	{
		return responsibilityMemory.updateResponsibilities(locationKey, peerId);
	}

	@Override
	public Collection<Number480> storedDirectReplication()
	{
		// since we are memory based, we do not need to give anything back, as
		// this is called on startup
		return new ArrayList<Number480>(0);
	}

	@Override
	public void removeResponsibility(Number160 locationKey)
	{
		responsibilityMemory.removeResponsibility(locationKey);
	}

	// TODO: make check timeout time based in a thread, but for now its ok.
	private Collection<Number480> checkTimeout()
	{
		long time = System.currentTimeMillis();
		List<Number480> toRemove = new ArrayList<Number480>();
		for (Map.Entry<Long, Set<Number480>> entry : timeoutMapRev.subMap(0L, time).entrySet())
		{
			toRemove.addAll(entry.getValue());
		}
		if (toRemove.size() > 0)
		{
			for (Number480 key : toRemove)
			{
				logger.debug("Remove key " + key + " due to expiration");
				remove(key, null, true);
			}
		}
		return toRemove;
	}
}
