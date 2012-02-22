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
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.rpc.HashData;
import net.tomp2p.utils.Utils;

public abstract class StorageGeneric implements Storage
{
	public enum ProtectionEnable
	{
		ALL, NONE
	};
	public enum ProtectionMode
	{
		NO_MASTER, MASTER_PUBLIC_KEY
	};
	// Hash of public key is always preferred
	private ProtectionMode protectionDomainMode = ProtectionMode.MASTER_PUBLIC_KEY;
	// Domains can generallay be protected
	private ProtectionEnable protectionDomainEnable = ProtectionEnable.ALL;
	// Hash of public key is always preferred
	private ProtectionMode protectionEntryMode = ProtectionMode.MASTER_PUBLIC_KEY;
	// Entries can generallay be protected
	private ProtectionEnable protectionEntryEnable = ProtectionEnable.ALL;
	// stores the domains that cannot be reserved and items can be added by
	// anyone
	private Collection<Number160> removedDomains = new HashSet<Number160>();

	public SortedMap<Number480, Data> remove(Number320 number320, PublicKey publicKey)
	{
		return remove(number320.min(), number320.max(), publicKey);
	}

	public SortedMap<Number480, Data> get(Number320 key)
	{
		return get(key.min(), key.max());
	}

	public void setProtection(ProtectionEnable protectionDomainEnable,
			ProtectionMode protectionDomainMode, ProtectionEnable protectionEntryEnable,
			ProtectionMode protectionEntryMode)
	{
		setProtectionDomainEnable(protectionDomainEnable);
		setProtectionDomainMode(protectionDomainMode);
		setProtectionEntryEnable(protectionEntryEnable);
		setProtectionEntryMode(protectionEntryMode);
	}

	public void setProtectionDomainMode(ProtectionMode protectionDomainMode)
	{
		this.protectionDomainMode = protectionDomainMode;
	}

	public ProtectionMode getProtectionDomainMode()
	{
		return protectionDomainMode;
	}

	public void setProtectionDomainEnable(ProtectionEnable protectionDomainEnable)
	{
		this.protectionDomainEnable = protectionDomainEnable;
	}

	public ProtectionEnable getProtectionDomainEnable()
	{
		return protectionDomainEnable;
	}

	public void setProtectionEntryMode(ProtectionMode protectionEntryMode)
	{
		this.protectionEntryMode = protectionEntryMode;
	}

	public ProtectionMode getProtectionEntryMode()
	{
		return protectionEntryMode;
	}

	public void setProtectionEntryEnable(ProtectionEnable protectionEntryEnable)
	{
		this.protectionEntryEnable = protectionEntryEnable;
	}

	public ProtectionEnable getProtectionEntryEnable()
	{
		return protectionEntryEnable;
	}

	public void removeDomainProtection(Number160 removeDomain)
	{
		removedDomains.add(removeDomain);
	}

	boolean isDomainRemoved(Number160 domain)
	{
		return removedDomains.contains(domain);
	}

	boolean canProtectDomain(Number320 partKey, PublicKey publicKey)
	{
		if (getProtectionDomainEnable() == ProtectionEnable.ALL)
		{
			if (!isDomainRemoved(partKey.getDomainKey()))
			{
				// ok we can protect, but we must check if another one has
				// already protect the domain
				return true;
			}
		}
		// we cannot protect, but maybe we have the rigth public key, but first
		// check if the domain is removed
		if (!isDomainRemoved(partKey.getDomainKey()))
			return foreceOverrideDomain(partKey.getDomainKey(), publicKey);
		else
			// the domain is removed, so we cannot even use the public key here
			return false;
	}

	boolean foreceOverrideDomain(Number160 domainKey, PublicKey publicKey)
	{
		// we are in public key mode
		if (getProtectionDomainMode() == ProtectionMode.MASTER_PUBLIC_KEY && publicKey != null)
		{
			// if the hash of the public key is the same as the domain, we can
			// overwrite
			return isMine(domainKey, publicKey);
		}
		return false;
	}

	boolean foreceOverrideEntry(Number160 entryKey, PublicKey publicKey)
	{
		// we are in public key mode
		if (getProtectionEntryMode() == ProtectionMode.MASTER_PUBLIC_KEY && publicKey != null)
		{
			// if the hash of the public key is the same as the domain, we can
			// overwrite
			return isMine(entryKey, publicKey);
		}
		return false;
	}

	boolean canUpdateEntry(Number480 key, Data oldData, Data newData, boolean protectEntry)
	{
		if (protectEntry)
		{
			return canProtectEntry(key, oldData, newData);
		}
		return true;
	}

	private boolean canProtectEntry(Number480 key, Data oldData, Data newData)
	{
		if (getProtectionEntryEnable() == ProtectionEnable.ALL)
		{
			if (oldData == null)
				return true;
			else if (oldData.getPublicKey() == null)
				return true;
			else
			{
				if (oldData.getPublicKey().equals(newData.getPublicKey()))
					return true;
			}
		}
		// we cannot protect, but maybe we have the rigth public key
		return foreceOverrideEntry(key.getContentKey(), newData.getPublicKey());
	}

	static boolean isMine(Number160 key, PublicKey publicKey)
	{
		return key.equals(Utils.makeSHAHash(publicKey.getEncoded()));
	}

	/**
	 * Compares and puts the data if the compare matches
	 * 
	 * @param locationKey The location key
	 * @param domainKey The domain key
	 * @param hashDataMap The map with the data and the hashes to compare to
	 * @param publicKey The public key
	 * @param partial If set to true, then partial puts are OK, otherwise all
	 *        the data needs to be absent.
	 * @param protectDomain Flag to protect domain
	 * @return The keys that have been stored
	 */
	public Collection<Number160> compareAndPut(Number160 locationKey, Number160 domainKey,
			Map<Number160, HashData> hashDataMap, PublicKey publicKey, boolean partial,
			boolean protectDomain)
	{
		Collection<Number160> retVal = new ArrayList<Number160>();
		Lock lock = acquire(locationKey, domainKey);
		try
		{
			boolean perfectMatch = true;
			for (Map.Entry<Number160, HashData> entry : hashDataMap.entrySet())
			{
				Number480 key = new Number480(locationKey, domainKey, entry.getKey());
				Data data = get(key);
				if(data == null)
				{
					perfectMatch = false;
					continue;
				}
				Number160 storedHash = data.getHash();
				if (!storedHash.equals(entry.getValue().getHash()))
				{
					perfectMatch = false;
					continue;
				}
				if (partial)
				{
					put(key, entry.getValue().getData(), publicKey, false, protectDomain);
					retVal.add(key.getContentKey());
				}
			}
			if (!partial && perfectMatch)
			{
				for (Map.Entry<Number160, HashData> entry : hashDataMap.entrySet())
				{
					Number480 key = new Number480(locationKey, domainKey, entry.getKey());
					put(key, entry.getValue().getData(), publicKey, false, protectDomain);
					retVal.add(key.getContentKey());
				}
			}
		}
		finally
		{
			release(lock);
		}
		return retVal;
	}

	public Lock acquire(Number160 locationKey)
	{
		// TODO make this more smart and lock only for location and domain
		Lock lock = new ReentrantLock();
		lock.tryLock();
		return lock;
	}
	
	public Lock acquire(Number320 locationDomainKey)
	{
		return acquire(locationDomainKey.getLocationKey(), locationDomainKey.getDomainKey());
	}

	public Lock acquire(Number160 locationKey, Number160 domainKey)
	{
		// TODO make this more smart and lock only for location and domain
		Lock lock = new ReentrantLock();
		lock.tryLock();
		return lock;
	}
	
	public Lock acquire(Number480 locationDomainContentKey)
	{
		return acquire(locationDomainContentKey.getLocationKey(), locationDomainContentKey.getDomainKey(), locationDomainContentKey.getContentKey());
	}
	
	public Lock acquire(Number160 locationKey, Number160 domainKey, Number160 contentKey)
	{
		// TODO make this more smart and lock only for location and domain
		Lock lock = new ReentrantLock();
		lock.tryLock();
		return lock;
	}
	
	public void release(Lock lock)
	{
		lock.unlock();
	}
}