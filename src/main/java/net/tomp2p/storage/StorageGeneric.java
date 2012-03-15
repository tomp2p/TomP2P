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
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.HashData;
import net.tomp2p.utils.Timings;
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
	final private Collection<Number160> removedDomains = new HashSet<Number160>();
	
	final private KeyLock<Number320> dataLock320 = new KeyLock<Number320>();
	final private KeyLock<Number480> dataLock480 = new KeyLock<Number480>();

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
		Number320 lockKey = new Number320(locationKey, domainKey);
		Lock lock = dataLock320.lock(lockKey);
		try
		{
			boolean perfectMatch = true;
			for (Map.Entry<Number160, HashData> entry : hashDataMap.entrySet())
			{
				Number480 key = new Number480(locationKey, domainKey, entry.getKey());
				Data data = get(locationKey, domainKey, entry.getKey());
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
					put(locationKey, domainKey, entry.getKey(), entry.getValue().getData(), publicKey, false, protectDomain);
					retVal.add(key.getContentKey());
				}
			}
			if (!partial && perfectMatch)
			{
				for (Map.Entry<Number160, HashData> entry : hashDataMap.entrySet())
				{
					Number480 key = new Number480(locationKey, domainKey, entry.getKey());
					put(locationKey, domainKey, entry.getKey(), entry.getValue().getData(), publicKey, false, protectDomain);
					retVal.add(key.getContentKey());
				}
			}
		}
		finally
		{
			dataLock320.unlock(lockKey, lock);
		}
		return retVal;
	}
	
	public boolean put(Number160 locationKey, Number160 domainKey, Number160 contentKey, 
			Data newData, PublicKey publicKey, boolean putIfAbsent, boolean domainProtection)
	{
		boolean retVal = false;
		Number480 lockKey = new Number480(locationKey, domainKey, contentKey);
		Lock lock = dataLock480.lock(lockKey);		
		try
		{
			if (!securityDomainCheck(locationKey, domainKey, publicKey, domainProtection))
			{
				return false;
			}
			boolean contains = contains(locationKey, domainKey, contentKey); 
			if (putIfAbsent && contains)
			{
				return false;
			}
			if (contains)
			{
				Data oldData = get(locationKey, domainKey, contentKey);
				boolean protectEntry = newData.isProtectedEntry();
				if (!canUpdateEntry(contentKey, oldData, newData, protectEntry))
				{
					return false;
				}
			}
			retVal = put(locationKey, domainKey, contentKey, newData);
			if(retVal)
			{
				long expiration = newData.getExpirationMillis();
				// handle timeout
				addTimeout(locationKey, domainKey, contentKey, expiration);
			}
		}
		finally
		{
			dataLock480.unlock(lockKey, lock);
		}
		return retVal;
	}
	
	public Data remove(Number160 locationKey, Number160 domainKey, Number160 contentKey, PublicKey publicKey)
	{
		Number480 lockKey = new Number480(locationKey, domainKey, contentKey);
		Lock lock = dataLock480.lock(lockKey);		
		try
		{
			if(!canClaimDomain(locationKey, domainKey, publicKey))
			{
				return null;
			}
			Data data = get(locationKey, domainKey, contentKey);
			if (data == null)
			{
				return null;
			}
			if (data.getPublicKey() == null || data.getPublicKey().equals(publicKey))
			{
				removeTimeout(locationKey, domainKey, contentKey);
				removeContentResponsibility(locationKey);
				return remove(locationKey, domainKey, contentKey);
			}
		}
		finally
		{
			dataLock480.unlock(lockKey, lock);
		}
		return null;
	}
	
	public SortedMap<Number480, Data> remove(Number160 locationKey, Number160 domainKey, Number160 fromContentKey, 
			Number160 toContentKey, PublicKey publicKey)
	{
		Number320 lockKey = new Number320(locationKey, domainKey);
		Lock lock = dataLock320.lock(lockKey);
		try
		{
			if(!canClaimDomain(locationKey, domainKey, publicKey))
			{
				return null;
			}
			SortedMap<Number480, Data> tmp = subMap(locationKey, domainKey, fromContentKey, toContentKey);
			Collection<Number480> keys = new ArrayList<Number480>(tmp.keySet());
			SortedMap<Number480, Data> result = new TreeMap<Number480, Data>();
			for (Number480 key : keys)
			{
				Data data = get(key.getLocationKey(), key.getDomainKey(), key.getContentKey());
				if (data.getPublicKey() == null || data.getPublicKey().equals(publicKey))
				{
					removeTimeout(key.getLocationKey(), key.getDomainKey(), key.getContentKey());
					removeContentResponsibility(key.getLocationKey());
					result.put(key, remove(key.getLocationKey(), key.getDomainKey(), key.getContentKey()));
				}
			}
			return result;
		}
		finally
		{
			dataLock320.unlock(lockKey, lock);
		}
	}
	
	public SortedMap<Number480, Data> get(Number160 locationKey, Number160 domainKey, Number160 fromContentKey, 
			Number160 toContentKey)
	{
		Number320 lockKey = new Number320(locationKey, domainKey);
		Lock lock = dataLock320.lock(lockKey);
		try
		{
			return subMap(locationKey, domainKey, fromContentKey, toContentKey);
		}
		finally
		{
			dataLock320.unlock(lockKey, lock);
		}
	}
	
	public void checkTimeout()
	{
		long time = Timings.currentTimeMillis();
		Collection<Number480> toRemove = subMapTimeout(time);
		if (toRemove.size() > 0)
		{
			for (Number480 key : toRemove)
			{
				remove(key.getLocationKey(), key.getDomainKey(), key.getContentKey());
			}
		}
	}
	
	private DigestInfo digest(Number160 locationKey, Number160 domainKey)
	{
		DigestInfo digestInfo = new DigestInfo();
		Number320 lockKey = new Number320(locationKey, domainKey);
		Lock lock = dataLock320.lock(lockKey);
		try
		{
			SortedMap<Number480, Data> tmp = get(locationKey, domainKey, Number160.ZERO, Number160.MAX_VALUE);
			for (Number480 key2 : tmp.keySet())
			{
				digestInfo.getKeyDigests().add(key2.getContentKey());
			}
		}
		finally
		{
			dataLock320.unlock(lockKey, lock);
		}
		return digestInfo;
	}

	@Override
	public DigestInfo digest(Number160 locationKey, Number160 domainKey, Collection<Number160> contentKeys)
	{
		if (contentKeys == null)
		{
			return digest(locationKey, domainKey);
		}
		DigestInfo digestInfo = new DigestInfo();
		for(Number160 contentKey:contentKeys)
		{
			Number320 lockKey = new Number320(locationKey, domainKey);
			Lock lock = dataLock320.lock(lockKey);
			try
			{
				if (contains(locationKey, domainKey, contentKey))
				{
					digestInfo.getKeyDigests().add(contentKey);
				}
			}
			finally
			{
				dataLock320.unlock(lockKey, lock);
			}
		}
		return digestInfo;
	}
	
	public Number160 findResponsiblePeers(Number160 locationKey)
	{
		return findPeerIDForResponsibleContent(locationKey);	
	}
	
	public Collection<Number160> findContentResponsibility(Number160 peerID)
	{
		return findContentForResponsiblePeerID(peerID);
	}
	
	public boolean updateContentResponsibilities(Number160 locationKey, Number160 peerId)
	{
		return updateResponsibilities(locationKey, peerId);
	}
	
	public void removeContentResponsibility(Number160 locationKey)
	{
		removeResponsibility(locationKey);
	}

	private boolean canClaimDomain(Number160 locationKey, Number160 domainKey, PublicKey publicKey)
	{
		boolean domainProtectedByOthers = isDomainProtectedByOthers(locationKey, domainKey, publicKey);
		boolean domainOverridableByMe = foreceOverrideDomain(domainKey, publicKey);
		return !domainProtectedByOthers || domainOverridableByMe;
	}
	
	boolean canProtectDomain(Number160 locationKey, Number160 domainKey, PublicKey publicKey)
	{
		if (isDomainRemoved(domainKey))
		{
			return false;
		}
		if (getProtectionDomainEnable() == ProtectionEnable.ALL)
		{
			return true;
		}
		else if (getProtectionDomainEnable() == ProtectionEnable.NONE)
		{
			//only if we have the master key
			return foreceOverrideDomain(domainKey, publicKey);
		}
		return false;
	}
	
	private boolean securityDomainCheck(Number160 locationKey, Number160 domainKey, 
			PublicKey publicKey, boolean domainProtection)
	{
		boolean domainProtectedByOthers = isDomainProtectedByOthers(locationKey, domainKey, publicKey);
		//I dont want to claim the domain
		if(!domainProtection)
		{
			//returns true if the domain is not protceted by others, otherwise false if the domain is protected
			return !domainProtectedByOthers;
		}
		else
		{			
			if (canClaimDomain(locationKey, domainKey, publicKey) && canProtectDomain(locationKey, domainKey, publicKey))
			{
				return protectDomain(locationKey, domainKey, publicKey);
			}
		}
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

	boolean canUpdateEntry(Number160 contentKey, Data oldData, Data newData, boolean protectEntry)
	{
		if (protectEntry)
		{
			return canProtectEntry(contentKey, oldData, newData);
		}
		return true;
	}

	private boolean canProtectEntry(Number160 contentKey, Data oldData, Data newData)
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
		return foreceOverrideEntry(contentKey, newData.getPublicKey());
	}
	
	static boolean isMine(Number160 key, PublicKey publicKey)
	{
		return key.equals(Utils.makeSHAHash(publicKey.getEncoded()));
	}
}