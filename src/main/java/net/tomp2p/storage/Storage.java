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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.utils.Utils;

public abstract class Storage implements Digest
{
	public enum ProtectionEnable
	{
		ALL, NONE
	};
	public enum ProtectionMode
	{
		NO_MASTER, MASTER_PUBLIC_KEY
	};
	public enum ProtectionEntryInDomain
	{
		ENTRY_REMOVE_IF_DOMAIN_CLAIMED, ENTRY_LEAVE
	};
	// Hash of public key is always preferred
	private ProtectionMode protectionDomainMode = ProtectionMode.MASTER_PUBLIC_KEY;
	// Domains can generallay be protected
	private ProtectionEnable protectionDomainEnable = ProtectionEnable.ALL;
	// Hash of public key is always preferred
	private ProtectionMode protectionEntryMode = ProtectionMode.MASTER_PUBLIC_KEY;
	// Entries can generallay be protected
	private ProtectionEnable protectionEntryEnable = ProtectionEnable.ALL;
	// default is not to delete
	private ProtectionEntryInDomain protectionEntryInDomain = ProtectionEntryInDomain.ENTRY_LEAVE;
	// stores the domains that cannot be reserved and items can be added by
	// anyone
	private Collection<Number160> removedDomains = new HashSet<Number160>();

	// Core
	public abstract boolean put(Number480 key, Data data, PublicKey publicKey, boolean putIfAbsent,
			boolean domainProtection);

	public abstract Data get(Number480 key);

	public abstract SortedMap<Number480, Data> get(Number480 fromKey, Number480 toKey);

	public abstract SortedMap<Number480, Data> remove(Number480 fromKey, Number480 toKey,
			PublicKey publicKey);

	public abstract Data remove(Number480 key, PublicKey publicKey);
	
	public SortedMap<Number480, Data> remove(Number320 number320, PublicKey publicKey)
	{
		return remove(number320.min(), number320.max(), publicKey);
	}

	public abstract boolean contains(Number480 key);

	public abstract void iterateAndRun(Number160 locationKey, StorageRunner runner);

	public abstract void close();

	// Replication
	public abstract Collection<Number160> findResponsibleData(Number160 peerID);

	public abstract Number160 findResponsiblePeerID(Number160 key);

	public abstract boolean updateResponsibilities(Number160 key, Number160 closest);

	public abstract Collection<Number480> storedDirectReplication();
	
	public Map<Number160, Data> get(Collection<Number480> keys)
	{
		return get(keys, null);
	}

	public Map<Number160, Data> get(Collection<Number480> keys, PublicKey publicKey)
	{
		Map<Number160, Data> result = new HashMap<Number160, Data>();
		for (Number480 key : keys)
		{
			Data data = get(key);
			if (data != null && (publicKey==null || publicKey.equals(data.getDataPublicKey())))
				result.put(key.getContentKey(), data);
		}
		return result;
	}

	public SortedMap<Number480, Data> get(Number320 key)
	{
		return get(key.min(), key.max());
	}

	public void setProtection(ProtectionEnable protectionDomainEnable,
			ProtectionMode protectionDomainMode, ProtectionEnable protectionEntryEnable,
			ProtectionMode protectionEntryMode, ProtectionEntryInDomain protectionEntryInDomain)
	{
		setProtectionDomainEnable(protectionDomainEnable);
		setProtectionDomainMode(protectionDomainMode);
		setProtectionEntryEnable(protectionEntryEnable);
		setProtectionEntryMode(protectionEntryMode);
		setProtectionEntryInDomain(protectionEntryInDomain);
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

	public void setProtectionEntryInDomain(ProtectionEntryInDomain protectionEntryInDomain)
	{
		this.protectionEntryInDomain = protectionEntryInDomain;
	}

	public ProtectionEntryInDomain getProtectionEntryInDomain()
	{
		return protectionEntryInDomain;
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
			else if (oldData.getDataPublicKey() == null)
				return true;
			else
			{
				if (oldData.getDataPublicKey().equals(newData.getDataPublicKey()))
					return true;
			}
		}
		// we cannot protect, but maybe we have the rigth public key
		return foreceOverrideEntry(key.getContentKey(), newData.getDataPublicKey());
	}

	static boolean isMine(Number160 key, PublicKey publicKey)
	{
		return key.equals(Utils.makeSHAHash(publicKey.getEncoded()));
	}
}