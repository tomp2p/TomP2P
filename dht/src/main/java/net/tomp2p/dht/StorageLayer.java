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
package net.tomp2p.dht;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.DigestStorage;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageLayer implements DigestStorage {

	private static final Logger LOG = LoggerFactory.getLogger(StorageLayer.class);

	public enum ProtectionEnable {
		ALL, NONE
	};

	public enum ProtectionMode {
		NO_MASTER, MASTER_PUBLIC_KEY
	};

	// The number of PutStatus should never exceed 255.
	public enum PutStatus {
		OK, OK_PREPARED, OK_UNCHANGED, FAILED_NOT_ABSENT, FAILED_SECURITY, FAILED, VERSION_FORK, NOT_FOUND, DELETED  
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

	final private RangeLock<Number640> rangeLock = new RangeLock<Number640>();
	final private RangeLock<Number640> responsibilityLock = new RangeLock<Number640>();
	
	final private Storage backend;
	final int maxVersions;

	public StorageLayer(Storage backend) {
		this.backend = backend;
		this.maxVersions = -1;
	}
	
	public StorageLayer(Storage backend, int maxVersions) {
		this.backend = backend;
		this.maxVersions = maxVersions;
	}

	public void protection(ProtectionEnable protectionDomainEnable, ProtectionMode protectionDomainMode,
	        ProtectionEnable protectionEntryEnable, ProtectionMode protectionEntryMode) {
		protectionDomainEnable(protectionDomainEnable);
		protectionDomainMode(protectionDomainMode);
		protectionEntryEnable(protectionEntryEnable);
		protectionEntryMode(protectionEntryMode);
	}

	public void protectionDomainMode(ProtectionMode protectionDomainMode) {
		this.protectionDomainMode = protectionDomainMode;
	}

	public ProtectionMode protectionDomainMode() {
		return protectionDomainMode;
	}

	public void protectionDomainEnable(ProtectionEnable protectionDomainEnable) {
		this.protectionDomainEnable = protectionDomainEnable;
	}

	public ProtectionEnable protectionDomainEnable() {
		return protectionDomainEnable;
	}

	public void protectionEntryMode(ProtectionMode protectionEntryMode) {
		this.protectionEntryMode = protectionEntryMode;
	}

	public ProtectionMode protectionEntryMode() {
		return protectionEntryMode;
	}

	public void protectionEntryEnable(ProtectionEnable protectionEntryEnable) {
		this.protectionEntryEnable = protectionEntryEnable;
	}

	public ProtectionEnable protectionEntryEnable() {
		return protectionEntryEnable;
	}

	public void removeDomainProtection(Number160 removeDomain) {
		removedDomains.add(removeDomain);
	}

	boolean isDomainRemoved(Number160 domain) {
		return removedDomains.contains(domain);
	}
	
	private RangeLock<Number640>.Range lock(Number640 min, Number640 max) { 
		return rangeLock.lock(min, max);
	}
	
	private RangeLock<Number640>.Range lock(Number640 number640) { 
		return rangeLock.lock(number640, number640);
	}
	
	private RangeLock<Number640>.Range lock(Number480 number480) { 
		return rangeLock.lock(new Number640(number480, Number160.ZERO), new Number640(number480, Number160.MAX_VALUE));
	}
	
	private RangeLock<Number640>.Range lock(Number320 number320) { 
		return rangeLock.lock(
				new Number640(number320, Number160.ZERO, Number160.ZERO), 
				new Number640(number320, Number160.MAX_VALUE, Number160.MAX_VALUE));
	}
	
	private RangeLock<Number640>.Range lock(Number160 number160) { 
		return rangeLock.lock(
				new Number640(number160, Number160.ZERO, Number160.ZERO, Number160.ZERO), 
				new Number640(number160, Number160.MAX_VALUE, Number160.MAX_VALUE, Number160.MAX_VALUE));
	}
	
	private RangeLock<Number640>.Range lockResponsibility(Number160 number160) { 
		return responsibilityLock.lock(
				new Number640(number160, Number160.ZERO, Number160.ZERO, Number160.ZERO), 
				new Number640(number160, Number160.MAX_VALUE, Number160.MAX_VALUE, Number160.MAX_VALUE));
	}
	
	private RangeLock<Number640>.Range lock() { 
		return rangeLock.lock(
				new Number640(Number160.ZERO, Number160.ZERO, Number160.ZERO, Number160.ZERO), 
				new Number640(Number160.MAX_VALUE, Number160.MAX_VALUE, Number160.MAX_VALUE, Number160.MAX_VALUE));
	}
	
	public Map<Number640, Enum<?>> putAll(final NavigableMap<Number640, Data> dataMap, PublicKey publicKey, boolean putIfAbsent,
	        boolean domainProtection, boolean sendSelf) {
		if(dataMap.isEmpty()) {
			return Collections.emptyMap();
		}
		final Number640 min = dataMap.firstKey();
		final Number640 max = dataMap.lastKey();
		final Map<Number640, Enum<?>> retVal = new HashMap<Number640, Enum<?>>();
		final HashSet<Number480> keysToCheck = new HashSet<Number480>();
		final RangeLock<Number640>.Range lock = lock(min, max);
		try {
			for(Map.Entry<Number640, Data> entry: dataMap.entrySet()) {
				Number640 key = entry.getKey();
				keysToCheck.add(key.locationAndDomainAndContentKey());
				Data newData = entry.getValue();
				if (!securityDomainCheck(key.locationAndDomainKey(), publicKey, publicKey, domainProtection)) {
					retVal.put(key, PutStatus.FAILED_SECURITY);
					newData.release();
					continue;
				}
				
				// We need this check in case we did not use the encoder/deconder,
				// which is the case if we send the message to ourself. In that
				// case, the public key of the data is never set to the message
				// publick key, if the publick key of the data was null.
				final PublicKey dataKey;
				if(sendSelf && newData.publicKey() == null) {
					dataKey = publicKey;
				} else {
					dataKey = newData.publicKey();
				}
				
				if (!securityEntryCheck(key.locationAndDomainAndContentKey(), publicKey, dataKey,
				        newData.isProtectedEntry())) {
					retVal.put(key, PutStatus.FAILED_SECURITY);
					newData.release();
					continue;
				}
				
				boolean contains = backend.contains(key);
				if (contains) {
					if(putIfAbsent) {
						retVal.put(key, PutStatus.FAILED_NOT_ABSENT);
						newData.release();
						continue;
					}
					final Data oldData = backend.get(key);
					if(oldData.isDeleted()) {
						retVal.put(key, PutStatus.DELETED);
						newData.release();
						continue;
					}
					if(!oldData.basedOnSet().equals(newData.basedOnSet())) {
						retVal.put(key, PutStatus.VERSION_FORK);
						newData.release();
						continue;
					}
				}
				
				Data oldData = backend.put(key, newData);
				
				long expiration = newData.expirationMillis();
				// handle timeout
				backend.addTimeout(key, expiration);
				
				if(newData.hasPrepareFlag()) {
					retVal.put(key, PutStatus.OK_PREPARED);
				} else {
					retVal.put(key, PutStatus.OK);
				}
				if(oldData != null && oldData != newData) {
					oldData.release();
				}
			}
			
			for(Number480 key:keysToCheck) {
				
				//now check for forks
				Number640 minVersion = new Number640(key, Number160.ZERO);
				Number640 maxVersion = new Number640(key, Number160.MAX_VALUE);
				NavigableMap<Number640, Data> tmp = backend.subMap(minVersion, maxVersion);
				tmp = filterCopyOrig(tmp, -1, true);
				NavigableMap<Number640, Data> heads = getLatestInternalOrig(tmp);
				
				final boolean forked = heads.size() > 1; 
				for(final Map.Entry<Number640, Data> entry:heads.entrySet()) {
					if(forked) {
						if(retVal.containsKey(entry.getKey())) {
							retVal.put(entry.getKey(), PutStatus.VERSION_FORK);
						}
					}
				}
				
				//now remove old versions
				if (maxVersions > 0) {
		        	NavigableMap<Number640, Data> versions = backend.subMap(minVersion, maxVersion);
					
					while (!versions.isEmpty()
					        && versions.firstKey().versionKey().timestamp() + maxVersions <= versions.lastKey().versionKey()
					                .timestamp()) {
						Map.Entry<Number640, Data> entry = versions.pollFirstEntry();
						Data removed = backend.remove(entry.getKey(), true);
						if(removed != null) {
							removed.release();
						}
						backend.removeTimeout(entry.getKey());
					}
				}
			}			
			return retVal;
			
		} finally {
			lock.unlock();
		}
	}
	
	public Enum<?> put(final Number640 key, Data newData, PublicKey publicKey, boolean putIfAbsent,
	        boolean domainProtection, boolean sendSelf) {
		final NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
		dataMap.put(key, newData);
		Map<Number640, Enum<?>> putStatus = putAll(dataMap, publicKey, putIfAbsent, domainProtection, sendSelf);
		Enum<?> retVal = putStatus.get(key);
		if(retVal == null) {
			return PutStatus.FAILED;
		} else {
			return retVal;
		}
	}

	public Pair<Data, Enum<?>> remove(Number640 key, PublicKey publicKey, boolean returnData) {
		RangeLock<Number640>.Range lock = lock(key);
		try {
			if (!canClaimDomain(key.locationAndDomainKey(), publicKey)) {
				return new Pair<Data, Enum<?>>(null, PutStatus.FAILED_SECURITY);
			}
			if (!canClaimEntry(key.locationAndDomainAndContentKey(), publicKey)) {
				return new Pair<Data, Enum<?>>(null, PutStatus.FAILED_SECURITY);
			}
			if (!backend.contains(key)) {
				return new Pair<Data, Enum<?>>(null, PutStatus.NOT_FOUND);
			}
			backend.removeTimeout(key);
			Data removed = backend.remove(key, returnData);
			if(removed != null && !returnData) {
				removed.release();
			}
			// else -> it will be removed in the encoder 
			return new Pair<Data, Enum<?>>(removed, PutStatus.OK);
		} finally {
			lock.unlock();
		}
	}

	public Data get(Number640 key) {
		RangeLock<Number640>.Range lock = lock(key);
		try {
			Data tmp = getInternal(key);
			return tmp == null? null:tmp.duplicate();
		} finally {
			lock.unlock();
		}
	}

	private Data getInternal(Number640 key) {
		Data data = backend.get(key);
		if (data != null && !data.hasPrepareFlag()) {
			return data;
		} else {
			return null;
		}
	}

	public NavigableMap<Number640, Data> get(Number640 from, Number640 to, int limit, boolean ascending) {
		RangeLock<Number640>.Range lock = rangeLock.lock(from, to);
		try {
			NavigableMap<Number640, Data> tmp = backend.subMap(from, to);
			tmp = filterCopy(tmp, limit, ascending);
			return tmp;
		} finally {
			lock.unlock();
		}
	}

	public NavigableMap<Number640, Data> getLatestVersion(Number640 key) {
		RangeLock<Number640>.Range lock = lock(key.locationAndDomainAndContentKey());
		try {
			NavigableMap<Number640, Data> tmp = backend.subMap(key.minVersionKey(), key.maxVersionKey());
			tmp = filterCopyOrig(tmp, -1, true);
			return getLatestInternal(tmp);
		} finally {
			lock.unlock();
		}
	}

	private NavigableMap<Number640, Data> getLatestInternal(NavigableMap<Number640, Data> input) {
	    // delete all predecessors
		NavigableMap<Number640, Data> result = new TreeMap<Number640, Data>();
	    while (!input.isEmpty()) {
	    	// first entry is a latest version
	    	Entry<Number640, Data> latest = input.lastEntry();
	    	// store in results list
	    	result.put(latest.getKey(), latest.getValue().duplicate());
	    	// delete all predecessors of latest entry
	    	deletePredecessors(latest.getKey(), input);
	    }
	    return result;
    }
	
	private NavigableMap<Number640, Data> getLatestInternalOrig(NavigableMap<Number640, Data> input) {
	    // delete all predecessors
		NavigableMap<Number640, Data> result = new TreeMap<Number640, Data>();
	    while (!input.isEmpty()) {
	    	// first entry is a latest version
	    	Entry<Number640, Data> latest = input.lastEntry();
	    	// store in results list
	    	result.put(latest.getKey(), latest.getValue());
	    	// delete all predecessors of latest entry
	    	deletePredecessors(latest.getKey(), input);
	    }
	    return result;
    }

	private NavigableMap<Number640, Data> filterCopy(final NavigableMap<Number640, Data> tmp, int limit, boolean ascending) {
		NavigableMap<Number640, Data> retVal = new TreeMap<Number640, Data>();
		int counter = 0;
		for(Map.Entry<Number640, Data> entry : ascending ? tmp.entrySet() : tmp.descendingMap().entrySet()) {
	    	if (!entry.getValue().hasPrepareFlag()) {
	    		if(limit >= 0 && counter++ >= limit) {
	    			break;
	    		}
	    		retVal.put(entry.getKey(), entry.getValue().duplicate());
	    	} 
	    }
		return retVal;
    }
	
	private NavigableMap<Number640, Data> filterCopyOrig(final NavigableMap<Number640, Data> tmp, int limit, boolean ascending) {
		NavigableMap<Number640, Data> retVal = new TreeMap<Number640, Data>();
		int counter = 0;
		for(Map.Entry<Number640, Data> entry : ascending ? tmp.entrySet() : tmp.descendingMap().entrySet()) {
	    	if (!entry.getValue().hasPrepareFlag()) {
	    		if(limit >= 0 && counter++ >= limit) {
	    			break;
	    		}
	    		retVal.put(entry.getKey(), entry.getValue());
	    	} 
	    }
		return retVal;
    }
	
	//iterative version
	private NavigableMap<Number640, Data> deletePredecessors(Number640 key, NavigableMap<Number640, Data> sortedMap) {
		final List<Number640> toRemove = new ArrayList<Number640>();
		final NavigableMap<Number640, Data> removed = new TreeMap<Number640, Data>();
		toRemove.add(key);
		while(!toRemove.isEmpty()) {
			final Number640 key2 = toRemove.remove(0);
			final Data version = sortedMap.remove(key2);
			if(version != null) {
				removed.put(key2, version);
				// check if version has been already deleted && // check if version is initial version
				if(!version.basedOnSet().isEmpty()) {
					for (final Number160 basedOnKey : version.basedOnSet()) {
						toRemove.add(new Number640(key.locationAndDomainAndContentKey(), basedOnKey));
					}
				}
			}
		}
		return removed;
	}

	public NavigableMap<Number640, Data> get() {
		RangeLock<Number640>.Range lock = lock();
		try {
			return filterCopy(backend.map(), -1, true);
		} finally {
			lock.unlock();
		}
	}

	public boolean contains(Number640 key) {
		RangeLock<Number640>.Range lock = lock(key);
		try {
			return backend.contains(key);
		} finally {
			lock.unlock();
		}
	}

	public NavigableMap<Number640, Data> get(Number640 from, Number640 to, SimpleBloomFilter<Number160> contentKeyBloomFilter,
	        SimpleBloomFilter<Number160> versionKeyBloomFilter, SimpleBloomFilter<Number160> contentBloomFilter, 
	        int limit, boolean ascending, boolean isBloomFilterAnd) {
		RangeLock<Number640>.Range lock = rangeLock.lock(from, to);
		try {
			NavigableMap<Number640, Data> tmp = backend.subMap(from, to);
			tmp = filterCopy(tmp, limit, ascending);
			Iterator<Map.Entry<Number640, Data>> iterator = tmp.entrySet().iterator();

			while (iterator.hasNext()) {
				Map.Entry<Number640, Data> entry = iterator.next();

				if (entry.getValue().hasPrepareFlag()) {
					entry.getValue().release();
					iterator.remove();
					continue;
				}

				if (isBloomFilterAnd) {
					if (!contentKeyBloomFilter.contains(entry.getKey().contentKey())) {
						entry.getValue().release();
						iterator.remove();
						continue;
					}
					if (!versionKeyBloomFilter.contains(entry.getKey().versionKey())) {
						entry.getValue().release();
						iterator.remove();
						continue;
					}
					if (!contentBloomFilter.contains(entry.getValue().hash())) {
						entry.getValue().release();
						iterator.remove();
						continue;
					}
				} else {
					if (contentKeyBloomFilter.contains(entry.getKey().contentKey())) {
						entry.getValue().release();
						iterator.remove();
						continue;
					}
					if (versionKeyBloomFilter.contains(entry.getKey().versionKey())) {
						entry.getValue().release();
						iterator.remove();
						continue;
					}
					if (contentBloomFilter.contains(entry.getValue().hash())) {
						entry.getValue().release();
						iterator.remove();
						continue;
					}
				}
			}

			return tmp;
		} finally {
			lock.unlock();
		}
	}

	public NavigableMap<Number640, Data> removeReturnData(Number640 from, Number640 to, PublicKey publicKey) {
		RangeLock<Number640>.Range lock = rangeLock.lock(from, to);
		try {
			Map<Number640, Data> tmp = backend.subMap(from, to);
			NavigableMap<Number640, Data> result = new TreeMap<Number640, Data>();

			for (Number640 key : tmp.keySet()) {
				// fail fast, as soon as we want to remove 1 domain that we
				// cannot, abort
				if (!canClaimDomain(key.locationAndDomainKey(), publicKey)) {
					result.put(key, null);
				} else if (!canClaimEntry(key.locationAndDomainAndContentKey(), publicKey)) {
					result.put(key, null);
				} else {
					Data toRemove = backend.get(key);
					if (toRemove!= null && (toRemove.publicKey() == null || toRemove.publicKey().equals(publicKey))) {
						backend.removeTimeout(key);
						Data removed = backend.remove(key, true);
						result.put(key, removed);
					}
				}
			}
			return result;
		} finally {
			lock.unlock();
		}
	}

	public SortedMap<Number640, Byte> removeReturnStatus(Number640 from, Number640 to, PublicKey publicKey) {
		RangeLock<Number640>.Range lock = rangeLock.lock(from, to);
		try {
			Map<Number640, Data> tmp = backend.subMap(from, to);
			SortedMap<Number640, Byte> result = new TreeMap<Number640, Byte>();
			for (Number640 key : tmp.keySet()) {
				Pair<Data, Enum<?>> pair = remove(key, publicKey, false);
				if(pair.element0() != null) {
					pair.element0().release();
				}
				result.put(key, (byte) pair.element1().ordinal());
			}
			return result;
		} finally {
			lock.unlock();
		}
	}

	public void checkTimeout() {
		long time = System.currentTimeMillis();
		Collection<Number640> toRemove = backend.subMapTimeout(time);
		for (Number640 key : toRemove) {
			RangeLock<Number640>.Range lock = lock(key);
			try {
				Data oldData = backend.remove(key, false);
				if(oldData != null) {
					oldData.release();
				}
				backend.removeTimeout(key);
				// remove responsibility if we don't have any data stored under
				// locationkey
				Number160 locationKey = key.locationKey();
				RangeLock<Number640>.Range lockResp= lockResponsibility(locationKey);
				try {
					if (isEmpty(locationKey)) {
						backend.removeResponsibility(locationKey);
					}
				} finally {
					lockResp.unlock();
				}
			} finally {
				lock.unlock();
			}
		}
	}

	private boolean isEmpty(Number160 locationKey) {
		Number640 from = new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO);
		Number640 to = new Number640(locationKey, Number160.MAX_VALUE, Number160.MAX_VALUE, Number160.MAX_VALUE);
		Map<Number640, Data> tmp = backend.subMap(from, to);
		return tmp.size() == 0;
	}

	/* (non-Javadoc)
	 * @see net.tomp2p.dht.DigestStorage#digest(net.tomp2p.peers.Number640, net.tomp2p.peers.Number640, int, boolean)
	 */
	@Override
    public DigestInfo digest(Number640 from, Number640 to, int limit, boolean ascending) {
		DigestInfo digestInfo = new DigestInfo();
		RangeLock<Number640>.Range lock = rangeLock.lock(from, to);
		try {
			NavigableMap<Number640, Data> tmp = backend.subMap(from, to);
			tmp = filterCopy(tmp, limit, ascending);
			for (Map.Entry<Number640, Data> entry : tmp.entrySet()) {
				if (!entry.getValue().hasPrepareFlag()) {
					digestInfo.put(entry.getKey(), entry.getValue().basedOnSet());
				}
			}
			return digestInfo;
		} finally {
			lock.unlock();
		}
	}

	/* (non-Javadoc)
	 * @see net.tomp2p.dht.DigestStorage#digest(net.tomp2p.peers.Number320, net.tomp2p.rpc.SimpleBloomFilter, net.tomp2p.rpc.SimpleBloomFilter, int, boolean, boolean)
	 */
	@Override
    public DigestInfo digest(Number320 locationAndDomainKey, SimpleBloomFilter<Number160> keyBloomFilter,
	        SimpleBloomFilter<Number160> contentKeyBloomFilter, int limit, boolean ascending, boolean isBloomFilterAnd) {
		DigestInfo digestInfo = new DigestInfo();
		RangeLock<Number640>.Range lock = lock(locationAndDomainKey);
		try {
			Number640 from = new Number640(locationAndDomainKey, Number160.ZERO, Number160.ZERO);
			Number640 to = new Number640(locationAndDomainKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
			NavigableMap<Number640, Data> tmp = backend.subMap(from, to);
			tmp = filterCopy(tmp, limit, ascending);
			for (Map.Entry<Number640, Data> entry : tmp.entrySet()) {
				if (isBloomFilterAnd) {
					if (keyBloomFilter == null || keyBloomFilter.contains(entry.getKey().contentKey())) {
						if (contentKeyBloomFilter == null || contentKeyBloomFilter.contains(entry.getValue().hash())) {
							if (!entry.getValue().hasPrepareFlag()) {
								digestInfo.put(entry.getKey(), entry.getValue().basedOnSet());
							}
						}
					}
				} else {
					if (keyBloomFilter == null || !keyBloomFilter.contains(entry.getKey().contentKey())) {
						if (contentKeyBloomFilter == null || !contentKeyBloomFilter.contains(entry.getValue().hash())) {
							if (!entry.getValue().hasPrepareFlag()) {
								digestInfo.put(entry.getKey(),entry.getValue().basedOnSet());
							}
						}
					}
				}
			}
			return digestInfo;
		} finally {
			lock.unlock();
		}
	}

	/* (non-Javadoc)
	 * @see net.tomp2p.dht.DigestStorage#digest(java.util.Collection)
	 */
	@Override
    public DigestInfo digest(Collection<Number640> number640s) {
		DigestInfo digestInfo = new DigestInfo();
		for (Number640 number640 : number640s) {
			RangeLock<Number640>.Range lock = lock(number640);
			try {
				if (backend.contains(number640)) {
					Data data = getInternal(number640);
					if (data != null) {
						digestInfo.put(number640, data.basedOnSet());
					}
				}
			} finally {
				lock.unlock();
			}
		}
		return digestInfo;
	}

	private boolean securityDomainCheck(Number320 key, PublicKey publicKey, PublicKey newPublicKey,
	        boolean domainProtection) {

		boolean domainProtectedByOthers = backend.isDomainProtectedByOthers(key, publicKey);
		// I dont want to claim the domain
		if (!domainProtection) {
			LOG.debug("no domain protection requested {} for domain {}", Utils.hash(newPublicKey), key);
			// returns true if the domain is not protceted by others, otherwise
			// false if the domain is protected
			return !domainProtectedByOthers;
		} else {
			LOG.debug("domain protection requested {} for domain {}", Utils.hash(newPublicKey), key);
			if (canClaimDomain(key, publicKey)) {
				if (canProtectDomain(key.domainKey(), publicKey)) {
					LOG.debug("set domain protection");
					return backend.protectDomain(key, newPublicKey);
				} else {
					return true;
				}
			}
		}
		return false;
	}

	private boolean securityEntryCheck(Number480 key, PublicKey publicKeyMessage, PublicKey publicKeyData,
	        boolean entryProtection) {
		boolean entryProtectedByOthers = backend.isEntryProtectedByOthers(key, publicKeyMessage);
		// I dont want to claim the domain
		if (!entryProtection) {
			// returns true if the domain is not protceted by others, otherwise
			// false if the domain is protected
			return !entryProtectedByOthers;
		} else {
			//replication cannot sign messages with the originators key, so we must also check the public key of the data
			if (canClaimEntry(key, publicKeyMessage) || canClaimEntry(key, publicKeyData)) {
				if (canProtectEntry(key.domainKey(), publicKeyMessage)) {
					return backend.protectEntry(key, publicKeyData);
				} else {
					return true;
				}
			}
		}
		return false;
	}

	private boolean foreceOverrideDomain(Number160 domainKey, PublicKey publicKey) {
		// we are in public key mode
		if (protectionDomainMode() == ProtectionMode.MASTER_PUBLIC_KEY && publicKey != null) {
			// if the hash of the public key is the same as the domain, we can
			// overwrite
			return isMine(domainKey, publicKey);
		}
		return false;
	}

	private boolean foreceOverrideEntry(Number160 entryKey, PublicKey publicKey) {
		// we are in public key mode
		if (protectionEntryMode() == ProtectionMode.MASTER_PUBLIC_KEY && publicKey != null
		        && publicKey.getEncoded() != null) {
			// if the hash of the public key is the same as the domain, we can
			// overwrite
			return isMine(entryKey, publicKey);
		}
		return false;
	}

	private boolean canClaimDomain(Number320 key, PublicKey publicKey) {
		boolean domainProtectedByOthers = backend.isDomainProtectedByOthers(key, publicKey);
		boolean domainOverridableByMe = foreceOverrideDomain(key.domainKey(), publicKey);
		return !domainProtectedByOthers || domainOverridableByMe;
	}

	private boolean canClaimEntry(Number480 key, PublicKey publicKey) {
		boolean entryProtectedByOthers = backend.isEntryProtectedByOthers(key, publicKey);
		boolean entryOverridableByMe = foreceOverrideEntry(key.contentKey(), publicKey);
		return !entryProtectedByOthers || entryOverridableByMe;
	}

	private boolean canProtectDomain(Number160 domainKey, PublicKey publicKey) {
		if (isDomainRemoved(domainKey)) {
			return false;
		}
		if (protectionDomainEnable() == ProtectionEnable.ALL) {
			return true;
		} else if (protectionDomainEnable() == ProtectionEnable.NONE) {
			// only if we have the master key
			return foreceOverrideDomain(domainKey, publicKey);
		}
		return false;
	}

	private boolean canProtectEntry(Number160 contentKey, PublicKey publicKey) {
		if (protectionEntryEnable() == ProtectionEnable.ALL) {
			return true;
		} else if (protectionEntryEnable() == ProtectionEnable.NONE) {
			// only if we have the master key
			return foreceOverrideEntry(contentKey, publicKey);
		}
		return false;
	}

	private static boolean isMine(Number160 key, PublicKey publicKey) {
		return key.equals(Utils.makeSHAHash(publicKey.getEncoded()));
	}

	public RangeLock<Number640> rangeLock() {
		return rangeLock;
	}

	public Collection<Number160> findContentForResponsiblePeerID(Number160 peerID) {
		RangeLock<Number640>.Range lockResp = lockResponsibility(peerID);
		try {
			Collection<Number160> contentIDs = backend.findContentForResponsiblePeerID(peerID);
			if (contentIDs == null) {
				return Collections.<Number160> emptyList();
			} else {
				return new ArrayList<Number160>(contentIDs);
			}
		} finally {
			lockResp.unlock();
        }
	}
	
	public Number160 findPeerIDsForResponsibleContent(Number160 locationKey) {
		RangeLock<Number640>.Range lockResp = lockResponsibility(locationKey);
		try {
			return backend.findPeerIDsForResponsibleContent(locationKey);
		} finally {
			lockResp.unlock();
        }
	}
	
	public boolean updateResponsibilities(Number160 locationKey, Number160 peerId) {
		RangeLock<Number640>.Range lockResp1 = lockResponsibility(peerId);
		RangeLock<Number640>.Range lockResp2 = lockResponsibility(locationKey);
        try {
            return backend.updateResponsibilities(locationKey, peerId);
        } finally {
        	lockResp1.unlock();
        	lockResp2.unlock();
        }
	}
	
	public void removeResponsibility(Number160 locationKey, boolean keepData) {
		RangeLock<Number640>.Range lockResp = lockResponsibility(locationKey);
		try {
			if (!keepData) {
				RangeLock<Number640>.Range lock = lock(locationKey);
				try {
					final NavigableMap<Number640, Data> removed = backend.remove(
						new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO),
						new Number640(locationKey, Number160.MAX_VALUE, Number160.MAX_VALUE, Number160.MAX_VALUE));
					for(Map.Entry<Number640,Data> entry:removed.entrySet()) {
						if(entry.getValue() != null) {
							entry.getValue().release();
						}
						backend.removeTimeout(entry.getKey());
					}
				} finally {
					lock.unlock();
		        }
			}
        	backend.removeResponsibility(locationKey);
        } finally {
        	lockResp.unlock();
        }
	}

	private class StorageMaintenanceTask implements Runnable {
		@Override
		public void run() {
			checkTimeout();
		}
	}

	public void start(ScheduledExecutorService timer, int storageIntervalMillis) {
		timer.scheduleAtFixedRate(new StorageMaintenanceTask(), storageIntervalMillis, storageIntervalMillis,
		        TimeUnit.MILLISECONDS);
	}

	public Enum<?> updateMeta(Number320 locationAndDomainKey, PublicKey publicKey, PublicKey newPublicKey) {
		if (!securityDomainCheck(locationAndDomainKey, publicKey, newPublicKey, true)) {
			return PutStatus.FAILED_SECURITY;
		}
		return PutStatus.OK;
	}

	public Enum<?> updateMeta(PublicKey publicKey, Number640 key, Data newData) {
		RangeLock<Number640>.Range lock = lock(key);
		try {
			if (!securityEntryCheck(key.locationAndDomainAndContentKey(), publicKey, newData.publicKey(),
			        newData.isProtectedEntry())) {
				return PutStatus.FAILED_SECURITY;
			}

			final Data data = backend.get(key);
			boolean changed = false;
			if (data!=null && newData.publicKey() != null) {
				data.publicKey(newData.publicKey());
				changed = true;
			}
			if (data!=null && newData.isSigned()) {
				data.signature(newData.signature());
				changed = true;
			}
			if (data!=null) {
				data.validFromMillis(newData.validFromMillis());
				data.ttlSeconds(newData.ttlSeconds());
				changed = true;
			}
			if (changed) {
				long expiration = data.expirationMillis();
				// handle timeout
				backend.addTimeout(key, expiration);
				// no release of old data, as we just update it
				backend.put(key, data);
				return PutStatus.OK;
			} else {
				return PutStatus.NOT_FOUND;
			}
		} finally {
			lock.unlock();
		}
	}

	public int storageCheckIntervalMillis() {
	    return backend.storageCheckIntervalMillis();
    }

	public Enum<?> putConfirm(PublicKey publicKey, Number640 key, Data newData) {
		RangeLock<Number640>.Range lock = lock(key);
		try {
			if (!securityEntryCheck(key.locationAndDomainAndContentKey(), publicKey, newData.publicKey(),
					newData.isProtectedEntry())) {
				return PutStatus.FAILED_SECURITY;
			}

			final Data data = backend.get(key);
			if (data != null) {
				// remove prepare flag
				data.prepareFlag(false);

				data.validFromMillis(newData.validFromMillis());
				data.ttlSeconds(newData.ttlSeconds());

				long expiration = data.expirationMillis();
				// handle timeout
				backend.addTimeout(key, expiration);
				backend.put(key, data);
				//don't release data as we just update
				return PutStatus.OK;
			} else {
				return PutStatus.NOT_FOUND;
			}
		} finally {
			lock.unlock();
		}
		//TODO: check for FORKS!
	}

	public void close() {
		backend.close();
	}
}
