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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageLayer {

	private static final Logger LOG = LoggerFactory.getLogger(StorageLayer.class);

	public enum ProtectionEnable {
		ALL, NONE
	};

	public enum ProtectionMode {
		NO_MASTER, MASTER_PUBLIC_KEY
	};

	// The number of PutStatus should never exceed 255.
	public enum PutStatus {
		OK, FAILED_NOT_ABSENT, FAILED_SECURITY, FAILED, VERSION_FORK, NOT_FOUND
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

	final private KeyLock<Storage> dataLock = new KeyLock<Storage>();

	final private KeyLock<Number160> dataLock160 = new KeyLock<Number160>();

	final private KeyLock<Number320> dataLock320 = new KeyLock<Number320>();

	final private KeyLock<Number480> dataLock480 = new KeyLock<Number480>();

	final private KeyLock<Number640> dataLock640 = new KeyLock<Number640>();
	
	final private KeyLock<Number160> responsibilityLock = new KeyLock<Number160>();

	final private Storage backend;

	public StorageLayer(Storage backend) {
		this.backend = backend;
	}

	public void setProtection(ProtectionEnable protectionDomainEnable, ProtectionMode protectionDomainMode,
	        ProtectionEnable protectionEntryEnable, ProtectionMode protectionEntryMode) {
		setProtectionDomainEnable(protectionDomainEnable);
		setProtectionDomainMode(protectionDomainMode);
		setProtectionEntryEnable(protectionEntryEnable);
		setProtectionEntryMode(protectionEntryMode);
	}

	public void setProtectionDomainMode(ProtectionMode protectionDomainMode) {
		this.protectionDomainMode = protectionDomainMode;
	}

	public ProtectionMode getProtectionDomainMode() {
		return protectionDomainMode;
	}

	public void setProtectionDomainEnable(ProtectionEnable protectionDomainEnable) {
		this.protectionDomainEnable = protectionDomainEnable;
	}

	public ProtectionEnable getProtectionDomainEnable() {
		return protectionDomainEnable;
	}

	public void setProtectionEntryMode(ProtectionMode protectionEntryMode) {
		this.protectionEntryMode = protectionEntryMode;
	}

	public ProtectionMode getProtectionEntryMode() {
		return protectionEntryMode;
	}

	public void setProtectionEntryEnable(ProtectionEnable protectionEntryEnable) {
		this.protectionEntryEnable = protectionEntryEnable;
	}

	public ProtectionEnable getProtectionEntryEnable() {
		return protectionEntryEnable;
	}

	public void removeDomainProtection(Number160 removeDomain) {
		removedDomains.add(removeDomain);
	}

	boolean isDomainRemoved(Number160 domain) {
		return removedDomains.contains(domain);
	}

	public Enum<?> put(final Number640 key, Data newData, PublicKey publicKey, boolean putIfAbsent,
	        boolean domainProtection) {
		boolean retVal = false;
		KeyLock<Number640>.RefCounterLock lock = dataLock640.lock(key);
		try {
			if (!securityDomainCheck(key.locationAndDomainKey(), publicKey, publicKey, domainProtection)) {
				return PutStatus.FAILED_SECURITY;
			}
			if (!securityEntryCheck(key.locationDomainAndContentKey(), publicKey, newData.publicKey(),
			        newData.isProtectedEntry())) {
				return PutStatus.FAILED_SECURITY;
			}

			boolean contains = backend.contains(key);
			if (putIfAbsent && contains) {
				return PutStatus.FAILED_NOT_ABSENT;
			}

			boolean versionFork = false;
			for (Number160 bKey: newData.basedOnSet()) {
				versionFork |= checkForForks(bKey);
			}

			retVal = backend.put(key, newData);
			if (retVal) {
				long expiration = newData.expirationMillis();
				// handle timeout
				backend.addTimeout(key, expiration);
			}

			if (retVal && versionFork) {
				return PutStatus.VERSION_FORK;
			} else if (retVal) {
				return PutStatus.OK;
			} else {
				return PutStatus.FAILED;
			}
		} finally {
			dataLock640.unlock(lock);
		}
	}

	public Pair<Data, Enum<?>> remove(Number640 key, PublicKey publicKey, boolean returnData) {
		KeyLock<Number640>.RefCounterLock lock = dataLock640.lock(key);
		try {
			if (!canClaimDomain(key.locationAndDomainKey(), publicKey)) {
				return new Pair<Data, Enum<?>>(null, PutStatus.FAILED_SECURITY);
			}
			if (!canClaimEntry(key.locationDomainAndContentKey(), publicKey)) {
				return new Pair<Data, Enum<?>>(null, PutStatus.FAILED_SECURITY);
			}
			if (!backend.contains(key)) {
				return new Pair<Data, Enum<?>>(null, PutStatus.NOT_FOUND);
			}
			backend.removeTimeout(key);
			backend.removeResponsibility(key.getLocationKey());
			return new Pair<Data, Enum<?>>(backend.remove(key, returnData), PutStatus.OK);
		} finally {
			dataLock640.unlock(lock);
		}
	}

	public Data get(Number640 key) {
		KeyLock<Number640>.RefCounterLock lock = dataLock640.lock(key);
		try {
			return getInternal(key);
		} finally {
			dataLock640.unlock(lock);
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
		KeyLock<?>.RefCounterLock lock = findAndLock(from, to);
		try {
			NavigableMap<Number640, Data> tmp = backend.subMap(from, to, limit, ascending);
			Iterator<Map.Entry<Number640, Data>> iterator = tmp.entrySet().iterator();

			while (iterator.hasNext()) {
				Map.Entry<Number640, Data> entry = iterator.next();
				if (entry.getValue().hasPrepareFlag()) {
					iterator.remove();
				} 
			}

			return tmp;
		} finally {
			lock.unlock();
		}
	}

	public Map<Number640, Data> getLatestVersion(Number640 from, Number640 to) {
		KeyLock<?>.RefCounterLock lock = findAndLock(from, to);
		try {
			NavigableMap<Number640, Data> tmp = backend.subMap(from, to, -1, true);
			Iterator<Map.Entry<Number640, Data>> iterator = tmp.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<Number640, Data> entry = iterator.next();
				if (entry.getValue().hasPrepareFlag()) {
					iterator.remove();
				} 
			}

			// delete all predecessors
			Map<Number640, Data> result = new HashMap<Number640, Data>();
			while (!tmp.isEmpty()) {
				// first entry is a latest version
				Entry<Number640, Data> latest = tmp.lastEntry();
				// store in results list
				result.put(latest.getKey(), latest.getValue());
				// delete all predecessors of latest entry
				deletePredecessors(tmp, latest.getKey());
			}

			return result;
		} finally {
			lock.unlock();
		}
	}

	private void deletePredecessors(NavigableMap<Number640, Data> sortedMap, Number640 key) {
		Data version = sortedMap.remove(key);
		// check if version has been already deleted
		if (version == null) {
			return;
		}
		// check if version is initial version
		if (version.basedOnSet().isEmpty()) {
			return;
		}
		// remove all predecessor versions recursively
		for (Number160 basedOnKey : version.basedOnSet()) {
			deletePredecessors(sortedMap, new Number640(key.locationDomainAndContentKey(), basedOnKey));
		}
	}

	public NavigableMap<Number640, Data> get() {
		KeyLock<Storage>.RefCounterLock lock = dataLock.lock(backend);
		try {
			return backend.map();
		} finally {
			lock.unlock();
		}
	}

	public boolean contains(Number640 key) {
		KeyLock<Number640>.RefCounterLock lock = dataLock640.lock(key);
		try {
			return backend.contains(key);
		} finally {
			dataLock640.unlock(lock);
		}
	}

	public Map<Number640, Data> get(Number640 from, Number640 to, SimpleBloomFilter<Number160> contentBloomFilter,
	        SimpleBloomFilter<Number160> versionBloomFilter, int limit, boolean ascending, boolean isBloomFilterAnd) {
		KeyLock<?>.RefCounterLock lock = findAndLock(from, to);
		try {
			NavigableMap<Number640, Data> tmp = backend.subMap(from, to, limit, ascending);
			Iterator<Map.Entry<Number640, Data>> iterator = tmp.entrySet().iterator();

			while (iterator.hasNext()) {
				Map.Entry<Number640, Data> entry = iterator.next();

				if (entry.getValue().hasPrepareFlag()) {
					iterator.remove();
					continue;
				}

				if (isBloomFilterAnd) {
					if (contentBloomFilter != null && !contentBloomFilter.contains(entry.getKey().getContentKey())) {
						iterator.remove();
						continue;
					}
					if (versionBloomFilter != null && !versionBloomFilter.contains(entry.getValue().hash())) {
						iterator.remove();
					}
				} else {
					if (contentBloomFilter != null && contentBloomFilter.contains(entry.getKey().getContentKey())) {
						iterator.remove();
						continue;
					}
					if (versionBloomFilter != null && versionBloomFilter.contains(entry.getValue().hash())) {
						iterator.remove();
					}
				}
			}

			return tmp;
		} finally {
			lock.unlock();
		}
	}

	private KeyLock<?>.RefCounterLock findAndLock(Number640 from, Number640 to) {
		if (!from.getLocationKey().equals(to.getLocationKey())) {
			// everything is different, return a 640 lock
			KeyLock<Storage>.RefCounterLock lock = dataLock.lock(backend);
			return lock;
		} else if (!from.getDomainKey().equals(to.getDomainKey())) {
			// location key is the same, rest is different
			KeyLock<Number160>.RefCounterLock lock = dataLock160.lock(from.getLocationKey());
			return lock;
		} else if (!from.getContentKey().equals(to.getContentKey())) {
			// location and domain key are same, rest is different
			KeyLock<Number320>.RefCounterLock lock = dataLock320.lock(from.locationAndDomainKey());
			return lock;
		} else if (!from.getVersionKey().equals(to.getVersionKey())) {
			// location, domain, and content key are the same, rest is different
			KeyLock<Number480>.RefCounterLock lock = dataLock480.lock(from.locationDomainAndContentKey());
			return lock;
		} else {
			KeyLock<Number640>.RefCounterLock lock = dataLock640.lock(from);
			return lock;
		}
	}

	public SortedMap<Number640, Data> removeReturnData(Number640 from, Number640 to, PublicKey publicKey) {
		KeyLock<?>.RefCounterLock lock = findAndLock(from, to);
		try {
			Map<Number640, Data> tmp = backend.subMap(from, to, -1, true);

			for (Number640 key : tmp.keySet()) {
				// fail fast, as soon as we want to remove 1 domain that we
				// cannot, abort
				if (!canClaimDomain(key.locationAndDomainKey(), publicKey)) {
					return null;
				}
				if (!canClaimEntry(key.locationDomainAndContentKey(), publicKey)) {
					return null;
				}
			}
			SortedMap<Number640, Data> result = backend.remove(from, to, true);
			for (Map.Entry<Number640, Data> entry : result.entrySet()) {
				Data data = entry.getValue();
				if (data.publicKey() == null || data.publicKey().equals(publicKey)) {
					backend.removeTimeout(entry.getKey());
					backend.removeResponsibility((entry.getKey().getLocationKey()));
				}
			}
			return result;
		} finally {
			lock.unlock();
		}
	}

	public SortedMap<Number640, Byte> removeReturnStatus(Number640 from, Number640 to, PublicKey publicKey) {
		KeyLock<?>.RefCounterLock lock = findAndLock(from, to);
		try {
			Map<Number640, Data> tmp = backend.subMap(from, to, -1, true);
			SortedMap<Number640, Byte> result = new TreeMap<Number640, Byte>();
			for (Number640 key : tmp.keySet()) {
				Pair<Data, Enum<?>> pair = remove(key, publicKey, false);
				result.put(key, (byte) pair.element1().ordinal());
			}
			return result;
		} finally {
			lock.unlock();
		}
	}

	public void checkTimeout() {
		long time = Timings.currentTimeMillis();
		Collection<Number640> toRemove = backend.subMapTimeout(time);
		if (toRemove.size() > 0) {
			for (Number640 key : toRemove) {
				KeyLock<Number640>.RefCounterLock lock = dataLock640.lock(key);
				try {
					backend.remove(key, false);
					backend.removeTimeout(key);
				} finally {
					lock.unlock();
				}
				// remove responsibility if we don't have any data stored under
				// locationkey
				Number160 locationKey = key.getLocationKey();
				KeyLock<Number160>.RefCounterLock lock1 = dataLock160.lock(locationKey);
				try {
					if (isEmpty(locationKey)) {
						backend.removeResponsibility(locationKey);
					}
				} finally {
					lock1.unlock();
				}
			}
		}
	}

	private boolean isEmpty(Number160 locationKey) {
		Number640 from = new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO);
		Number640 to = new Number640(locationKey, Number160.MAX_VALUE, Number160.MAX_VALUE, Number160.MAX_VALUE);
		Map<Number640, Data> tmp = backend.subMap(from, to, 1, false);
		return tmp.size() == 0;
	}

	public DigestInfo digest(Number640 from, Number640 to, int limit, boolean ascending) {
		DigestInfo digestInfo = new DigestInfo();
		KeyLock<?>.RefCounterLock lock = findAndLock(from, to);
		try {
			Map<Number640, Data> tmp = backend.subMap(from, to, limit, ascending);
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

	public DigestInfo digest(Number320 locationAndDomainKey, SimpleBloomFilter<Number160> keyBloomFilter,
	        SimpleBloomFilter<Number160> contentBloomFilter, int limit, boolean ascending, boolean isBloomFilterAnd) {
		DigestInfo digestInfo = new DigestInfo();
		KeyLock<Number320>.RefCounterLock lock = dataLock320.lock(locationAndDomainKey);
		try {
			Number640 from = new Number640(locationAndDomainKey, Number160.ZERO, Number160.ZERO);
			Number640 to = new Number640(locationAndDomainKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
			Map<Number640, Data> tmp = backend.subMap(from, to, limit, ascending);
			for (Map.Entry<Number640, Data> entry : tmp.entrySet()) {
				if (isBloomFilterAnd) {
					if (keyBloomFilter == null || keyBloomFilter.contains(entry.getKey().getContentKey())) {
						if (contentBloomFilter == null || contentBloomFilter.contains(entry.getValue().hash())) {
							digestInfo.put(entry.getKey(), entry.getValue().basedOnSet());
						}
					}
				} else {
					if (keyBloomFilter == null || !keyBloomFilter.contains(entry.getKey().getContentKey())) {
						if (contentBloomFilter == null || !contentBloomFilter.contains(entry.getValue().hash())) {
							digestInfo.put(entry.getKey(),entry.getValue().basedOnSet());
						}
					}
				}
			}
			return digestInfo;
		} finally {
			lock.unlock();
		}
	}

	public DigestInfo digest(Collection<Number640> number640s) {
		DigestInfo digestInfo = new DigestInfo();
		for (Number640 number640 : number640s) {
			KeyLock<Number640>.RefCounterLock lock = dataLock640.lock(number640);
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
				if (canProtectDomain(key.getDomainKey(), publicKey)) {
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
			if (canClaimEntry(key, publicKeyMessage)) {
				if (canProtectEntry(key.getDomainKey(), publicKeyMessage)) {
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
		if (getProtectionDomainMode() == ProtectionMode.MASTER_PUBLIC_KEY && publicKey != null) {
			// if the hash of the public key is the same as the domain, we can
			// overwrite
			return isMine(domainKey, publicKey);
		}
		return false;
	}

	private boolean foreceOverrideEntry(Number160 entryKey, PublicKey publicKey) {
		// we are in public key mode
		if (getProtectionEntryMode() == ProtectionMode.MASTER_PUBLIC_KEY && publicKey != null
		        && publicKey.getEncoded() != null) {
			// if the hash of the public key is the same as the domain, we can
			// overwrite
			return isMine(entryKey, publicKey);
		}
		return false;
	}

	private boolean canClaimDomain(Number320 key, PublicKey publicKey) {
		boolean domainProtectedByOthers = backend.isDomainProtectedByOthers(key, publicKey);
		boolean domainOverridableByMe = foreceOverrideDomain(key.getDomainKey(), publicKey);
		return !domainProtectedByOthers || domainOverridableByMe;
	}

	private boolean canClaimEntry(Number480 key, PublicKey publicKey) {
		boolean entryProtectedByOthers = backend.isEntryProtectedByOthers(key, publicKey);
		boolean entryOverridableByMe = foreceOverrideEntry(key.getContentKey(), publicKey);
		return !entryProtectedByOthers || entryOverridableByMe;
	}

	private boolean canProtectDomain(Number160 domainKey, PublicKey publicKey) {
		if (isDomainRemoved(domainKey)) {
			return false;
		}
		if (getProtectionDomainEnable() == ProtectionEnable.ALL) {
			return true;
		} else if (getProtectionDomainEnable() == ProtectionEnable.NONE) {
			// only if we have the master key
			return foreceOverrideDomain(domainKey, publicKey);
		}
		return false;
	}

	private boolean canProtectEntry(Number160 contentKey, PublicKey publicKey) {
		if (getProtectionEntryEnable() == ProtectionEnable.ALL) {
			return true;
		} else if (getProtectionEntryEnable() == ProtectionEnable.NONE) {
			// only if we have the master key
			return foreceOverrideEntry(contentKey, publicKey);
		}
		return false;
	}

	private static boolean isMine(Number160 key, PublicKey publicKey) {
		return key.equals(Utils.makeSHAHash(publicKey.getEncoded()));
	}

	public KeyLock<Storage> getLockStorage() {
		return dataLock;
	}

	public KeyLock<Number160> getLockNumber160() {
		return dataLock160;
	}

	public KeyLock<Number320> getLockNumber320() {
		return dataLock320;
	}

	public KeyLock<Number480> getLockNumber480() {
		return dataLock480;
	}

	public Collection<Number160> findContentForResponsiblePeerID(Number160 peerID) {
		Collection<Number160> contentIDs = backend.findContentForResponsiblePeerID(peerID);
        if (contentIDs == null) {
            return Collections.<Number160> emptyList();
        } else {
            KeyLock<Number160>.RefCounterLock lock = responsibilityLock.lock(peerID);
            try {
                return new ArrayList<Number160>(contentIDs);
            } finally {
                responsibilityLock.unlock(lock);
            }
        }
	}
	
	public Collection<Number160> findPeerIDsForResponsibleContent(Number160 locationKey) {
		Collection<Number160> peerIDs = backend.findPeerIDsForResponsibleContent(locationKey);
        if (peerIDs == null) {
            return Collections.<Number160> emptyList();
        } else {
            KeyLock<Number160>.RefCounterLock lock = responsibilityLock.lock(locationKey);
            try {
                return new ArrayList<Number160>(peerIDs);
            } finally {
                responsibilityLock.unlock(lock);
            }
        }
	}
	
	public boolean updateResponsibilities(Number160 locationKey, Number160 peerId) {
		KeyLock<Number160>.RefCounterLock lock1 = responsibilityLock.lock(peerId);
        try {
            return backend.updateResponsibilities(locationKey, peerId);
        } finally {
            responsibilityLock.unlock(lock1);
        }
	}
	
	public void removeResponsibility(Number160 locationKey) {
		// TODO add locks
		backend.removeResponsibility(locationKey);
	}

	public void removeResponsibility(Number160 locationKey, Number160 peerId) {
		// TODO add locks
		backend.removeResponsibility(locationKey, peerId);
	}

	private class StorageMaintenanceTask implements Runnable {
		@Override
		public void run() {
			checkTimeout();
		}
	}

	public void init(ScheduledExecutorService timer, int storageIntervalMillis) {
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
		boolean found = false;
		KeyLock<Number640>.RefCounterLock lock = dataLock640.lock(key);
		try {
			if (!securityEntryCheck(key.locationDomainAndContentKey(), publicKey, newData.publicKey(),
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
				found = backend.put(key, data);
			}
		} finally {
			dataLock640.unlock(lock);
		}
		return found ? PutStatus.OK : PutStatus.NOT_FOUND;
	}

	public Enum<?> putConfirm(PublicKey publicKey, Number640 key, Data newData) {
		boolean found = false;
		KeyLock<Number640>.RefCounterLock lock = dataLock640.lock(key);
		try {
			if (!securityEntryCheck(key.locationDomainAndContentKey(), publicKey, newData.publicKey(),
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
				found = backend.put(key, data);
			}
		} finally {
			dataLock640.unlock(lock);
		}
		return found ? PutStatus.OK : PutStatus.NOT_FOUND;
	}

	/**
	 * Checks for version forks. Iterates through the backend and checks if the
	 * given based on key appears also as a based on key of another entry.
	 * 
	 * @param basedOnKey
	 * @return <code>true</code> if a version fork is present,
	 *         <code>false</code> if not
	 */
	private boolean checkForForks(Number160 basedOnKey) {
		NavigableMap<Number640, Data> map = backend.map();
		for (Entry<Number640, Data> entry: map.entrySet()) {
			for (Number160 bKey: entry.getValue().basedOnSet()) {
				if (bKey.equals(basedOnKey)) {
					return true;
				}
			}
		}
		return false;
	}
}
