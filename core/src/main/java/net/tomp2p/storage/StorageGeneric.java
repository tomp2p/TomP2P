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
import net.tomp2p.peers.Number640;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

public abstract class StorageGeneric implements Storage {
    public enum ProtectionEnable {
        ALL, NONE
    };

    public enum ProtectionMode {
        NO_MASTER, MASTER_PUBLIC_KEY
    };

    //The number of PutStatus should never exceed 255.
    public enum PutStatus {
        OK, FAILED_NOT_ABSENT, FAILED_SECURITY, FAILED, VERSION_CONFLICT
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

    public PutStatus put(final Number640 key, Data newData,
            PublicKey publicKey, boolean putIfAbsent, boolean domainProtection) {
        boolean retVal = false;
        Lock lock = dataLock640.lock(key);
        try {
            if (!securityDomainCheck(key.getLocationKey(), key.getDomainKey(), publicKey, domainProtection)) {
                return PutStatus.FAILED;
            }
            boolean contains = contains(key);
            if (putIfAbsent && contains) {
                return PutStatus.FAILED_NOT_ABSENT;
            }
            if (contains) {
                Data oldData = get(key);
                boolean protectEntry = newData.protectedEntry();
                if (!canUpdateEntry(key.getContentKey(), oldData, newData, protectEntry)) {
                    return PutStatus.FAILED_SECURITY;
                }
            }
            retVal = put(key, newData);
            if (retVal) {
                long expiration = newData.expirationMillis();
                // handle timeout
                addTimeout(key, expiration);
            }
        } finally {
            dataLock640.unlock(key, lock);
        }
        return retVal ? PutStatus.OK : PutStatus.FAILED;
    }

    public Data remove(Number640 key, PublicKey publicKey) {
        //Number480 lockKey = new Number480(locationKey, domainKey, contentKey);
        Lock lock = dataLock640.lock(key);
        try {
            if (!canClaimDomain(key.getLocationKey(), key.getDomainKey(), publicKey)) {
                return null;
            }
            Data data = get(key);
            if (data == null) {
                return null;
            }
            if (data.publicKey() == null || data.publicKey().equals(publicKey)) {
                removeTimeout(key);
                removeContentResponsibility(key.getLocationKey());
                return remove(key);
            }
        } finally {
            dataLock640.unlock(key, lock);
        }
        return null;
    }

    public SortedMap<Number640, Data> remove(Number640 from, Number640 to, PublicKey publicKey) {
        Number320 lockKey = new Number320(locationKey, domainKey);
        Lock lock = dataLock320.lock(lockKey);
        try {
            if (!canClaimDomain(locationKey, domainKey, publicKey)) {
                return null;
            }
            SortedMap<Number480, Data> tmp = subMap(locationKey, domainKey, fromContentKey, toContentKey);
            Collection<Number480> keys = new ArrayList<Number480>(tmp.keySet());
            SortedMap<Number480, Data> result = new TreeMap<Number480, Data>();
            for (Number480 key : keys) {
                Data data = get(key.getLocationKey(), key.getDomainKey(), key.getContentKey());
                if (data.publicKey() == null || data.publicKey().equals(publicKey)) {
                    removeTimeout(key.getLocationKey(), key.getDomainKey(), key.getContentKey());
                    removeContentResponsibility(key.getLocationKey());
                    result.put(key, remove(key.getLocationKey(), key.getDomainKey(), key.getContentKey()));
                }
            }
            return result;
        } finally {
            dataLock320.unlock(lockKey, lock);
        }
    }

    public SortedMap<Number640, Data> get(Number160 locationKey, Number160 domainKey, Number160 fromContentKey,
            Number160 toContentKey) {
        Number320 lockKey = new Number320(locationKey, domainKey);
        Lock lock = dataLock320.lock(lockKey);
        try {
            return subMap(locationKey, domainKey, fromContentKey, toContentKey);
        } finally {
            dataLock320.unlock(lockKey, lock);
        }
    }

    public void checkTimeout() {
        long time = Timings.currentTimeMillis();
        Collection<Number640> toRemove = subMapTimeout(time);
        if (toRemove.size() > 0) {
            for (Number640 key : toRemove) {
                remove(key);
            }
        }
    }

    private DigestInfo digest(Number160 locationKey, Number160 domainKey) {
        DigestInfo digestInfo = new DigestInfo();
        Number320 lockKey = new Number320(locationKey, domainKey);
        Lock lock = dataLock320.lock(lockKey);
        try {
            SortedMap<Number480, Data> tmp = get(locationKey, domainKey, Number160.ZERO, Number160.MAX_VALUE);
            for (Map.Entry<Number480, Data> entry : tmp.entrySet()) {
                digestInfo.put(entry.getKey(), entry.getValue().hash());
            }
        } finally {
            dataLock320.unlock(lockKey, lock);
        }
        return digestInfo;
    }

    @Override
    public DigestInfo digest(Number160 locationKey, Number160 domainKey,
            SimpleBloomFilter<Number160> keyBloomFilter, SimpleBloomFilter<Number160> contentBloomFilter) {
        DigestInfo digestInfo = new DigestInfo();
        Number320 lockKey = new Number320(locationKey, domainKey);
        SortedMap<Number480, Data> tmp = get(locationKey, domainKey, Number160.ZERO, Number160.MAX_VALUE);
        Lock lock = dataLock320.lock(lockKey);
        try {
            for (Map.Entry<Number480, Data> entry : tmp.entrySet()) {
                if (keyBloomFilter == null || keyBloomFilter.contains(entry.getKey().getContentKey())) {
                    if (contentBloomFilter == null || contentBloomFilter.contains(entry.getValue().hash())) {
                        digestInfo.put(entry.getKey(), entry.getValue().hash());
                    }
                }
            }
        } finally {
            dataLock320.unlock(lockKey, lock);
        }
        return digestInfo;
    }
    
    public DigestInfo digest(Collection<Number640> number640s) {
        DigestInfo digestInfo = new DigestInfo();
        for (Number640 number640 : number640s) {
            Lock lock = dataLock640.lock(number640);
            try {
                if (contains(number640)) {
                    Data data = get(number640);
                    digestInfo.put(number640, data.hash());
                }
            } finally {
                dataLock640.unlock(number640, lock);
            }
        }
        return digestInfo;
    }

    @Override
    public DigestInfo digest(Number160 locationKey, Number160 domainKey, Number160 contentKey) {
        if (contentKey == null) {
            return digest(locationKey, domainKey);
        }
        DigestInfo digestInfo = new DigestInfo();
        Number480 lockKey = new Number480(locationKey, domainKey, contentKey);
        Lock lock = dataLock480.lock(lockKey);
        try {
            if (contains(locationKey, domainKey, contentKey)) {
                Data data = get(locationKey, domainKey, contentKey);
                digestInfo.put(lockKey, data.hash());
            }
        } finally {
            dataLock480.unlock(lockKey, lock);
        }

        return digestInfo;
    }

    private void removeContentResponsibility(Number160 locationKey) {
        removeResponsibility(locationKey);
    }

    private boolean canClaimDomain(Number160 locationKey, Number160 domainKey, PublicKey publicKey) {
        boolean domainProtectedByOthers = isDomainProtectedByOthers(locationKey, domainKey, publicKey);
        boolean domainOverridableByMe = foreceOverrideDomain(domainKey, publicKey);
        return !domainProtectedByOthers || domainOverridableByMe;
    }

    private boolean canProtectDomain(Number160 locationKey, Number160 domainKey, PublicKey publicKey) {
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

    private boolean securityDomainCheck(Number160 locationKey, Number160 domainKey, PublicKey publicKey,
            boolean domainProtection) {
        boolean domainProtectedByOthers = isDomainProtectedByOthers(locationKey, domainKey, publicKey);
        // I dont want to claim the domain
        if (!domainProtection) {
            // returns true if the domain is not protceted by others, otherwise
            // false if the domain is protected
            return !domainProtectedByOthers;
        } else {
            if (canClaimDomain(locationKey, domainKey, publicKey)) {
                if (canProtectDomain(locationKey, domainKey, publicKey)) {
                    return protectDomain(locationKey, domainKey, publicKey);
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
        if (getProtectionEntryMode() == ProtectionMode.MASTER_PUBLIC_KEY && publicKey != null) {
            // if the hash of the public key is the same as the domain, we can
            // overwrite
            return isMine(entryKey, publicKey);
        }
        return false;
    }

    private boolean canUpdateEntry(Number160 contentKey, Data oldData, Data newData, boolean protectEntry) {
        if (protectEntry) {
            return canProtectEntry(contentKey, oldData, newData);
        }
        return true;
    }

    private boolean canProtectEntry(Number160 contentKey, Data oldData, Data newData) {
        if (getProtectionEntryEnable() == ProtectionEnable.ALL) {
            if (oldData == null)
                return true;
            else if (oldData.publicKey() == null)
                return true;
            else {
                if (oldData.publicKey().equals(newData.publicKey()))
                    return true;
            }
        }
        // we cannot protect, but maybe we have the rigth public key
        return foreceOverrideEntry(contentKey, newData.publicKey());
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
}