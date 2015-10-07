/*
 * Copyright 2012 Thomas Bocek
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
//import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageMemory implements Storage {

	public static final int DEFAULT_STORAGE_CHECK_INTERVAL= 60 * 1000;

    private static final Logger LOG = LoggerFactory.getLogger(StorageMemory.class);
    

    // Core
    final private NavigableMap<Number640, Data> dataMap = new ConcurrentSkipListMap<Number640, Data>();

    // Maintenance
    final private Map<Number640, Long> timeoutMap = new ConcurrentHashMap<Number640, Long>();
    final private ConcurrentSkipListMap<Long, Set<Number640>> timeoutMapRev = new ConcurrentSkipListMap<Long, Set<Number640>>();

    // Protection
    final private Map<Number320, PublicKey> protectedMap = new ConcurrentHashMap<Number320, PublicKey>();
    final private Map<Number480, PublicKey> entryMap = new ConcurrentHashMap<Number480, PublicKey>();

    // Responsibility
    final private Map<Number160, Number160> responsibilityMap = new ConcurrentHashMap<Number160, Number160>();
    final private Map<Number160, Set<Number160>> responsibilityMapRev = new ConcurrentHashMap<Number160, Set<Number160>>();
    
    final int storageCheckIntervalMillis;
    
    
    public StorageMemory() {
    	this(DEFAULT_STORAGE_CHECK_INTERVAL);
    }

    public StorageMemory(int storageCheckIntervalMillis) {
    	this.storageCheckIntervalMillis = storageCheckIntervalMillis;
	}

	// Core
    @Override
    public Data put(Number640 key, Data value) {
    	return dataMap.put(key, value);
    }

    @Override
    public Data get(Number640 key) {
        return dataMap.get(key);
    }

    @Override
    public boolean contains(Number640 key) {
        return dataMap.containsKey(key);
    }

    @Override
    public int contains(Number640 fromKey, Number640 toKey) {
        NavigableMap<Number640, Data> tmp = dataMap.subMap(fromKey, true, toKey, true);
        return tmp.size();
    }

    @Override
    public Data remove(Number640 key, boolean returnData) {
    	return dataMap.remove(key);
    }

    @Override
    public NavigableMap<Number640, Data> remove(Number640 fromKey, Number640 toKey) {
    	NavigableMap<Number640, Data> tmp = dataMap.subMap(fromKey, true, toKey, true);
        final NavigableMap<Number640, Data> retVal = new TreeMap<Number640, Data>();
        retVal.putAll(tmp);
        tmp.clear();
        return retVal;
    }

    @Override
    public NavigableMap<Number640, Data> subMap(Number640 fromKey, Number640 toKey) {	
    	return dataMap.subMap(fromKey, true, toKey, true);
    }

    @Override
    public NavigableMap<Number640, Data> map() {    	
        return dataMap;
    }

    // Maintenance
    @Override
    public void addTimeout(Number640 key, long expiration) {
        Long oldExpiration = timeoutMap.put(key, expiration);
        Set<Number640> tmp = putIfAbsent2(expiration,
                Collections.newSetFromMap(new ConcurrentHashMap<Number640, Boolean>()));
        tmp.add(key);
        if (oldExpiration == null) {
            return;
        }
        removeRevTimeout(key, oldExpiration);
    }
    
    //TODO: unnecessary creation of object
    private Set<Number640> putIfAbsent2(long expiration, Set<Number640> hashSet) {
        Set<Number640> timeouts = timeoutMapRev.putIfAbsent(expiration, hashSet);
        return timeouts == null ? hashSet : timeouts;
    }

    @Override
    public void removeTimeout(Number640 key) {
        Long expiration = timeoutMap.remove(key);
        if (expiration == null) {
            return;
        }
        removeRevTimeout(key, expiration);
    }

    private void removeRevTimeout(Number640 key, Long expiration) {
        Set<Number640> tmp = timeoutMapRev.get(expiration);
        if (tmp != null) {
            tmp.remove(key);
            if (tmp.isEmpty()) {
                timeoutMapRev.remove(expiration);
            }
        }
    }

    @Override
    public Collection<Number640> subMapTimeout(long to) {
        SortedMap<Long, Set<Number640>> tmp = timeoutMapRev.subMap(0L, to);
        Collection<Number640> toRemove = new ArrayList<Number640>();
        for (Set<Number640> set : tmp.values()) {
            toRemove.addAll(set);
        }
        return toRemove;
    }

    // Protection
    @Override
    public boolean protectDomain(Number320 key, PublicKey publicKey) {
        protectedMap.put(key, publicKey);
        return true;
    }

    @Override
    public boolean isDomainProtectedByOthers(Number320 key, PublicKey publicKey) {
        PublicKey other = protectedMap.get(key);
        if (other == null) {
        	LOG.debug("domain {} not protected", key);
            return false;
        }
        final boolean retVal = !other.equals(publicKey);
        LOG.debug("domain {} protected: {}", key, retVal);
        return retVal;
    }

    

	@Override
	public Number160 findPeerIDsForResponsibleContent(Number160 locationKey) {
		return responsibilityMap.get(locationKey);
	}

    @Override
    public Collection<Number160> findContentForResponsiblePeerID(Number160 peerID) {
		return responsibilityMapRev.get(peerID);
    }

	@Override
	public boolean updateResponsibilities(Number160 locationKey, Number160 peerId) {
		final Number160 oldPeerID =  responsibilityMap.put(locationKey, peerId);
		final boolean hasChanged;
		if(oldPeerID != null) {
			if(oldPeerID.equals(peerId)) {
				hasChanged = false;
			} else {
				removeRevResponsibility(oldPeerID, locationKey);
				hasChanged = true;
			}
		} else {
			hasChanged = true;
		}
		Set<Number160> contentIDs = responsibilityMapRev.get(peerId);
		if(contentIDs == null) {
			contentIDs = Collections.newSetFromMap(new ConcurrentHashMap<Number160, Boolean>()); 
			responsibilityMapRev.put(peerId, contentIDs);
		}
		contentIDs.add(locationKey);
		LOG.debug("Update {} is responsible for key {}.", peerId, locationKey);
		return hasChanged;
	}

    @Override
    public void removeResponsibility(Number160 locationKey) {
    	 Number160 peerId = responsibilityMap.remove(locationKey);
    	 if(peerId != null) {
			removeRevResponsibility(peerId, locationKey);
			LOG.debug("Remove responsiblity for {}.", locationKey);
    	 }
    }
    
    private void removeRevResponsibility(Number160 peerId, Number160 locationKey) {
        Set<Number160> contentIDs = responsibilityMapRev.get(peerId);
        if (contentIDs != null) {
            contentIDs.remove(locationKey);
            if (contentIDs.isEmpty()) {
                responsibilityMapRev.remove(peerId);
            }
        }
    }

    // Misc
    @Override
    public void close() {
    	for(Data data:dataMap.values()) {
    		data.release();
    	}
        dataMap.clear();
        protectedMap.clear();
        timeoutMap.clear();
        timeoutMapRev.clear();
    }

	@Override
    public boolean protectEntry(Number480 key, PublicKey publicKey) {
		entryMap.put(key, publicKey);
	    return true;
    }

	@Override
    public boolean isEntryProtectedByOthers(Number480 key, PublicKey publicKey) {
		PublicKey other = entryMap.get(key);
        if (other == null) {
            return false;
        }
        return !other.equals(publicKey);
    }

	@Override
    public int storageCheckIntervalMillis() {
	    return storageCheckIntervalMillis;
    }
}
