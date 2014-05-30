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

package net.tomp2p.storage;

import java.io.File;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;

import org.mapdb.DB;
import org.mapdb.DBMaker;

public class StorageDisk implements Storage {
    // Core
    final private NavigableMap<Number640, Data> dataMap;
    // Maintenance
    final private Map<Number640, Long> timeoutMap;
    final private ConcurrentNavigableMap<Long, Set<Number640>> timeoutMapRev;
    // Protection
    final private Map<Number320, PublicKey> protectedDomainMap;
    final private Map<Number480, PublicKey> protectedEntryMap;
    // Responsibility
    final private Map<Number160, Number160> responsibilityMap;
    final private Map<Number160, Set<Number160>> responsibilityMapRev;
    
    final private DB db;
    
    //for full control
    public StorageDisk(DB db, Number160 peerId, File path) {
    	this.db = db;
    	DataSerializer dataSerializer = new DataSerializer(path);
    	this.dataMap = db.createTreeMap("dataMap_" + peerId.toString()).valueSerializer(dataSerializer).make();
    	this.timeoutMap = db.createTreeMap("timeoutMap_" + peerId.toString()).valueSerializer(dataSerializer).make();
    	this.timeoutMapRev = db.createTreeMap("timeoutMapRev_" + peerId.toString()).valueSerializer(dataSerializer).make();
    	this.protectedDomainMap = db.createTreeMap("protectedDomainMap_" + peerId.toString()).valueSerializer(dataSerializer).make();
    	this.protectedEntryMap = db.createTreeMap("protectedEntryMap_" + peerId.toString()).valueSerializer(dataSerializer).make();
    	this.responsibilityMap = db.createTreeMap("responsibilityMap_" + peerId.toString()).valueSerializer(dataSerializer).make();
    	this.responsibilityMapRev = db.createTreeMap("responsibilityMapRev_" + peerId.toString()).valueSerializer(dataSerializer).make();
    }
    
    //set parameter to a reasonable default
    public StorageDisk(File path, Number160 peerId) {
    	this(DBMaker.newFileDB(new File(path, "tomp2p")).transactionDisable().closeOnJvmShutdown().make(), peerId, path);
    }
    
    @Override
    public boolean put(Number640 key, Data value) {
		dataMap.put(key, value);
		db.commit();
        return true;
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
    public int contains(Number640 from, Number640 to) {
		 NavigableMap<Number640, Data> tmp = dataMap.subMap(from, true, to, true);
	     return tmp.size();
    }
    
    @Override
    public Data remove(Number640 key, boolean returnData) {
    	Data retVal = dataMap.remove(key);
		db.commit();
		return retVal;
    }
    
    @Override
    public NavigableMap<Number640, Data> remove(Number640 from, Number640 to, boolean returnData) {
		NavigableMap<Number640, Data> tmp = dataMap.subMap(from, true, to, true);
        NavigableMap<Number640, Data> copy = new TreeMap<Number640, Data>(tmp);
        tmp.clear();
        db.commit();
        return copy;
    }
    
    @Override
    public NavigableMap<Number640, Data> subMap(Number640 from, Number640 to, int limit, boolean ascending) {
		NavigableMap<Number640, Data> tmp = dataMap.subMap(from, true, to, true);
        if (limit < 0) {
            return new TreeMap<Number640, Data>(ascending ? tmp : tmp.descendingMap());
        } else {
            NavigableMap<Number640, Data> retVal = new TreeMap<Number640, Data>();
            limit = Math.min(limit, tmp.size());
            Iterator<Map.Entry<Number640, Data>> iterator = ascending ? tmp.entrySet().iterator() : tmp
                    .descendingMap().entrySet().iterator();
            for (int i = 0; iterator.hasNext() && i < limit; i++) {
                Map.Entry<Number640, Data> entry = iterator.next();
                retVal.put(entry.getKey(), entry.getValue());
            }
            return retVal;
        }
    }
    
    @Override
    public NavigableMap<Number640, Data> map() {
		return new TreeMap<Number640, Data>(dataMap);
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
         db.commit();
    }
 	
 	@Override
    public void removeTimeout(Number640 key) {
		Long expiration = timeoutMap.remove(key);
        if (expiration == null) {
            return;
        }
        removeRevTimeout(key, expiration);
        db.commit();
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
 	
 	private Set<Number640> putIfAbsent2(long expiration, Set<Number640> hashSet) {
        Set<Number640> timeouts = timeoutMapRev.putIfAbsent(expiration, hashSet);
        return timeouts == null ? hashSet : timeouts;
    }
    
 	// Responsibility
	@Override
    public Number160 findPeerIDForResponsibleContent(Number160 locationKey) {
		return responsibilityMap.get(locationKey);
    }

	@Override
    public Collection<Number160> findContentForResponsiblePeerID(Number160 peerID) {
		return responsibilityMapRev.get(peerID);
    }

	@Override
    public boolean updateResponsibilities(Number160 locationKey, Number160 peerId) {
		boolean isNew = true;
        Number160 oldPeerId = responsibilityMap.put(locationKey, peerId);
        // add to the reverse map
        Set<Number160> contentIDs = putIfAbsent1(peerId, new HashSet<Number160>());
        contentIDs.add(locationKey);
        if (oldPeerId != null) {
            isNew = !oldPeerId.equals(peerId);
            if (isNew) {
                removeRevResponsibility(oldPeerId, locationKey);
            }
        }
        db.commit();
        return isNew;
    }
	
	private Set<Number160> putIfAbsent1(Number160 peerId, Set<Number160> hashSet) {
        Set<Number160> contentIDs = ((ConcurrentMap<Number160, Set<Number160>>) responsibilityMapRev).putIfAbsent(
                peerId, hashSet);
        return contentIDs == null ? hashSet : contentIDs;
    }

	@Override
    public void removeResponsibility(Number160 locationKey) {
		Number160 peerId = responsibilityMap.remove(locationKey);
		if(peerId != null) {
   	 		removeRevResponsibility(peerId, locationKey);
   	 		db.commit();
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
	    db.close();	    
    }
	
	// Protection Domain
	@Override
    public boolean protectDomain(Number320 key, PublicKey publicKey) {
		protectedDomainMap.put(key, publicKey);
        return true;
    }

	@Override
    public boolean isDomainProtectedByOthers(Number320 key, PublicKey publicKey) {
		PublicKey other = protectedDomainMap.get(key);
        if (other == null) {
            return false;
        }
        return !other.equals(publicKey);
    }

	// Protection Entry
	@Override
    public boolean protectEntry(Number480 key, PublicKey publicKey) {
		protectedEntryMap.put(key, publicKey);
	    return true;
    }

	@Override
    public boolean isEntryProtectedByOthers(Number480 key, PublicKey publicKey) {
		PublicKey other = protectedEntryMap.get(key);
        if (other == null) {
            return false;
        }
        return !other.equals(publicKey);
    }
}