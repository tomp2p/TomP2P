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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;

import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.dht.Storage;
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
    
    final private int storageCheckIntervalMillis;
    
    //for full control
    public StorageDisk(DB db, Number160 peerId, File path, SignatureFactory signatureFactory, int storageCheckIntervalMillis) {
    	this.db = db;
    	DataSerializer dataSerializer = new DataSerializer(path, signatureFactory);
    	this.dataMap = db.createTreeMap("dataMap_" + peerId.toString()).valueSerializer(dataSerializer).makeOrGet();
    	this.timeoutMap = db.createTreeMap("timeoutMap_" + peerId.toString()).makeOrGet();
    	this.timeoutMapRev = db.createTreeMap("timeoutMapRev_" + peerId.toString()).makeOrGet();
    	this.protectedDomainMap = db.createTreeMap("protectedDomainMap_" + peerId.toString()).makeOrGet();
    	this.protectedEntryMap = db.createTreeMap("protectedEntryMap_" + peerId.toString()).makeOrGet();
    	this.responsibilityMap = db.createTreeMap("responsibilityMap_" + peerId.toString()).makeOrGet();
    	this.responsibilityMapRev = db.createTreeMap("responsibilityMapRev_" + peerId.toString()).makeOrGet();
    	this.storageCheckIntervalMillis = storageCheckIntervalMillis;
    }
    
    //set parameter to a reasonable default
    public StorageDisk(Number160 peerId, File path, SignatureFactory signatureFactory) {
    	this(DBMaker.newFileDB(new File(path, "tomp2p")).transactionDisable().closeOnJvmShutdown().make(), 
    			peerId, path, signatureFactory, 60 * 1000);
    }
    
    @Override
    public Data put(Number640 key, Data value) {
		Data oldData = dataMap.put(key, value);
		db.commit();
        return oldData;
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
    public NavigableMap<Number640, Data> remove(Number640 from, Number640 to) {
		NavigableMap<Number640, Data> tmp = dataMap.subMap(from, true, to, true);
		final NavigableMap<Number640, Data> retVal = new TreeMap<Number640, Data>();
        for(final Map.Entry<Number640, Data> entry:tmp.entrySet()) {
        	retVal.put(entry.getKey(), entry.getValue());
        }
		
        tmp.clear();
        db.commit();
        return retVal;
    }
    
    @Override
    public NavigableMap<Number640, Data> subMap(Number640 from, Number640 to) {
		return dataMap.subMap(from, true, to, true);
    }
    
    @Override
    public NavigableMap<Number640, Data> map() {    	
		return dataMap;
    }
    
    // Maintenance
	@Override
	public void addTimeout(Number640 key, long expiration) {
		Long oldExpiration = timeoutMap.put(key, expiration);
		putIfAbsent2(expiration, key);
		if (oldExpiration == null) {
			return;
		}
		removeRevTimeout(key, oldExpiration);
		db.commit();
	}
 	
 	private void putIfAbsent2(long expiration, Number640 key) {
        Set<Number640> timeouts = timeoutMapRev.get(expiration);
        if(timeouts == null) {
        	timeouts = Collections.newSetFromMap(new ConcurrentHashMap<Number640, Boolean>());
        }
        timeouts.add(key);
        timeoutMapRev.put(expiration, timeouts);
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
            } else {
            	timeoutMapRev.put(expiration, tmp);
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
 	
 	
    
 	// Responsibility
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
		final Number160 oldPeerID = responsibilityMap.put(locationKey, peerId);
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
			contentIDs = new HashSet<Number160>();
		}
		contentIDs.add(locationKey);
		responsibilityMapRev.put(peerId, contentIDs);
		db.commit();
		return hasChanged;
    }

	@Override
    public void removeResponsibility(Number160 locationKey) {
		final Number160 peerId = responsibilityMap.remove(locationKey);
    	if(peerId != null) {
    		removeRevResponsibility(peerId, locationKey);
    	}
    	db.commit();
    }
	
	private void removeRevResponsibility(Number160 peerId, Number160 locationKey) {
        Set<Number160> contentIDs = responsibilityMapRev.get(peerId);
        if (contentIDs != null) {
            contentIDs.remove(locationKey);
            if (contentIDs.isEmpty()) {
                responsibilityMapRev.remove(peerId);
            } else {
            	responsibilityMapRev.put(peerId, contentIDs);
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

	@Override
    public int storageCheckIntervalMillis() {
	    return storageCheckIntervalMillis;
    }
}
