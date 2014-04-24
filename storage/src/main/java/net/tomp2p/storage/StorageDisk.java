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
import java.util.Collection;
import java.util.NavigableMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageDisk implements Storage {
    final private static Logger LOG = LoggerFactory.getLogger(StorageDisk.class);
    
    final private DB db;
    
    //for full control
    public StorageDisk(DB db) {
    	this.db = db;
    }
    
    //set parameter to a reasonable default
    public StorageDisk(File file) {
    	db = DBMaker.newFileDB(file).make();
    }

	@Override
    public Number160 findPeerIDForResponsibleContent(Number160 locationKey) {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    public Collection<Number160> findContentForResponsiblePeerID(Number160 peerID) {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    public boolean updateResponsibilities(Number160 locationKey, Number160 peerId) {
	    // TODO Auto-generated method stub
	    return false;
    }

	@Override
    public void removeResponsibility(Number160 locationKey) {
	    // TODO Auto-generated method stub
	    
    }

	@Override
    public boolean put(Number640 key, Data value) {
	    // TODO Auto-generated method stub
	    return false;
    }

	@Override
    public Data get(Number640 key) {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    public boolean contains(Number640 key) {
	    // TODO Auto-generated method stub
	    return false;
    }

	@Override
    public int contains(Number640 from, Number640 to) {
	    // TODO Auto-generated method stub
	    return 0;
    }

	@Override
    public Data remove(Number640 key, boolean returnData) {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    public NavigableMap<Number640, Data> remove(Number640 from, Number640 to, boolean returnData) {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    public NavigableMap<Number640, Data> subMap(Number640 from, Number640 to, int limit, boolean ascending) {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    public NavigableMap<Number640, Data> map() {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    public void close() {
	    // TODO Auto-generated method stub
	    
    }

	@Override
    public void addTimeout(Number640 key, long expiration) {
	    // TODO Auto-generated method stub
	    
    }

	@Override
    public void removeTimeout(Number640 key) {
	    // TODO Auto-generated method stub
	    
    }

	@Override
    public Collection<Number640> subMapTimeout(long to) {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    public boolean protectDomain(Number320 key, PublicKey publicKey) {
	    // TODO Auto-generated method stub
	    return false;
    }

	@Override
    public boolean isDomainProtectedByOthers(Number320 key, PublicKey publicKey) {
	    // TODO Auto-generated method stub
	    return false;
    }

	@Override
    public boolean protectEntry(Number480 key, PublicKey publicKey) {
	    // TODO Auto-generated method stub
	    return false;
    }

	@Override
    public boolean isEntryProtectedByOthers(Number480 key, PublicKey publicKey) {
	    // TODO Auto-generated method stub
	    return false;
    }

   
}