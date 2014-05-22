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

package net.tomp2p.rpc;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class DigestResult {
    final private SimpleBloomFilter<Number160> contentBloomFilter;
    final private SimpleBloomFilter<Number160> versoinBloomFilter;

    final private NavigableMap<Number640, Set<Number160>> keyDigest;
    
    final private Map<Number640, Data> dataMap;

    public DigestResult(SimpleBloomFilter<Number160> contentBloomFilter, SimpleBloomFilter<Number160> versoinBloomFilter) {
        this.contentBloomFilter = contentBloomFilter;
        this.versoinBloomFilter = versoinBloomFilter;
        this.keyDigest = null;
        this.dataMap = null;
    }

    public DigestResult(NavigableMap<Number640, Set<Number160>> keyDigest) {
        this.keyDigest = keyDigest;
        this.contentBloomFilter = null;
        this.versoinBloomFilter = null;
        this.dataMap = null;
    }

    public DigestResult(Map<Number640, Data> dataMap) {
	    this.dataMap = dataMap;
	    this.keyDigest = null;
	    this.contentBloomFilter = null;
        this.versoinBloomFilter = null;
    }

	public SimpleBloomFilter<Number160> contentBloomFilter() {
        return contentBloomFilter;
    }

    public SimpleBloomFilter<Number160> versoinBloomFilter() {
        return versoinBloomFilter;
    }

    public NavigableMap<Number640, Set<Number160>> keyDigest() {
        return keyDigest;
    }

    public Map<Number640, Data> dataMap() {
        return dataMap;
    }
    

    @Override
    public int hashCode() {
        int hashCode = 0;
        if (keyDigest != null) {
            hashCode ^= keyDigest.hashCode();
        }
        if (contentBloomFilter != null) {
            hashCode ^= contentBloomFilter.hashCode();
        }
        if (versoinBloomFilter != null) {
            hashCode ^= versoinBloomFilter.hashCode();
        }
        if	(dataMap!=null) {
        	hashCode ^= dataMap.hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DigestResult)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        DigestResult o = (DigestResult) obj;
        
        return 	Utils.equals(keyDigest, o.keyDigest) &&
        		Utils.equals(contentBloomFilter, o.contentBloomFilter) &&
        		Utils.equals(versoinBloomFilter, o.versoinBloomFilter) &&
        		Utils.equals(dataMap, o.dataMap);
    }
}
