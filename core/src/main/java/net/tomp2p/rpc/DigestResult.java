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

import java.util.NavigableMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public class DigestResult {
    private SimpleBloomFilter<Number160> keyBloomFilter;

    private SimpleBloomFilter<Number160> contentBloomFilter;

    private NavigableMap<Number640, Number160> keyDigest;

    public DigestResult(SimpleBloomFilter<Number160> keyBloomFilter, SimpleBloomFilter<Number160> contentBloomFilter) {
        this.keyBloomFilter = keyBloomFilter;
        this.contentBloomFilter = contentBloomFilter;
    }

    public DigestResult(NavigableMap<Number640, Number160> keyDigest) {
        this.keyDigest = keyDigest;
    }

    public SimpleBloomFilter<Number160> getKeyBloomFilter() {
        return keyBloomFilter;
    }

    public void setKeyBloomFilter(SimpleBloomFilter<Number160> keyBloomFilter) {
        this.keyBloomFilter = keyBloomFilter;
    }

    public SimpleBloomFilter<Number160> getContentBloomFilter() {
        return contentBloomFilter;
    }

    public void setContentBloomFilter(SimpleBloomFilter<Number160> contentBloomFilter) {
        this.contentBloomFilter = contentBloomFilter;
    }

    public NavigableMap<Number640, Number160> getKeyDigest() {
        return keyDigest;
    }

    public void setKeyDigest(NavigableMap<Number640, Number160> keyDigest) {
        this.keyDigest = keyDigest;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        if (keyDigest != null) {
            hashCode ^= keyDigest.hashCode();
        }
        if (keyBloomFilter != null) {
            hashCode ^= keyBloomFilter.hashCode();
        }
        if (contentBloomFilter != null) {
            hashCode ^= contentBloomFilter.hashCode();
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
        boolean test1 = keyDigest == o.keyDigest;
        if (!test1 && keyDigest != null && o.keyDigest != null) {
            test1 = keyDigest.equals(o.keyDigest);
        }
        boolean test2 = keyBloomFilter == o.keyBloomFilter;
        if (!test2 && keyBloomFilter != null && o.keyBloomFilter != null) {
            test2 = keyBloomFilter.equals(o.keyBloomFilter);
        }
        boolean test3 = contentBloomFilter == o.contentBloomFilter;
        if (!test3 && contentBloomFilter != null && o.contentBloomFilter != null) {
            test3 = contentBloomFilter.equals(o.contentBloomFilter);
        }
        return test1 && test2 && test3;
    }
}
