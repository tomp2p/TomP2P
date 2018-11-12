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
package net.tomp2p.rpc;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import net.tomp2p.peers.Number256;

/**
 * Calculates or sets a global hash. The digest is used in two places: for routing, where a message needs to have a
 * predictable size. Thus in this case a global hash is calculated. The second usage is get() for getting a list of
 * hashes from peers. Here we don't need to restrict ourself, since we use TCP.
 * 
 * @author Thomas Bocek
 */
public class DigestInfo {
    private volatile Number256 keyDigest = null;

    private volatile Number256 contentDigest = null;

    private volatile int size = -1;

    private final NavigableMap<Object, Collection<Number256>> mapDigests = new TreeMap<Object, Collection<Number256>>();

    /**
     * Empty constructor is used to add the hashes to the list.
     */
    public DigestInfo() {
    }

    /**
     * Creates a digest with the size only.
     * 
     * @param size
     *            The number of items
     */
    public DigestInfo(final int size) {
        this.size = size;
    }

    /**
     * If a global hash has already been calculated, then this constructor is used to store those. Note that once a
     * global hash is set it cannot be unset.
     * 
     * @param keyDigest
     *            The digest of all keys
     * @param contentDigest
     *            The digest of all contents
     * @param size
     *            The number of entries
     */
    public DigestInfo(final Number256 keyDigest, final Number256 contentDigest, final int size) {
        this.keyDigest = keyDigest;
        this.contentDigest = contentDigest;
        this.size = size;
    }

    /**
     * @return Returns or calculates the global key hash. The global key hash will be calculated if the empty
     *         constructor is used.
     */
    public Number256 keyDigest() {
        if (keyDigest == null) {
            process();
        }
        return keyDigest;
    }

    /**
     * @return Returns or calculates the global content hash. The global content hash will be calculated if the empty
     *         constructor is used.
     */
    public Number256 contentDigest() {
        if (contentDigest == null) {
            process();
        }
        return contentDigest;
    }

    /**
     * Calculates the digest.
     */
    private void process() {
        Number256 hashKey = Number256.ZERO;
        Number256 hashContent = Number256.ZERO;
        for (Map.Entry<Object, Collection<Number256>> entry : mapDigests.entrySet()) {
            //hashKey = hashKey.xor(entry.getKey().locationKey());
            //hashKey = hashKey.xor(entry.getKey().domainKey());
            //hashKey = hashKey.xor(entry.getKey().contentKey());
            //hashKey = hashKey.xor(entry.getKey().versionKey());
            for (Number256 basedOn: entry.getValue()) {
            	hashContent = hashContent.xor(basedOn);
            }
        }
        keyDigest = hashKey;
        contentDigest = hashContent;
    }
    
    public NavigableMap<Object, Collection<Number256>> mapDigests() {
    	return mapDigests;
    }

    /**
     * Stores a key and the hash of the content for further processing.
     * 
     * @param key
     *            The key of the content
     * @param basedOnSet
     *            The hash of the content
     */
    public void put(final Object key, final Collection<Number256> basedOnSet) {
        mapDigests.put(key, basedOnSet);
    }

    /**
     * @return The list of hashes
     */
    public NavigableMap<Object, Collection<Number256>> digests() {
        return mapDigests;
    }

    /**
     * @return The number of hashes
     */
    public int size() {
        if (size == -1) {
            size = mapDigests.size();
        }
        return size;
    }

    /**
     * @return True, if the digest information has not been provided.
     */
    public boolean isEmpty() {
        return size <= 0;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof DigestInfo)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        DigestInfo other = (DigestInfo) obj;
        return keyDigest().equals(other.keyDigest()) && size() == other.size()
                && contentDigest().equals(other.contentDigest());
    }

    @Override
    public int hashCode() {
        return keyDigest().hashCode() ^ size() ^ contentDigest().hashCode();
    }
}
