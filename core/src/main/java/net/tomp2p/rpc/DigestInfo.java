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

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

/**
 * Calculates or sets a global hash. The digest is used in two places: for routing, where a message needs to have a
 * predictable size. Thus in this case a global hash is calculated. The second usage is get() for getting a list of
 * hashes from peers. Here we don't need to restrict ourself, since we use TCP.
 * 
 * @author Thomas Bocek
 */
public class DigestInfo {
    private volatile Number160 keyDigest = null;

    private volatile Number160 contentDigest = null;

    private volatile int size = -1;

    private final NavigableMap<Number640, Number160> mapDigests = new TreeMap<Number640, Number160>();

    /**
     * Empty constructor is used to add the hashes to the list.
     */
    public DigestInfo() {
    }

    /**
     * Create a digest with the size only.
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
    public DigestInfo(final Number160 keyDigest, final Number160 contentDigest, final int size) {
        this.keyDigest = keyDigest;
        this.contentDigest = contentDigest;
        this.size = size;
    }

    /**
     * @return Returns or calculates the global key hash. The global key hash will be calculated if the empty
     *         constructor is used.
     */
    public Number160 getKeyDigest() {
        if (keyDigest == null) {
            process();
        }
        return keyDigest;
    }

    /**
     * @return Returns or calculates the global content hash. The global content hash will be calculated if the empty
     *         constructor is used.
     */
    public Number160 getContentDigest() {
        if (contentDigest == null) {
            process();
        }
        return contentDigest;
    }

    /**
     * Calculates the digest.
     */
    private void process() {
        Number160 hashKey = Number160.ZERO;
        Number160 hashContent = Number160.ZERO;
        for (Map.Entry<Number640, Number160> entry : mapDigests.entrySet()) {
            hashKey = hashKey.xor(entry.getKey().getLocationKey());
            hashKey = hashKey.xor(entry.getKey().getDomainKey());
            hashKey = hashKey.xor(entry.getKey().getContentKey());
            hashKey = hashKey.xor(entry.getKey().getVersionKey());
            hashContent = hashContent.xor(entry.getValue());
        }
        keyDigest = hashKey;
        contentDigest = hashContent;
    }

    /**
     * @param factory
     *            The bloom filter creator
     * @return The bloom filter of the keys that are on this peer
     */
    public SimpleBloomFilter<Number160> getLocationKeyBloomFilter(final BloomfilterFactory factory) {
        SimpleBloomFilter<Number160> sbf = factory.createLoctationKeyBloomFilter();
        for (Map.Entry<Number640, Number160> entry : mapDigests.entrySet()) {
            sbf.add(entry.getKey().getLocationKey());
        }
        return sbf;
    }
    
    /**
     * @param factory
     *            The bloom filter creator
     * @return The bloom filter of the keys that are on this peer
     */
    public SimpleBloomFilter<Number160> getDomainKeyBloomFilter(final BloomfilterFactory factory) {
        SimpleBloomFilter<Number160> sbf = factory.createDomainKeyBloomFilter();
        for (Map.Entry<Number640, Number160> entry : mapDigests.entrySet()) {
            sbf.add(entry.getKey().getDomainKey());
        }
        return sbf;
    }
    
    /**
     * @param factory
     *            The bloom filter creator
     * @return The bloom filter of the keys that are on this peer
     */
    public SimpleBloomFilter<Number160> getContentKeyBloomFilter(final BloomfilterFactory factory) {
        SimpleBloomFilter<Number160> sbf = factory.createContentKeyBloomFilter();
        for (Map.Entry<Number640, Number160> entry : mapDigests.entrySet()) {
            sbf.add(entry.getKey().getContentKey());
        }
        return sbf;
    }

    /**
     * @param factory
     *            The bloom filter creator
     * @return The bloom filter of the content keys that are on this peer
     */
    public SimpleBloomFilter<Number160> getVersionKeyBloomFilter(final BloomfilterFactory factory) {
        SimpleBloomFilter<Number160> sbf = factory.createContentBloomFilter();
        for (Map.Entry<Number640, Number160> entry : mapDigests.entrySet()) {
            sbf.add(entry.getKey().getVersionKey());
        }
        return sbf;
    }

    /**
     * Stores a key and the hash of the content for further processing.
     * 
     * @param key
     *            The key of the content
     * @param basedOn
     *            The hash of the content
     */
    public void put(final Number640 key, final Number160 basedOn) {
        mapDigests.put(key, basedOn);
    }

    /**
     * @return The list of hashes
     */
    public NavigableMap<Number640, Number160> getDigests() {
        return mapDigests;
    }

    /**
     * @return The number of hashes
     */
    public int getSize() {
        if (size == -1) {
            size = mapDigests.size();
        }
        return size;
    }

    /**
     * @return True is the digest information has not been provided.
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
        return getKeyDigest().equals(other.getKeyDigest()) && getSize() == other.getSize()
                && getContentDigest().equals(other.getContentDigest());
    }

    @Override
    public int hashCode() {
        return getKeyDigest().hashCode() ^ getSize() ^ getContentDigest().hashCode();
    }
}
