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

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.SimpleBloomFilter;

/**
 * The storage stores Number480, which is separated in 3 Number160. The first Number160 is the location key, the second
 * is the domain key, and the third is the content key. A digest can be calculated over all content keys for a specific
 * location and domain. The digest can also be calculated over a selected range of content keys for a specific location
 * and domain.
 * 
 * @author Thomas Bocek
 */
public interface Digest {
    /**
     * Calculates a digest over a specific location and domain. It will return those content keys that are stored. Those
     * keys that are not stored are ignored
     * 
     * @param from
     *            The start key for the digest
     * @param to
     *            The end key for the digest
     * @param contentKey
     *            The content key to look for. The key that is not found is ignored. Can be set to null -> gets the
     *            information for all content keys
     * @return A list of all hashes for the content keys. To return a predictable amount (important for routing), the
     *         hashes can be xored.
     */
    DigestInfo digest(Number640 from, Number640 to);

    /**
     * Calculates a digest over a specific location and domain. It will return those content keys that match the Bloom
     * filter. Those keys that are not stored are ignored
     * 
     * @param key
     *            The location and domain key
     * @param keyBloomFilter
     *            The bloomFilter of those key elements we want the digest. Please not that there might be false
     *            positive, e.g., a Number160 that is included in the digest but not stored on disk/memory.
     * @param contentBloomFilter
     *            The bloomFilter of those data elements we want the digest. Please not that there might be false
     *            positive, e.g., a Number160 that is included in the digest but not stored on disk/memory.
     * @return A list of all hashes for the content keys. To return a predictable amount (important for routing), the
     *         hashes can be xored.
     */
    DigestInfo digest(Number320 key, SimpleBloomFilter<Number160> keyBloomFilter, SimpleBloomFilter<Number160> contentBloomFilter);
}
