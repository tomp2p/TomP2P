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

import java.security.PublicKey;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;

public interface Storage extends Digest, ReplicationStorage {
    // Core
    public abstract boolean put(Number160 locationKey, Number160 domainKey, Number160 contentKey, Data value);

    public abstract Data get(Number160 locationKey, Number160 domainKey, Number160 contentKey);

    public abstract boolean contains(Number160 locationKey, Number160 domainKey, Number160 contentKey);

    public abstract Data remove(Number160 locationKey, Number160 domainKey, Number160 contentKey);

    public abstract SortedMap<Number480, Data> subMap(Number160 locationKey, Number160 domainKey,
            Number160 fromContentKey, Number160 toContentKey);

    public abstract Map<Number480, Data> subMap(Number160 locationKey);

    /**
     * The storage is typically backed by multiple Java collections (HashMap,
     * TreeMap, etc.). This map returns the map that stores the values which are
     * present in the DHT. If you plan to do transactions (put/get), make sure
     * you do the locking in order to not interfere with other threads that use
     * this map. Although the storage is threadsafe, there may be concurrency
     * issues with respect to transactions (e.g., do a get before a put). Please
     * use {@link StorageGeneric#getLockStorage()} for full locking, and
     * {@link StorageGeneric#getLockNumber160()},
     * {@link StorageGeneric#getLockNumber320()},
     * {@link StorageGeneric#getLockNumber480()} for fine grained locking.
     * 
     * @return The backing dataMap
     */
    public abstract NavigableMap<Number480, Data> map();

    public abstract void close();

    // Maintenance
    public abstract void addTimeout(Number160 locationKey, Number160 domainKey, Number160 contentKey, long expiration);

    public abstract void removeTimeout(Number160 locationKey, Number160 domainKey, Number160 contentKey);

    public abstract Collection<Number480> subMapTimeout(long to);

    // Domain / entry protection
    public abstract boolean protectDomain(Number160 locationKey, Number160 domainKey, PublicKey publicKey);

    public abstract boolean isDomainProtectedByOthers(Number160 locationKey, Number160 domainKey, PublicKey publicKey);
}