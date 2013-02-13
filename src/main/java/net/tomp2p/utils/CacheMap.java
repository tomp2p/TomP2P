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
package net.tomp2p.utils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The CacheMap is a LRU cache with a given capacity. The elements that do not
 * fit into the cache will be removed. The flag updateEntryOnInsert will
 * determine if {@link #put(Object, Object)} or
 * {@link #putIfAbsent(Object, Object)} will be used. This is useful for entries
 * that have timing information and that should not be updated if the same key
 * is going to be used. This class extends {@link LinkedHashMap}, which means
 * that this class is not thread safe.
 * 
 * @author Thomas Bocek
 * @param <K>
 *            The key
 * @param <V>
 *            The value
 */
public class CacheMap<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = 5937613180687142367L;

    private final int maxEntries;

    private final boolean updateEntryOnInsert;

    /**
     * Creates a new CacheMap with a fixed capacity
     * 
     * @param maxEntries
     *            The number of entries that can be stored in this map
     * @param updateEntryOnInsert
     *            Set to true to update (overwrite) values. Set false to not
     *            overwrite the values if there is a value present.
     */
    public CacheMap(int maxEntries, boolean updateEntryOnInsert) {
        this.maxEntries = maxEntries;
        this.updateEntryOnInsert = updateEntryOnInsert;
    }

    @Override
    public V put(K key, V value) {
        if (updateEntryOnInsert) {
            return super.put(key, value);
        } else {
            return putIfAbsent(key, value);
        }
    }

    /**
     * If the key is not associated with a value, associate it with the value.
     * This is the same as:
     * 
     * <pre>
     * if (!map.containsKey(key)) {
     *     return map.put(key, value);
     * } else {
     *     return map.get(key);
     * }
     * </pre>
     * 
     * @param key
     *            key with which the value is to be associated.
     * @param value
     *            value to be associated with the key.
     * @return previous value associated with key, or null if there was no
     *         mapping for this key.
     */
    public V putIfAbsent(K key, V value) {
        if (!containsKey(key)) {
            return super.put(key, value);
        } else {
            return super.get(key);
        }
    };

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxEntries;
    }
}