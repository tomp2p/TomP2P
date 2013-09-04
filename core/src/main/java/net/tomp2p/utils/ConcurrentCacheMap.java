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
package net.tomp2p.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A map with expiration and more or less LRU. Since the maps are separated in segments, the LRU is done for each
 * segment. A segment is chosen based on the hash of the key. If one segments is more loaded than another, then an entry
 * of the loaded segment may get evicted before an entry used least recently from an other segment. The expiration is
 * done best effort. There is no thread checking for timed out entries since the cache has a fixed size. Once an entry
 * times out, it remains in the map until it either is accessed or evicted. A test showed that for the default entry
 * size of 1024, this map has a size of 967 if 1024 items are inserted. This is due to the segmentation and hashing.
 * 
 * @author Thomas Bocek
 * @param <K>
 *            the type of the key
 * @param <V>
 *            the type of the value
 */
public class ConcurrentCacheMap<K, V> implements ConcurrentMap<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentCacheMap.class);

    /**
     * Number of segments that can be accessed concurrently.
     */
    public static final int SEGMENT_NR = 16;

    /**
     * Max. number of entries that the map can hold until the least recently used gets replaced
     */
    public static final int MAX_ENTRIES = 1024;

    /**
     * Time to live for a value. The value may stay longer in the map, but it is considered invalid.
     */
    public static final int DEFAULT_TIME_TO_LIVE = 60;

    private final CacheMap<K, ExpiringObject>[] segments;

    private final int timeToLive;

    private final boolean refreshTimeout;

    private final AtomicInteger removedCounter = new AtomicInteger();

    /**
     * Creates a new instance of ConcurrentCacheMap using the supplied values and a {@link CacheMap} for the internal
     * data structure.
     */
    public ConcurrentCacheMap() {
        this(DEFAULT_TIME_TO_LIVE, MAX_ENTRIES, true);
    }

    /**
     * Creates a new instance of ConcurrentCacheMap using the supplied values and a {@link CacheMap} for the internal
     * data structure.
     * 
     * @param timeToLive
     *            The time-to-live value (seconds)
     * @param maxEntries
     *            Set the maximum number of entries until items gets replaced with LRU
     */
    public ConcurrentCacheMap(final int timeToLive, final int maxEntries) {
        this(timeToLive, maxEntries, true);
    }

    /**
     * Creates a new instance of ConcurrentCacheMap using the supplied values and a {@link CacheMap} for the internal
     * data structure.
     * 
     * @param timeToLive
     *            The time-to-live value (seconds)
     * @param maxEntries
     *            The maximum entries to keep in cache, default is 1024
     * @param refreshTimeout
     *            If set to true, timeout will be reset in case of {@link #putIfAbsent(Object, Object)}
     */
    @SuppressWarnings("unchecked")
    public ConcurrentCacheMap(final int timeToLive, final int maxEntries, final boolean refreshTimeout) {
        this.segments = new CacheMap[SEGMENT_NR];
        final int maxEntriesPerSegment = maxEntries / SEGMENT_NR;
        for (int i = 0; i < SEGMENT_NR; i++) {
            // set the cachemap to true, since it should behave as a regular map
            segments[i] = new CacheMap<K, ExpiringObject>(maxEntriesPerSegment, true);
        }
        this.timeToLive = timeToLive;
        this.refreshTimeout = refreshTimeout;
    }

    /**
     * Returns the segment based on the key.
     * 
     * @param key
     *            The key where the hash code identifies the segment
     * @return The cache map that corresponds to this segment
     */
    private CacheMap<K, ExpiringObject> segment(final Object key) {
        return segments[(key.hashCode() & Integer.MAX_VALUE) % SEGMENT_NR];
    }

    @Override
    public V put(final K key, final V value) {
        final ExpiringObject newValue = new ExpiringObject(value, System.currentTimeMillis());
        final CacheMap<K, ExpiringObject> segment = segment(key);
        ExpiringObject oldValue;
        synchronized (segment) {
            oldValue = segment.put(key, newValue);
        }
        if (oldValue == null || oldValue.isExpired()) {
            return null;
        }
        return oldValue.getValue();
    }

    @Override
    /**
     * This does not reset the timer!
     */
    public V putIfAbsent(final K key, final V value) {
        final CacheMap<K, ExpiringObject> segment = segment(key);
        final ExpiringObject newValue = new ExpiringObject(value, System.currentTimeMillis());
        ExpiringObject oldValue = null;
        synchronized (segment) {
            if (!segment.containsKey(key)) {
                oldValue = segment.put(key, newValue);
            } else {
                oldValue = segment.get(key);
                if (oldValue.isExpired()) {
                    segment.put(key, newValue);
                } else if (refreshTimeout) {
                    oldValue = new ExpiringObject(oldValue.getValue(), System.currentTimeMillis());
                    segment.put(key, oldValue);
                }
            }
        }
        if (oldValue == null || oldValue.isExpired()) {
            return null;
        }
        return oldValue.getValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(final Object key) {
        final CacheMap<K, ExpiringObject> segment = segment(key);
        final ExpiringObject oldValue;
        synchronized (segment) {
            oldValue = segment.get(key);
        }
        if (oldValue != null) {
            if (expire(segment, (K) key, oldValue)) {
                return null;
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("get: " + key + ";" + oldValue.getValue());
                }
                return oldValue.getValue();
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("get not found: " + key);
        }
        return null;
    }

    @Override
    public V remove(final Object key) {
        final CacheMap<K, ExpiringObject> segment = segment(key);
        final ExpiringObject oldValue;
        synchronized (segment) {
            oldValue = segment.remove(key);
        }
        if (oldValue == null || oldValue.isExpired()) {
            return null;
        }
        return oldValue.getValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(final Object key, final Object value) {
        final CacheMap<K, ExpiringObject> segment = segment(key);
        final ExpiringObject oldValue;
        boolean removed = false;
        synchronized (segment) {
            oldValue = segment.get(key);
            if (oldValue != null && oldValue.equals(value) && !oldValue.isExpired()) {
                removed = segment.remove(key) != null;
            }
        }
        if (oldValue != null) {
            expire(segment, (K) key, oldValue);
        }
        return removed;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(final Object key) {
        final CacheMap<K, ExpiringObject> segment = segment(key);
        final ExpiringObject oldValue;
        synchronized (segment) {
            oldValue = segment.get(key);
        }
        if (oldValue != null) {
            if (!expire(segment, (K) key, oldValue)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsValue(final Object value) {
        for (final CacheMap<K, ExpiringObject> segment : segments) {
            synchronized (segment) {
                expireSegment(segment);
                if (segment.containsValue(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public int size() {
        int size = 0;
        for (final CacheMap<K, ExpiringObject> segment : segments) {
            synchronized (segment) {
                expireSegment(segment);
                size += segment.size();
            }
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        for (final CacheMap<K, ExpiringObject> segment : segments) {
            synchronized (segment) {
                expireSegment(segment);
                if (!segment.isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void clear() {
        for (final CacheMap<K, ExpiringObject> segment : segments) {
            synchronized (segment) {
                segment.clear();
            }
        }
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (final CacheMap<K, ExpiringObject> segment : segments) {
            synchronized (segment) {
                expireSegment(segment);
                // as seen in AbstractMap
                hashCode += segment.hashCode();
            }
        }
        return hashCode;
    }

    @Override
    public Set<K> keySet() {
        final Set<K> retVal = new HashSet<K>();
        for (final CacheMap<K, ExpiringObject> segment : segments) {
            synchronized (segment) {
                expireSegment(segment);
                retVal.addAll(segment.keySet());
            }
        }
        return retVal;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> inMap) {
        for (final Entry<? extends K, ? extends V> e : inMap.entrySet()) {
            this.put(e.getKey(), e.getValue());
        }
    }

    @Override
    public Collection<V> values() {
        final Collection<V> retVal = new ArrayList<V>();
        for (final CacheMap<K, ExpiringObject> segment : segments) {
            synchronized (segment) {
                final Iterator<ExpiringObject> iterator = segment.values().iterator();
                while (iterator.hasNext()) {
                    final ExpiringObject expiringObject = iterator.next();
                    if (expiringObject.isExpired()) {
                        iterator.remove();
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("remove in entrySet " + expiringObject.getValue());
                        }
                        removedCounter.incrementAndGet();
                    } else {
                        retVal.add(expiringObject.getValue());
                    }
                }
            }
        }
        return retVal;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        final Set<Map.Entry<K, V>> retVal = new HashSet<Map.Entry<K, V>>();
        for (final CacheMap<K, ExpiringObject> segment : segments) {
            synchronized (segment) {
                final Iterator<Map.Entry<K, ExpiringObject>> iterator = segment.entrySet().iterator();
                while (iterator.hasNext()) {
                    final Map.Entry<K, ExpiringObject> entry = iterator.next();
                    if (entry.getValue().isExpired()) {
                        iterator.remove();
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("remove in entrySet " + entry.getValue().getValue());
                        }
                        removedCounter.incrementAndGet();
                    } else {
                        retVal.add(new Map.Entry<K, V>() {
                            @Override
                            public K getKey() {
                                return entry.getKey();
                            }

                            @Override
                            public V getValue() {
                                return entry.getValue().getValue();
                            }

                            @Override
                            public V setValue(final V value) {
                                throw new UnsupportedOperationException("not supported");
                            }
                        });
                    }
                }
            }
        }
        return retVal;
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        final ExpiringObject oldValue2 = new ExpiringObject(oldValue, 0L);
        final ExpiringObject newValue2 = new ExpiringObject(newValue, System.currentTimeMillis());
        final CacheMap<K, ExpiringObject> segment = segment(key);
        final ExpiringObject oldValue3;
        boolean replaced = false;
        synchronized (segment) {
            oldValue3 = segment.get(key);
            if (oldValue3 != null && !oldValue3.isExpired() && oldValue2.equals(oldValue3.getValue())) {
                segment.put(key, newValue2);
                replaced = true;
            }
        }
        if (oldValue3 != null) {
            expire(segment, key, oldValue3);
        }
        return replaced;
    }

    @Override
    public V replace(final K key, final V value) {
        final ExpiringObject newValue = new ExpiringObject(value, System.currentTimeMillis());
        final CacheMap<K, ExpiringObject> segment = segment(key);
        final ExpiringObject oldValue;
        synchronized (segment) {
            oldValue = segment.get(key);
            if (oldValue != null && !oldValue.isExpired()) {
                segment.put(key, newValue);
            }
        }
        if (oldValue == null) {
            return null;
        }
        if (expire(segment, key, oldValue)) {
            return null;
        }
        return oldValue.getValue();
    }

    /**
     * Expires a key in a segment. If a key value pair is expired, it will get removed.
     * 
     * @param segment
     *            The segment
     * @param key
     *            The key
     * @param value
     *            The value
     * @return True if expired, otherwise false.
     */
    private boolean expire(final CacheMap<K, ExpiringObject> segment, final K key, final ExpiringObject value) {
        if (value.isExpired()) {
            synchronized (segment) {
                final ExpiringObject tmp = segment.get(key);
                if (tmp != null && tmp.equals(value)) {
                    segment.remove(key);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("remove in expire " + value.getValue());
                    }
                    removedCounter.incrementAndGet();
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Fast expiration. Since the ExpiringObject is ordered the for loop can break early if a object is not expired.
     * 
     * @param segment
     *            The segment
     */
    private void expireSegment(final CacheMap<K, ExpiringObject> segment) {
        final Iterator<ExpiringObject> iterator = segment.values().iterator();
        while (iterator.hasNext()) {
            final ExpiringObject expiringObject = iterator.next();
            if (expiringObject.isExpired()) {
                iterator.remove();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("remove in expireAll " + expiringObject.getValue());
                }
                removedCounter.incrementAndGet();
            } else {
                break;
            }
        }
    }

    /**
     * @return The number of expired objects
     */
    public int expiredCounter() {
        return removedCounter.get();
    }

    /**
     * An object that also holds expriation information.
     */
    private class ExpiringObject {
        private final V value;

        private final long lastAccessTime;

        private static final int MS_IN_S = 1000;

        /**
         * Creates a new expiring object with the given time of access.
         * 
         * @param value
         *            The value that is wrapped in this class
         * @param lastAccessTime
         *            The time of access
         */
        ExpiringObject(final V value, final long lastAccessTime) {
            if (value == null) {
                throw new IllegalArgumentException("An expiring object cannot be null.");
            }
            this.value = value;
            this.lastAccessTime = lastAccessTime;
        }

        /**
         * @return If entry is expired
         */
        public boolean isExpired() {
            return System.currentTimeMillis() >= lastAccessTime + (timeToLive * MS_IN_S);
        }

        /**
         * @return The wrapped value
         */
        public V getValue() {
            return value;
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof ConcurrentCacheMap.ExpiringObject)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            final ExpiringObject exp = (ExpiringObject) obj;
            return value.equals(exp.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
