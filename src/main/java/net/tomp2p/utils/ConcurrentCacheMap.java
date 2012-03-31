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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A map with expiration and more or less LRU. Since the maps are separated in
 * segments, the LRU is done for each segment. A segment is chosen based on the
 * hash of the key. If one segments is more loaded than another, then an entry
 * of the loaded segment may get evicted before an entry used least recently
 * from an other segment.
 * 
 * The expiration is done best effort. There is no thread checking for timed out
 * entries since the cache has a fixed size. Once an entry times out, it remains
 * in the map until it either is accessed or evicted.
 * 
 * A test showed that for the default entry size of 1024, this map has a size of
 * 967 if 1024 items are inserted. This is due to the segmentation and hashing.
 * 
 * @author Thomas Bocek
 */
public class ConcurrentCacheMap<K, V> implements ConcurrentMap<K, V>
{
	final private static Logger logger = LoggerFactory.getLogger(ConcurrentCacheMap.class);
	public static final int SEGMENT_NR = 16;
	public static final int MAX_ENTRIES = 1024;
	public static final int DEFAULT_TIME_TO_LIVE = 60;
	private final CacheMap<K, ExpiringObject>[] segments;
	private final int timeToLive;

	/**
	 * Creates a new instance of ConcurrentCacheMap using the supplied values
	 * and a {@link CacheMap} for the internal data structure.
	 */
	public ConcurrentCacheMap()
	{
		this(DEFAULT_TIME_TO_LIVE, MAX_ENTRIES, true);
	}

	/**
	 * Creates a new instance of ConcurrentCacheMap using the supplied values
	 * and a {@link CacheMap} for the internal data structure.
	 * 
	 * @param timeToLive The time-to-live value (seconds)
	 */
	public ConcurrentCacheMap(int timeToLive)
	{
		this(timeToLive, MAX_ENTRIES, true);
	}

	/**
	 * Creates a new instance of ConcurrentCacheMap using the supplied values
	 * and a {@link CacheMap} for the internal data structure.
	 * 
	 * @param timeToLive The time-to-live value (seconds)
	 * @param maxEntries The maximum entries to keep in cache, default is 1024
	 */
	@SuppressWarnings("unchecked")
	public ConcurrentCacheMap(int timeToLive, int maxEntries, boolean updateEntryOnInsert)
	{
		this.segments = new CacheMap[SEGMENT_NR];
		int maxEntriesPerSegment = maxEntries / SEGMENT_NR;
		for (int i = 0; i < SEGMENT_NR; i++)
		{
			segments[i] = new CacheMap<K, ExpiringObject>(maxEntriesPerSegment, updateEntryOnInsert);
		}
		this.timeToLive = timeToLive;
	}

	private CacheMap<K, ExpiringObject> segment(Object key)
	{
		return segments[(key.hashCode() & Integer.MAX_VALUE) % SEGMENT_NR];
	}

	@Override
	public V put(K key, V value)
	{
		CacheMap<K, ExpiringObject> segment = segment(key);
		ExpiringObject newValue = new ExpiringObject(value, System.currentTimeMillis());
		ExpiringObject oldValue;
		synchronized (segment)
		{
			oldValue = segment.put(key, newValue);
		}
		if (oldValue == null || oldValue.isExpired())
		{
			return null;
		}
		return oldValue.getValue();
	}

	@Override
	public V putIfAbsent(K key, V value)
	{
		CacheMap<K, ExpiringObject> segment = segment(key);
		ExpiringObject newValue = new ExpiringObject(value, System.currentTimeMillis());
		ExpiringObject oldValue = null;
		synchronized (segment)
		{
			if (!segment.containsKey(key))
			{
				oldValue = segment.put(key, newValue);
			}
			else
			{
				oldValue = segment.get(key);
			}
		}
		if (oldValue == null || oldValue.isExpired())
		{
			return null;
		}
		return oldValue.getValue();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key)
	{
		CacheMap<K, ExpiringObject> segment = segment(key);
		ExpiringObject oldValue;
		synchronized (segment)
		{
			oldValue = segment.get(key);
		}
		if (oldValue != null)
		{
			if (expire(segment, (K) key, oldValue))
			{
				return null;
			}
			else
			{
				if(logger.isDebugEnabled())
				{
					logger.debug("get: "+key+";"+oldValue.getValue());
				}
				return oldValue.getValue();
			}
		}
		if(logger.isDebugEnabled())
		{
			logger.debug("get not found: "+key);
		}
		return null;
	}

	@Override
	public V remove(Object key)
	{
		CacheMap<K, ExpiringObject> segment = segment(key);
		ExpiringObject oldValue;
		synchronized (segment)
		{
			oldValue = segment.remove(key);
		}
		if (oldValue == null || oldValue.isExpired())
		{
			return null;
		}
		return oldValue.getValue();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object key, Object value)
	{
		CacheMap<K, ExpiringObject> segment = segment(key);
		ExpiringObject oldValue;
		boolean removed = false;
		synchronized (segment)
		{
			oldValue = segment.get(key);
			if (oldValue != null && oldValue.equals(value) && !oldValue.isExpired())
			{
				removed = segment.remove(key) != null;
			}
		}
		if (oldValue != null)
		{
			expire(segment, (K) key, oldValue);
		}
		return removed;
	}

	@SuppressWarnings("unchecked")
	public boolean containsKey(Object key)
	{
		CacheMap<K, ExpiringObject> segment = segment(key);
		ExpiringObject oldValue;
		synchronized (segment)
		{
			oldValue = segment.get(key);
		}
		if (oldValue != null)
		{
			if(expire(segment, (K) key, oldValue))
			{
				return false;
			}
			else
			{
				return true;
			}
		}
		return false;
	}

	public boolean containsValue(Object value)
	{
		expireAll();
		for (CacheMap<K, ExpiringObject> segment : segments)
		{
			synchronized (segment)
			{
				if (segment.containsValue(value))
				{
					return true;
				}
			}
		}
		return false;
	}

	public int size()
	{
		expireAll();
		int size = 0;
		for (CacheMap<K, ExpiringObject> segment : segments)
		{
			synchronized (segment)
			{
				size += segment.size();
			}
		}
		return size;
	}

	public boolean isEmpty()
	{
		expireAll();
		for (CacheMap<K, ExpiringObject> segment : segments)
		{
			synchronized (segment)
			{
				if (!segment.isEmpty())
				{
					return false;
				}
			}
		}
		return true;
	}

	public void clear()
	{
		for (CacheMap<K, ExpiringObject> segment : segments)
		{
			synchronized (segment)
			{
				segment.clear();
			}
		}
	}

	@Override
	public int hashCode()
	{
		expireAll();
		int hashCode = 0;
		for (CacheMap<K, ExpiringObject> segment : segments)
		{
			synchronized (segment)
			{
				// as seen in AbstractMap
				hashCode += segment.hashCode();
			}
		}
		return hashCode;
	}

	public Set<K> keySet()
	{
		expireAll();
		Set<K> retVal = new HashSet<K>();
		for (CacheMap<K, ExpiringObject> segment : segments)
		{
			synchronized (segment)
			{
				retVal.addAll(segment.keySet());
			}
		}
		return retVal;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == this)
		{
			return true;
		}
		if (!(obj instanceof ConcurrentCacheMap))
		{
			return false;
		}
		int len = segments.length;
		@SuppressWarnings("unchecked")
		ConcurrentCacheMap<K, V> m = (ConcurrentCacheMap<K, V>) obj;
		if (len != m.segments.length)
		{
			return false;
		}
		expireAll();
		m.expireAll();
		for (int i = 0; i < len; i++)
		{
			synchronized (segments[i])
			{
				synchronized (m.segments[i])
				{
					if (!segments[i].equals(m.segments[i]))
					{
						return false;
					}
				}
			}
		}
		return true;
	}

	public void putAll(Map<? extends K, ? extends V> inMap)
	{
		for (Entry<? extends K, ? extends V> e : inMap.entrySet())
		{
			this.put(e.getKey(), e.getValue());
		}
	}

	public Collection<V> values()
	{
		Collection<V> retVal = new ArrayList<V>();
		for (CacheMap<K, ExpiringObject> segment : segments)
		{
			synchronized (segment)
			{
				Iterator<ExpiringObject> iterator = segment.values().iterator();
				while (iterator.hasNext())
				{
					ExpiringObject expiringObject = iterator.next();
					if (expiringObject.isExpired())
					{
						iterator.remove();
						if(logger.isDebugEnabled())
						{
							logger.debug("remove in entrySet "+expiringObject.getValue());
						}
					}
					else
					{
						retVal.add(expiringObject.getValue());
					}
				}
			}
		}
		return retVal;
	}

	public Set<Map.Entry<K, V>> entrySet()
	{
		Set<Map.Entry<K, V>> retVal = new HashSet<Map.Entry<K, V>>();
		for (CacheMap<K, ExpiringObject> segment : segments)
		{
			synchronized (segment)
			{
				Iterator<Map.Entry<K, ExpiringObject>> iterator = segment.entrySet().iterator();
				while (iterator.hasNext())
				{
					final Map.Entry<K, ExpiringObject> entry = iterator.next();
					if (entry.getValue().isExpired())
					{
						iterator.remove();
						if(logger.isDebugEnabled())
						{
							logger.debug("remove in entrySet "+entry.getValue().getValue());
						}
					}
					else
					{
						retVal.add(new Map.Entry<K, V>()
						{
							@Override
							public K getKey()
							{
								return entry.getKey();
							}

							@Override
							public V getValue()
							{
								return entry.getValue().getValue();
							}

							@Override
							public V setValue(V value)
							{
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
	public boolean replace(K key, V oldValue, V newValue)
	{
		ExpiringObject oldValue2 = new ExpiringObject(oldValue, 0L);
		ExpiringObject newValue2 = new ExpiringObject(newValue, System.currentTimeMillis());
		CacheMap<K, ExpiringObject> segment = segment(key);
		ExpiringObject oldValue3;
		boolean replaced = false;
		synchronized (segment)
		{
			oldValue3 = segment.get(key);
			if (oldValue3 != null && !oldValue3.isExpired()
					&& oldValue2.equals(oldValue3.getValue()))
			{
				segment.put(key, newValue2);
				replaced = true;
			}
		}
		if (oldValue3 != null)
		{
			expire(segment, key, oldValue3);
		}
		return replaced;
	}

	@Override
	public V replace(K key, V value)
	{
		ExpiringObject newValue = new ExpiringObject(value, System.currentTimeMillis());
		CacheMap<K, ExpiringObject> segment = segment(key);
		ExpiringObject oldValue;
		synchronized (segment)
		{
			oldValue = segment.get(key);
			if (oldValue != null && !oldValue.isExpired())
			{
				segment.put(key, newValue);
			}
		}
		if (oldValue == null)
		{
			return null;
		}
		if (expire(segment, key, oldValue))
		{
			return null;
		}
		return oldValue.getValue();
	}

	private boolean expire(CacheMap<K, ExpiringObject> segment, K key, ExpiringObject value)
	{
		if (value.isExpired())
		{
			synchronized (segment)
			{
				ExpiringObject tmp = segment.get(key);
				if (tmp != null && tmp.equals(value))
				{
					segment.remove(key);
					if(logger.isDebugEnabled())
					{
						logger.debug("remove in expire "+value.getValue());
					}
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Fast expiration. Since the ExpiringObject is ordered the for loop can
	 * break early if a object is not expired.
	 */
	private void expireAll()
	{
		for (CacheMap<K, ExpiringObject> segment : segments)
		{
			synchronized (segment)
			{
				Iterator<ExpiringObject> iterator = segment.values().iterator();
				while (iterator.hasNext())
				{
					ExpiringObject expiringObject = iterator.next();
					if (expiringObject.isExpired())
					{
						iterator.remove();
						if(logger.isDebugEnabled())
						{
							logger.debug("remove in expireAll "+expiringObject.getValue());
						}
					}
					else
					{
						break;
					}
				}
			}
		}
	}
	private class ExpiringObject
	{
		private final V value;
		private final long lastAccessTime;

		ExpiringObject(V value, long lastAccessTime)
		{
			if (value == null)
			{
				throw new IllegalArgumentException("An expiring object cannot be null.");
			}
			this.value = value;
			this.lastAccessTime = lastAccessTime;
		}

		public boolean isExpired()
		{
			return System.currentTimeMillis() > lastAccessTime + (timeToLive * 1000);
		}

		public V getValue()
		{
			return value;
		}

		@Override
		public boolean equals(Object obj)
		{
			if(!(obj instanceof ConcurrentCacheMap.ExpiringObject))
			{
				return false;
			}
			@SuppressWarnings("unchecked")
			ConcurrentCacheMap<K,V>.ExpiringObject exp = (ConcurrentCacheMap<K,V>.ExpiringObject) obj;
			return value.equals(exp.value);
		}

		@Override
		public int hashCode()
		{
			return value.hashCode();
		}
	}
}