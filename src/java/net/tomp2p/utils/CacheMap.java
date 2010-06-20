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

public class CacheMap<K, V> extends LinkedHashMap<K, V>
{
	private static final long serialVersionUID = 5937613180687142367L;
	final private int maxEntries;

	public CacheMap(int maxEntries)
	{
		this.maxEntries = maxEntries;
	}

	@Override
	public V put(K key, V value)
	{
		if (!containsKey(key))
			return super.put(key, value);
		else
			return null;
	};

	@Override
	protected boolean removeEldestEntry(Map.Entry<K, V> eldest)
	{
		return size() > maxEntries;
	}
}
