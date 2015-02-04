package net.tomp2p.storage;

import java.util.NavigableMap;
import java.util.TreeMap;

final public class RangeLock<K extends Comparable<K>> {
	private final Object lockInternal = new Object();
	private final NavigableMap<K, Long> cache = new TreeMap<K, Long>();
	
	final public class Range {
        final private K fromKey;
        final private K toKey;
        final private RangeLock<K> ref;
        private Range(final K fromKey, final K toKey, RangeLock<K> ref) {
        	this.fromKey = fromKey;
        	this.toKey = toKey;
        	this.ref = ref;
        }
        
        public void unlock() {
        	ref.unlock(this);
        }
    }
	
	/**
	 * The same thread can lock a range twice. The first unlock for range x unlocks all range x.   
	 * @param fromKey
	 * @param toKey
	 * @return
	 */
	public Range lock(final K fromKey, final K toKey) {
		final long id = Thread.currentThread().getId();
		synchronized (lockInternal) {
			final NavigableMap<K, Long> subMap = cache.subMap(fromKey, true, toKey, true);
			while (subMap.size() > 0) {
				if(mapSizeFiltered(id, subMap) == 0) {
					break;
				}
				try {
					lockInternal.wait();
				} catch (InterruptedException e) {
					return null;
				}
			}
        	
        	cache.put(fromKey, id);
        	cache.put(toKey, id);
        }
		return new Range(fromKey, toKey, this);
	}
	
	private int mapSizeFiltered(final long id, final NavigableMap<K, Long> subMap) {
		int counter = 0;
		for(final long longValue:subMap.values()) {
			if(longValue != id) {
				counter ++;
			}
		}
		return counter;
	}

	public void unlock(RangeLock<?>.Range lock) {
		synchronized (lockInternal) {
			cache.remove(lock.fromKey);
			cache.remove(lock.toKey);
			lockInternal.notifyAll();
	    }
	}
	
	public int size() {
		synchronized (lockInternal) {
			return cache.size();
		}
	}
	
}
