package net.tomp2p.storage;

import java.util.NavigableMap;
import java.util.TreeMap;

final public class RangeLock<K extends Comparable<K>> {
	private final Object lockInternal = new Object();
	private final NavigableMap<K, Boolean> cache = new TreeMap<K, Boolean>();
	
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
	
	public Range lock(final K fromKey, final K toKey) {
		NavigableMap<K, Boolean> subMap = null;
		synchronized (lockInternal) {
        	while ((subMap = cache.subMap(fromKey, true, toKey, true)).size() > 0) {
        		while(subMap.size() > 0) {
        			try {
	                    lockInternal.wait();
                    } catch (InterruptedException e) {
	                    return null;
                    }
        		}
        	}
        	cache.put(fromKey, Boolean.TRUE);
        	cache.put(toKey, Boolean.TRUE);
        }
		return new Range(fromKey, toKey, this);
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
