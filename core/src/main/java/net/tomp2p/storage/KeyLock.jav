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

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

//as seen in http://stackoverflow.com/questions/5639870/simple-java-name-based-locks
public class KeyLock<K> {
    public class RefCounterLock {
        final private K key;
        final public ReentrantLock sem = new ReentrantLock();
        final private KeyLock<K> keyLock;
        
        private int counter = 0;
        
        public RefCounterLock(K key, KeyLock<K> keyLock) {
            this.key = key;
            this.keyLock = keyLock;
        }
        
        public void unlock() {
            keyLock.unlock(this);
        }
    }

    private final ReentrantLock lockInternal = new ReentrantLock();

    // TODO: think about doing this with a weak hashmap, maybe counter will not be necessary.
    private final HashMap<K, RefCounterLock> cache = new HashMap<K, RefCounterLock>();

    public RefCounterLock lock(final K key) {
        final RefCounterLock cur;
        lockInternal.lock();
        try {
            if (!cache.containsKey(key)) {
                cur = new RefCounterLock(key, this);
                cache.put(key, cur);
            } else {
                cur = cache.get(key);
            }
            cur.counter++;
        } finally {
            lockInternal.unlock();
        }
        //System.err.println("lock: "+ key);
        cur.sem.lock();
        return cur;
    }

    /**
     * @param key
     * @param lock
     *            With this argument we make sure that lock has been called previously
     */
    public void unlock(KeyLock<?>.RefCounterLock lock) {
        RefCounterLock cur = null;
        lockInternal.lock();
        try {
            if (cache.containsKey(lock.key)) {
                cur = cache.get(lock.key);
                if (lock != cur) {
                    throw new IllegalArgumentException("lock does not matches the stored lock");
                }
                cur.counter--;
                cur.sem.unlock();
                if (cur.counter == 0) { // last reference
                    cache.remove(lock.key);
                }
            }
        } finally {
            lockInternal.unlock();
        }
    }

    public int cacheSize() {
        lockInternal.lock();
        try {
            return cache.size();
        } finally {
            lockInternal.unlock();
        }
    }
}