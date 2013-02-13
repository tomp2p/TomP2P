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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//as seen in http://stackoverflow.com/questions/5639870/simple-java-name-based-locks
public class KeyLock<K> {
    private class RefCounterLock {
        final public ReentrantLock sem = new ReentrantLock();

        private volatile int counter = 0;
    }

    private final ReentrantLock lockInternal = new ReentrantLock();

    private final HashMap<K, RefCounterLock> cache = new HashMap<K, RefCounterLock>();

    public Lock lock(K key) {
        RefCounterLock cur;
        lockInternal.lock();
        try {
            if (!cache.containsKey(key)) {
                cur = new RefCounterLock();
                cache.put(key, cur);
            } else {
                cur = cache.get(key);
            }
            cur.counter++;
        } finally {
            lockInternal.unlock();
        }
        cur.sem.lock();
        return cur.sem;
    }

    /**
     * @param key
     * @param lock
     *            With this argument we make sure that lock has been called
     *            previously
     */
    public void unlock(K key, Lock lock) {
        RefCounterLock cur = null;
        lockInternal.lock();
        try {
            if (cache.containsKey(key)) {
                cur = cache.get(key);
                if (lock != cur.sem) {
                    throw new IllegalArgumentException("lock does not matches the stored lock");
                }
                cur.counter--;
                cur.sem.unlock();
                if (cur.counter == 0) { // last reference
                    cache.remove(key);
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