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

package net.tomp2p.storage

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageDisk extends StorageGeneric {
    final private static Logger logger = LoggerFactory.getLogger(StorageDisk.class);

    private static final String DATA_MAP = "dataMap";

    private static final String RESPONSIBILITY_MAP_REV = "responsibilityMapRev";

    private static final String RESPONSIBILITY_MAP = "responsibilityMap";

    private static final String PROTECTED_MAP = "protectedMap";

    private static final String TIMEOUT_MAP_REV = "timeoutMapRev";

    private static final String TIMEOUT_MAP = "timeoutMap";

    final private DB db;

    // Core
    final private NavigableMap<Number480, Data> dataMap;

    // Maintenance
    final private Map<Number480, Long> timeoutMap;

    final private SortedMap<Long, Set<Number480>> timeoutMapRev;

    // Protection
    final private Map<Number320, byte[]> protectedMap;

    // Replication
    // maps content (locationKey) to peerid
    final private Map<Number160, Number160> responsibilityMap;

    // maps peerid to content (locationKey)
    final private Map<Number160, Set<Number160>> responsibilityMapRev;

    final private KeyLock<Number160> responsibilityLock = new KeyLock<Number160>();

    final private KeyLock<Long> timeoutLock = new KeyLock<Long>();

    final private String dirName;

    public StorageDisk(String dirName) {
        this.dirName = dirName;
        String fileName = dirName + File.separator + "tomp2p-jdbm3";
        db = DBMaker.openFile(fileName).make();
        dataMap = this.<Number480, Data> getOrCreateTreeMap(DATA_MAP);
        timeoutMap = this.<Number480, Long> getOrCreateHashMap(TIMEOUT_MAP);
        timeoutMapRev = this.<Long, Set<Number480>> getOrCreateTreeMap(TIMEOUT_MAP_REV);
        protectedMap = this.<Number320, byte[]> getOrCreateHashMap(PROTECTED_MAP);
        responsibilityMap = this.<Number160, Number160> getOrCreateHashMap(RESPONSIBILITY_MAP);
        responsibilityMapRev = this.<Number160, Set<Number160>> getOrCreateHashMap(RESPONSIBILITY_MAP_REV);
    }

    private <K extends Comparable<?>, V> NavigableMap<K, V> getOrCreateTreeMap(String name) {
        final NavigableMap<K, V> navigableMap = db.<K, V> getTreeMap(name);
        return navigableMap != null ? navigableMap : db.<K, V> createTreeMap(name);
    }

    private <K extends Comparable<?>, V> ConcurrentMap<K, V> getOrCreateHashMap(String name) {
        final ConcurrentMap<K, V> concurrentMap = db.<K, V> getHashMap(name);
        return concurrentMap != null ? concurrentMap : db.<K, V> createHashMap(name);
    }

    @Override
    public void close() {
        db.close();
    }

    @Override
    public boolean put(Number160 locationKey, Number160 domainKey, Number160 contentKey, Data value) {
        Number480 key = new Number480(locationKey, domainKey, contentKey);
        // 8MB is the limit
        boolean retVal = true;
        if (value.length() > (1 * 1024 * 1024)) {
            // store reference
            retVal = storeReference(value, key);
        } else {
            // store directly
            dataMap.put(key, value);
        }
        db.commit();
        return retVal;
    }

    private boolean storeReference(Data value, Number480 key) {
        try {
            File file = store(key, value);
            Data data = new Data(file, value.version(), value.ttlSeconds(), value.hasHash(),
                    value.protectedEntry());
            data.peerId(value.peerId());
            data.publicKey(value.publicKey());
            dataMap.put(key, data);
            return true;
        } catch (IOException e) {
            logger.error("cannot store reference: " + e);
            return false;
        } catch (ClassNotFoundException e) {
            logger.error("cannot store reference: " + e);
            return false;
        }
    }

    /**
     * Store big data on disk.
     * 
     * @param key
     *            The key of the value
     * @param value
     *            The value
     * @return Returns the file where the data was stored
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private File store(Number480 key, Data value) throws IOException, ClassNotFoundException {
        final File file = new File(dirName, key.toString());
        file.createNewFile();
        FileChannel destination = null;
        try {
            destination = new FileOutputStream(file).getChannel();
            destination.write(value.buffer().nioBuffers());
        } finally {
            if (destination != null) {
                destination.close();
            }
        }
        return file;
    }

    @Override
    public Data get(Number160 locationKey, Number160 domainKey, Number160 contentKey) {
        return dataMap.get(new Number480(locationKey, domainKey, contentKey));
    }

    @Override
    public boolean contains(Number160 locationKey, Number160 domainKey, Number160 contentKey) {
        return dataMap.containsKey(new Number480(locationKey, domainKey, contentKey));
    }

    @Override
    public Data remove(Number160 locationKey, Number160 domainKey, Number160 contentKey) {
        Data retVal = dataMap.remove(new Number480(locationKey, domainKey, contentKey));
        //
        try {
            if (retVal.object() instanceof File) {
                File file = (File) retVal.object();
                file.delete();
            }
        } catch (ClassNotFoundException e) {
            logger.error("cannot remove " + e);
        } catch (IOException e) {
            logger.error("cannot remove " + e);
        }
        db.commit();
        return retVal;
    }

    @Override
    public SortedMap<Number480, Data> subMap(Number160 locationKey, Number160 domainKey,
            Number160 fromContentKey, Number160 toContentKey) {
        return dataMap.subMap(new Number480(locationKey, domainKey, fromContentKey), new Number480(
                locationKey, domainKey, toContentKey));
    }

    @Override
    public Map<Number480, Data> subMap(Number160 locationKey) {
        return dataMap.subMap(new Number480(locationKey, Number160.ZERO, Number160.ZERO), new Number480(
                locationKey, Number160.MAX_VALUE, Number160.MAX_VALUE));
    }

    @Override
    public NavigableMap<Number480, Data> map() {
        return dataMap;
    }

    // Maintenance
    @Override
    public void addTimeout(Number160 locationKey, Number160 domainKey, Number160 contentKey, long expiration) {
        Number480 key = new Number480(locationKey, domainKey, contentKey);
        Long oldExpiration = timeoutMap.put(key, expiration);
        Lock lock1 = timeoutLock.lock(expiration);
        try {
            Set<Number480> tmp = putIfAbsent2(expiration, new HashSet<Number480>());
            tmp.add(key);
        } finally {
            timeoutLock.unlock(expiration, lock1);
        }
        if (oldExpiration == null) {
            return;
        }
        Lock lock2 = timeoutLock.lock(oldExpiration);
        try {
            removeRevTimeout(key, oldExpiration);

        } finally {
            timeoutLock.unlock(oldExpiration, lock2);
        }
        db.commit();
    }

    @Override
    public void removeTimeout(Number160 locationKey, Number160 domainKey, Number160 contentKey) {
        Number480 key = new Number480(locationKey, domainKey, contentKey);
        Long expiration = timeoutMap.remove(key);
        if (expiration == null) {
            return;
        }
        Lock lock = timeoutLock.lock(expiration);
        try {
            removeRevTimeout(key, expiration);
        } finally {
            timeoutLock.unlock(expiration, lock);
        }
        db.commit();
    }

    private void removeRevTimeout(Number480 key, Long expiration) {
        if (expiration != null) {
            Set<Number480> tmp2 = timeoutMapRev.get(expiration);
            if (tmp2 != null) {
                tmp2.remove(key);
                if (tmp2.isEmpty()) {
                    timeoutMapRev.remove(expiration);
                }
            }
        }
    }

    @Override
    public Collection<Number480> subMapTimeout(long to) {
        SortedMap<Long, Set<Number480>> tmp = timeoutMapRev.subMap(0L, to);
        Collection<Number480> toRemove = new ArrayList<Number480>();
        for (Map.Entry<Long, Set<Number480>> entry : tmp.entrySet()) {
            Lock lock = timeoutLock.lock(entry.getKey());
            try {
                toRemove.addAll(entry.getValue());
            } finally {
                timeoutLock.unlock(entry.getKey(), lock);
            }
        }
        return toRemove;
    }

    // Protection
    @Override
    public boolean protectDomain(Number160 locationKey, Number160 domainKey, PublicKey publicKey) {
        byte[] encodedPublicKey = publicKey.getEncoded();
        protectedMap.put(new Number320(locationKey, domainKey), encodedPublicKey);
        db.commit();
        return true;
    }

    @Override
    public boolean isDomainProtectedByOthers(Number160 locationKey, Number160 domainKey, PublicKey publicKey) {
        byte[] encodedOther = protectedMap.get(new Number320(locationKey, domainKey));
        if (encodedOther == null) {
            return false;
        }
        // the domain is protected by a public key, but we provided no public
        // key to check, so domain is protected
        if (publicKey == null) {
            return true;
        }
        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(encodedOther);
        KeyFactory keyFactory;
        try {
            String alg = publicKey.getAlgorithm();
            keyFactory = KeyFactory.getInstance(alg);
            PublicKey other = keyFactory.generatePublic(pubKeySpec);
            return !publicKey.equals(other);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return false;
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Number160 findPeerIDForResponsibleContent(Number160 locationKey) {
        return responsibilityMap.get(locationKey);
    }

    public Collection<Number160> findContentForResponsiblePeerID(Number160 peerID) {
        Collection<Number160> contentIDs = responsibilityMapRev.get(peerID);
        if (contentIDs == null) {
            return Collections.<Number160> emptyList();
        } else {
            Lock lock = responsibilityLock.lock(peerID);
            try {
                return new ArrayList<Number160>(contentIDs);
            } finally {
                responsibilityLock.unlock(peerID, lock);
            }
        }
    }

    public boolean updateResponsibilities(Number160 locationKey, Number160 peerId) {
        boolean isNew = true;
        Number160 oldPeerId = responsibilityMap.put(locationKey, peerId);
        // add to the reverse map
        Lock lock1 = responsibilityLock.lock(peerId);
        try {
            Set<Number160> contentIDs = putIfAbsent1(peerId, new HashSet<Number160>());
            contentIDs.add(locationKey);
        } finally {
            responsibilityLock.unlock(peerId, lock1);
        }
        if (oldPeerId != null) {
            isNew = !oldPeerId.equals(peerId);
            Lock lock2 = responsibilityLock.lock(oldPeerId);
            try {
                // clean up reverse map
                removeRevResponsibility(oldPeerId, locationKey);
            } finally {
                responsibilityLock.unlock(oldPeerId, lock2);
            }
        }
        db.commit();
        return isNew;
    }

    private Set<Number160> putIfAbsent1(Number160 peerId, Set<Number160> hashSet) {
        Set<Number160> contentIDs = ((ConcurrentMap<Number160, Set<Number160>>) responsibilityMapRev)
                .putIfAbsent(peerId, hashSet);
        return contentIDs == null ? hashSet : contentIDs;
    }

    private Set<Number480> putIfAbsent2(long expiration, Set<Number480> hashSet) {
        @SuppressWarnings("unchecked")
        Set<Number480> timeouts = ((ConcurrentMap<Long, Set<Number480>>) timeoutMapRev).putIfAbsent(
                expiration, hashSet);
        return timeouts == null ? hashSet : timeouts;
    }

    public void removeResponsibility(Number160 locationKey) {
        Number160 peerId = responsibilityMap.remove(locationKey);
        if (peerId == null) {
            return;
        }
        Lock lock = responsibilityLock.lock(peerId);
        try {
            removeRevResponsibility(peerId, locationKey);
        } finally {
            responsibilityLock.unlock(peerId, lock);
        }
        db.commit();
    }

    private void removeRevResponsibility(Number160 peerId, Number160 locationKey) {
        if (peerId == null || locationKey == null) {
            throw new IllegalArgumentException("both keys must not be null");
        }
        Set<Number160> contentIDs = responsibilityMapRev.get(peerId);
        if (contentIDs != null) {
            contentIDs.remove(locationKey);
            if (contentIDs.isEmpty()) {
                responsibilityMapRev.remove(peerId);
            }
        }
    }
}