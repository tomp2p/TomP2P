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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import net.tomp2p.peers.Number160;

public class StorageMemoryReplication implements ReplicationStorage {
    // Replication
    // maps content (locationKey) to peerid
    final private Map<Number160, Number160> responsibilityMap = new ConcurrentHashMap<Number160, Number160>();

    // maps peerid to content (locationKey)
    final private Map<Number160, Set<Number160>> responsibilityMapRev = new ConcurrentHashMap<Number160, Set<Number160>>();

    final private KeyLock<Number160> responsibilityLock = new KeyLock<Number160>();

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
            if (isNew) {
                Lock lock2 = responsibilityLock.lock(oldPeerId);
                try {
                    // clean up reverse map
                    removeRevResponsibility(oldPeerId, locationKey);
                } finally {
                    responsibilityLock.unlock(oldPeerId, lock2);
                }
            }
        }
        return isNew;
    }

    private Set<Number160> putIfAbsent1(Number160 peerId, Set<Number160> hashSet) {
        Set<Number160> contentIDs = ((ConcurrentMap<Number160, Set<Number160>>) responsibilityMapRev).putIfAbsent(
                peerId, hashSet);
        return contentIDs == null ? hashSet : contentIDs;
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
