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
package net.tomp2p.peers;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Keeps track of the statistics of a given peer.
 * 
 * @author Thomas Bocek
 * 
 */
public class PeerStatistic implements Serializable {
    
	private static final long serialVersionUID = -6225586345726672194L;

    private final long created = System.currentTimeMillis();

    private final AtomicLong lastSeenOnline = new AtomicLong(0);
    private final AtomicInteger successfullyChecked = new AtomicInteger(0);
    private final AtomicInteger failed = new AtomicInteger(0);
    
    private final Number160 peerId;
    private PeerAddress peerAddress;

    public PeerStatistic(final PeerAddress peerAddress) {
        if (peerAddress == null) {
            throw new IllegalArgumentException("PeerAddress cannot be null.");
        }
        this.peerId = peerAddress.peerId();
        this.peerAddress = peerAddress;
    }

    /**
     * Sets the time when last seen online to now.
     * 
     * @return The number of successful checks.
     */
    public int successfullyChecked() {
        lastSeenOnline.set(System.currentTimeMillis());
        failed.set(0);
        return successfullyChecked.incrementAndGet();

    }

    /**
     * Gets the time the peer has last been seen online.
     * 
     * @return The time the peer has last been seen online.
     */
    public long lastSeenOnline() {
        return lastSeenOnline.get();
    }

    /**
     * Gets the number of times the peer has been successfully checked.
     * 
     * @return The number of times the peer has been successfully checked.
     */
    public int successfullyCheckedCounter() {
        return successfullyChecked.get();
    }

    /**
     * Increases the failed counter.
     * 
     * @return The number of failed checks.
     */
    public int failed() {
        return failed.incrementAndGet();
    }

    /**
     * @return The time of this PeerStatistic creation.
     */
    public long created() {
        return created;
    }

    /**
     * @return The time that this peer is online.
     */
    public int onlineTime() {
        return (int) (lastSeenOnline.get() - created);
    }

    /**
     * @return The PeerAddress associated with this peer.
     */
    public PeerAddress peerAddress() {
        return peerAddress;
    }

    /**
     * Sets a new PeerAddress, but only if the previous had the same peer ID.
     * 
     * @param peerAddress
     *            The new peer address.
     * @return The old peer address.
     */
    public PeerAddress peerAddress(final PeerAddress peerAddress) {
        if (!peerId.equals(peerAddress.peerId())) {
            throw new IllegalArgumentException("Can only update PeerAddress with the same peer ID.");
        }
        PeerAddress previous = this.peerAddress;
        this.peerAddress = peerAddress;
        return previous;
    }
    
    @Override
    public int hashCode() {
        return peerId.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
        	return true;
        }
        if(!(obj instanceof PeerStatistic)) {
        	return false;
        }
        PeerStatistic p = (PeerStatistic) obj;
        return p.peerId.equals(peerId);
    }
}
