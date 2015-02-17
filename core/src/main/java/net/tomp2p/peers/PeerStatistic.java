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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.tomp2p.utils.FIFOCache;

/**
 * Keeps track of the statistics of a given peer.
 * 
 * @author Thomas Bocek
 * 
 */
public class PeerStatistic {

    public static final int RTT_CACHE_SIZE = 5;

    private final AtomicLong lastSeenOnline = new AtomicLong(0);

    private final long created = System.currentTimeMillis();

    private final AtomicInteger successfullyChecked = new AtomicInteger(0);

    private final AtomicInteger failed = new AtomicInteger(0);
    
    private final Number160 peerId;

    private PeerAddress peerAddress;
    
    private boolean local;

    private FIFOCache<RTT> rttCache = new FIFOCache<RTT>(RTT_CACHE_SIZE);

    private long numberOfResponses = 0;

    /**
     * Constructor. Sets the peer address
     * 
     * @param peerAddress
     *            The peer address that belongs to this statistics
     */
    public PeerStatistic(final PeerAddress peerAddress) {
        if (peerAddress == null) {
            throw new IllegalArgumentException("PeerAddress cannot be null");
        }
        this.peerId = peerAddress.peerId();
        this.peerAddress = peerAddress;
    }

    /**
     * Sets the time when last seen online to now.
     * 
     * @return The number of successful checks
     */
    public int successfullyChecked() {
        lastSeenOnline.set(System.currentTimeMillis());
        failed.set(0);
        return successfullyChecked.incrementAndGet();

    }

    /**
     * @return The time last seen online
     */
    public long lastSeenOnline() {
        return lastSeenOnline.get();
    }

    /**
     * @return The number of times the peer has been successfully checked
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
     * @return The time of creating this peer (statistic)
     */
    public long created() {
        return created;
    }

    /**
     * @return The time that this peer is online
     */
    public int onlineTime() {
        return (int) (lastSeenOnline.get() - created);
    }

    public long getNumberOfResponses() {
        return numberOfResponses;
    }

    public void increaseNumberOfResponses() {
        numberOfResponses++;
    }

    /**
     * @return the peer address associated with this peer address
     */
    public PeerAddress peerAddress() {
        return peerAddress;
    }

    /**
     * Set the peer address only if the previous peer address that had the same peer ID.
     * 
     * @param peerAddress
     *            The updated peer ID
     * @return The old peer address
     */
    public PeerAddress peerAddress(final PeerAddress peerAddress) {
        if (!peerId.equals(peerAddress.peerId())) {
            throw new IllegalArgumentException("can only update the same peer address");
        }
        PeerAddress previousPeerAddress = this.peerAddress;
        this.peerAddress = peerAddress;
        return previousPeerAddress;
    }
    
	public PeerStatistic local() {
	    setLocal(true);
		return this;
    }


	public boolean isLocal() {
		return local;
    }
	
	public PeerStatistic setLocal(boolean local) {
		this.local = local;
		return this;
    }

    /**
     * Adds a RTT to the cache. If the provided RTT object is an estimate
     * it will be ignored in case there are already first-hand measurements
     * in the cache. Also any real measurement will clear out any estimates
     * in the cache beforehand. This prevents any mix-up between real
     * measurements and estimates
     *
     * @param rtt   The RTT object that should be added
     * @return      The PeerStatistic object
     */
    public PeerStatistic addRTT(RTT rtt) {
        if (rtt != null && rtt.getRtt() > 0) {
            // If we have estimates in the cache,
            // clear cache before adding "real" measurement
            if (containsEstimates()) {
                if (!rtt.isEstimated())
                    rttCache.clear();

                rttCache.add(rtt);
                return this;

            // Only add measured RTTs to our cache if we have some already
            // Estimates will be ignored after we received at least 1 "real" RTT
            } else if (!rtt.isEstimated() || rttCache.isEmpty()) {
                rttCache.add(rtt);
                return this;
            }
        }
        return this;
    }

    /**
     * @return  True, if the RTT cache contains estimates, false if empty or
     *          cache is empty.
     */
    public boolean containsEstimates() {
        return !rttCache.isEmpty() && rttCache.peek().isEstimated();
    }

    /**
     * @return Most recent RTT or null if cache is empty
     */
    public RTT getLatestRTT() {
        if (rttCache.isEmpty())
            return null;

        return rttCache.peekTail();
    }

    /**
     * Get the mean of the last 5 RTTs
     *
     * @return Average RTT in milliseconds or -1 if cache is empty.
     */
    public long getMeanRTT() {
        if (rttCache.isEmpty())
            return -1;

        long sum = 0;
        for (Iterator<RTT> iterator = rttCache.iterator(); iterator.hasNext(); ) {
            RTT next = iterator.next();
            sum += next.getRtt();
        }
        return sum / rttCache.size();
    }

    /**
     * How many RTT measurements are in the cache
     *
     * @return  Number of RTT objects in cache. Number between
     *          0 and RTT_CACHE_SIZE
     */
    public int getRTTCount() {
        return rttCache.size();
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
