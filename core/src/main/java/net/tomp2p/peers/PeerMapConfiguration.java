/*
 * Copyright 2013 Thomas Bocek
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

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.p2p.DefaultPeerStatisticComparator;
import net.tomp2p.p2p.PeerStatisticComparator;

/**
 * The class that holds configuration settings for the {@link PeerMap}.
 * 
 * @author Thomas Bocek
 */
public class PeerMapConfiguration {

    private final Number160 self;
    private int[] bagSizesVerified = new int[Number160.BITS];
    private int[] bagSizesOverflow = new int[Number160.BITS];
    private int offlineTimeout;
    private int shutdownTimeout;
    private int exceptionTimeout;
    private int offlineCount;
    //we'll add 1-2 filters
    private Collection<PeerMapFilter> peerMapFilters = new ArrayList<PeerMapFilter>(2);
    private Maintenance maintenance;
    private boolean peerVerification;
    private PeerStatisticComparator peerStatisticComparator;

    /**
     * Constructor with reasonable defaults.
     * 
     * @param self
     *            The peer ID of this peer
     */
    public PeerMapConfiguration(final Number160 self) {
        this.self = self;
        setDoublingVerifiedBagSizes();
        setDoublingOverflowBagSizes();
        offlineTimeout = 60;
        shutdownTimeout = 20;
        exceptionTimeout = 120;
        offlineCount = 3;
        maintenance = new DefaultMaintenance(4, new int[] { 2, 4, 8, 16, 32, 64 });
        peerVerification = true;
        setPeerStatisticComparator(new DefaultPeerStatisticComparator());
    }

    /**
     * @return The peer ID of this peer
     */
    public Number160 self() {
        return self;
    }

    /**
     * Each distance bit has its own bag.
     * This is the size of the verified peers that are known to be online.
     * @return
     */
    public int getVerifiedBagSize(final int bag) {
        return bagSizesVerified[bag];
    }

    /**
     * Each distance bit has its own bag this is the size of the verified peers are know to be online
     * @return this class
     */
    public int[] getVerifiedBagSizes() {
        return bagSizesVerified;
    }

    /**
     * Sets the bag size for the verified peers to a fixed number
     *
     * @param fixedVerifiedBagSize The size for each bag to be set
     * @return This PeerMapConfiguration object
     */
    public PeerMapConfiguration setFixedVerifiedBagSizes(final int fixedVerifiedBagSize) {
        for (int i=0; i<this.bagSizesVerified.length; i++)
        {
            this.bagSizesVerified[i] = fixedVerifiedBagSize;
        }
        return this;
    }

    /**
     * Sets the bag sizes for the verified peers to (8, 8, 8, ... 8, 16, 32, 64, 128)
     * @return This PeerMapConfiguration object
     */
    public PeerMapConfiguration setDoublingVerifiedBagSizes() {
        for (int i = 0; i < Number160.BITS; i++) {
            if (i < Number160.BITS - 4) {
                bagSizesVerified[i] = 8;
            }else {
                bagSizesVerified[i] = 128 / (int)Math.pow(2,Number160.BITS - i - 1);
            }
        }
        return this;
    }

    /**
     * Allows to define custom bag sizes for the verified peers
     * @param bagSizesVerified Array of length Number160.BITS, specifying the size
     *                         for each bag. bagSizesVerified[0] is the closest bag,
     *                         bagSizesVerified[159] is the most distant bag.
     * @return This PeerMapConfiguration object
     */
    public PeerMapConfiguration setBagSizesVerified(final int[] bagSizesVerified) {
        if (bagSizesVerified.length != Number160.BITS)
            throw new IllegalArgumentException("The array of bag sizes must have length of " + Number160.BITS);
        this.bagSizesVerified = bagSizesVerified;
        return this;
    }

    /**
     * @param bag The bit distance with 0 being the closest, 159 being the most distant
     * @return The size of the bag of the given bit distance.
     */
    public int getOverflowBagSize(final int bag) {
        return bagSizesOverflow[bag];
    }

    /**
     * @return The array of sizes for the overflow peer bags
     */
    public int[] getOverflowBagSizes() {
        return bagSizesOverflow;
    }

    /**
     * Sets the bag size for the overflow peers to a fixed number
     *
     * @param fixedOverflowBagSize The size for each bag to be set
     * @return This PeerMapConfiguration object
     */
    public PeerMapConfiguration setFixedOverflowBagSizes(final int fixedOverflowBagSize) {
        for (int i=0; i<this.bagSizesOverflow.length; i++)
        {
            this.bagSizesOverflow[i] = fixedOverflowBagSize;
        }
        return this;
    }

    /**
     * Sets the bag sizes for the overflow peers to (8, 8, 8, ... 8, 16, 32, 64, 128)
     * @return This PeerMapConfiguration object
     */
    public PeerMapConfiguration setDoublingOverflowBagSizes() {
        for (int i = 0; i < Number160.BITS; i++) {
            if (i < Number160.BITS - 4) {
                bagSizesOverflow[i] = 8;
            }else {
                bagSizesOverflow[i] = 128 / (int)Math.pow(2,Number160.BITS - i - 1);
            }
        }
        return this;
    }

    /**
     * Allows to define custom bag sizes for the overflow peers
     * @param bagSizesOverflow Array of length Number160.BITS, specifying the size
     *                         for each bag. bagSizesOverflow[0] is the closest bag,
     *                         bagSizesOverflow[159] is the most distant bag.
     * @return This PeerMapConfiguration object
     */
    public PeerMapConfiguration setBagSizesOverflow(final int[] bagSizesOverflow) {
        if (bagSizesOverflow.length != Number160.BITS)
            throw new IllegalArgumentException("The array of bag sizes must have length of " + Number160.BITS);
        this.bagSizesOverflow = bagSizesOverflow;
        return this;
    }

    /**
     * @return The time a peer is considered offline in seconds. This is important, since we see that a peer is offline
     *         and an other peer reports this peer, we don't want to add it into our map. Thus, there is a map that
     *         keeps track of such peers. This also means that a fast reconnect is not possible and a peer has to wait
     *         until the timeout to rejoin
     */
    public int offlineTimeout() {
        return offlineTimeout;
    }

    /**
     * @param offlineTimeout
     *            The time a peer is considered offline in seconds. This is important, since we see that a peer is
     *            offline and an other peer reports this peer, we don't want to add it into our map. Thus, there is a
     *            map that keeps track of such peers. This also means that a fast reconnect is not possible and a peer
     *            has to wait until the timeout to rejoin
     * @return this class
     */
    public PeerMapConfiguration offlineTimeout(final int offlineTimeout) {
        this.offlineTimeout = offlineTimeout;
        return this;
    }

    /**
     * @return The number of times that the peer is not reachable. After that, the peer is considered offline
     */
    public int offlineCount() {
        return offlineCount;
    }

    /**
     * @param offlineCount
     *            The number of times that the peer is not reachabel. After that the peer is considered offline
     * @return this class
     */
    public PeerMapConfiguration offlineCount(final int offlineCount) {
        this.offlineCount = offlineCount;
        return this;
    }

    /**
     * @return These filters can be set to not accept certain peers
     */
    public Collection<PeerMapFilter> peerMapFilters() {
        return peerMapFilters;
    }

    /**
     * @param peerMapFilter
     *            This filter can be set to not accept certain peers
     * @return this class
     */
    public PeerMapConfiguration addMapPeerFilter(final PeerMapFilter peerMapFilter) {
        peerMapFilters.add(peerMapFilter);
        return this;
    }

    /**
     * @return The instance that is responsible for maintenance
     */
    public Maintenance maintenance() {
        return maintenance;
    }

    /**
     * @param maintenance
     *            The class that is responsible for maintenance
     * @return this class
     */
    public PeerMapConfiguration maintenance(final Maintenance maintenance) {
        this.maintenance = maintenance;
        return this;
    }
    
    /**
     * @return The time a peer is considered offline (shutdown) in seconds. This is important, since we see that a peer is
     *            offline and an other peer reports this peer, we don't want to add it into our map. Thus, there is a
     *            map that keeps track of such peers. This also means that a fast reconnect is not possible and a peer
     *            has to wait until the timeout to rejoin
     */
    public int shutdownTimeout() {
        return shutdownTimeout;
    }

    /**
     * @param shutdownTimeout
     *            The time a peer is considered offline (shutdown) in seconds. This is important, since we see that a peer is
     *            offline and an other peer reports this peer, we don't want to add it into our map. Thus, there is a
     *            map that keeps track of such peers. This also means that a fast reconnect is not possible and a peer
     *            has to wait until the timeout to rejoin
     * @return this class
     */
    public PeerMapConfiguration shutdownTimeout(final int shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
        return this;
    }
    
    /**
     * @return The time, in seconds, a peer is considered offline (exception). This is important, since we see that a peer is offline
     *         and an other peer reports this peer, we don't want to add it into our map. Thus, there is a map that
     *         keeps track of such peers. This also means that a fast reconnect is not possible and a peer has to wait
     *         until the timeout to rejoin
     */
    public int exceptionTimeout() {
        return exceptionTimeout;
    }

    /**
     * @param exceptionTimeout
     *            The time, in seconds, a peer is considered offline (exception). This is important, since we see that a peer is
     *            offline and an other peer reports this peer, we don't want to add it into our map. Thus, there is a
     *            map that keeps track of such peers. This also means that a fast reconnect is not possible and a peer
     *            has to wait until the timeout to rejoin
     * @return this class
     */
    public PeerMapConfiguration exceptionTimeout(final int exceptionTimeout) {
        this.exceptionTimeout = exceptionTimeout;
        return this;
    }
    
    public boolean isPeerVerification() {
    	return peerVerification;
    }

    public PeerMapConfiguration peerNoVerification() {
    	peerVerification = false;
    	return this;
    }

    public PeerMapConfiguration peerVerification(boolean reerVerification) {
    	this.peerVerification = reerVerification;
    	return this;
    }

    public PeerStatisticComparator getPeerStatisticComparator() {
        return peerStatisticComparator;
    }

    public PeerMapConfiguration setPeerStatisticComparator(PeerStatisticComparator peerStatisticComparator) {
        this.peerStatisticComparator = peerStatisticComparator;
        return this;
    }
}
