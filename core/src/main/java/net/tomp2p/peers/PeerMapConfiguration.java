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

/**
 * The class that holds configuration settings for the {@link PeerMap}.
 * 
 * @author Thomas Bocek
 * 
 */
public class PeerMapConfiguration {

    private final Number160 self;
    private int bagSizeVerified;
    private int bagSizeOverflow;
    private int offlineTimeout;
    private int shutdownTimeout;
    private int exceptionTimeout;
    private int offlineCount;
    //we'll add 1-2 filters
    private Collection<PeerFilter> peerFilters = new ArrayList<PeerFilter>(2);
    private Maintenance maintenance;
    private boolean peerVerification;

    /**
     * Constructor with reasonable defaults.
     * 
     * @param self
     *            The peer ID of this peer
     */
    public PeerMapConfiguration(final Number160 self) {
        this.self = self;
        // CHECKSTYLE:OFF
        bagSizeVerified = 10;
        bagSizeOverflow = 10;
        offlineTimeout = 60;
        shutdownTimeout = 20;
        exceptionTimeout = 120;
        offlineCount = 3;
        maintenance = new DefaultMaintenance(4, new int[] { 2, 4, 8, 16, 32, 64 });
        peerVerification = true;
        // CHECKSTYLE:ON
    }

    /**
     * @return The peer ID of this peer
     */
    public Number160 self() {
        return self;
    }

    /**
     * @return Each distance bit has its own bag this is the size of the verified peers are know to be online
     */
    public int bagSizeVerified() {
        return bagSizeVerified;
    }

    /**
     * @param bagSizeVerified
     *            Each distance bit has its own bag this is the size of the verified peers are know to be online
     * @return this class
     */
    public PeerMapConfiguration bagSizeVerified(final int bagSizeVerified) {
        this.bagSizeVerified = bagSizeVerified;
        return this;
    }

    /**
     * @return the Each distance bit has its own bag this is the size of the non-verified peers that may have been
     *         reported by other peers
     */
    public int bagSizeOverflow() {
        return bagSizeOverflow;
    }

    /**
     * @param bagSizeOverflow
     *            Each distance bit has its own bag this is the size of the non-verified peers that may have been
     *            reported by other peers
     * @return this class
     */
    public PeerMapConfiguration bagSizeOverflow(final int bagSizeOverflow) {
        this.bagSizeOverflow = bagSizeOverflow;
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
     * @return The number of times that the peer is not reachabel. After that the peer is considered offline
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
     * @return This filter can be set to not accept certain peers
     */
    public Collection<PeerFilter> peerFilters() {
        return peerFilters;
    }

    /**
     * @param peerFilter
     *            This filter can be set to not accept certain peers
     * @return this class
     */
    public PeerMapConfiguration addPeerFilter(final PeerFilter peerFilter) {
        peerFilters.add(peerFilter);
        return this;
    }

    /**
     * @return The class that is responsible for maintenance
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
     * @return The time a peer is considered offline (shutdown) in seconds. This is important, since we see that a peer is offline
     *         and an other peer reports this peer, we don't want to add it into our map. Thus, there is a map that
     *         keeps track of such peers. This also means that a fast reconnect is not possible and a peer has to wait
     *         until the timeout to rejoin
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
     * @return The time a peer is considered offline (exception) in seconds. This is important, since we see that a peer is offline
     *         and an other peer reports this peer, we don't want to add it into our map. Thus, there is a map that
     *         keeps track of such peers. This also means that a fast reconnect is not possible and a peer has to wait
     *         until the timeout to rejoin
     */
    public int exceptionTimeout() {
        return exceptionTimeout;
    }

    /**
     * @param exceptionTimeout
     *            The time a peer is considered offline (exception) in seconds. This is important, since we see that a peer is
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
}
