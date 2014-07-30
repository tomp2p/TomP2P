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

package net.tomp2p.tracker;

import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.builder.Builder;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.p2p.builder.SignatureBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerFilter;

public abstract class TrackerBuilder<K extends TrackerBuilder<K>> extends DefaultConnectionConfiguration
        implements SignatureBuilder<K>, Builder {
    public final static Number160 DEFAULT_DOMAIN = Number160.createHash("default-tracker");

    protected final static FutureTracker FUTURE_TRACKER_SHUTDOWN = new FutureTracker()
            .failed("Peer is shutting down");

    protected final PeerTracker peer;

    protected final Number160 locationKey;

    protected Number160 domainKey;

    protected RoutingConfiguration routingConfiguration;

    protected TrackerConfiguration trackerConfiguration;

    protected FutureChannelCreator futureChannelCreator;

    private Set<Number160> knownPeers;

    private K self;

    private KeyPair keyPair = null;
    
    private Collection<PeerFilter> peerFilters;


    public TrackerBuilder(PeerTracker peer, Number160 locationKey) {
        this.peer = peer;
        this.locationKey = locationKey;
    }

    public void self(K self) {
        this.self = self;
    }

    public Number160 domainKey() {
        return domainKey;
    }

    public Number160 locationKey() {
        return locationKey;
    }

    public K domainKey(Number160 domainKey) {
        this.domainKey = domainKey;
        return self;
    }

    public RoutingConfiguration routingConfiguration() {
        return routingConfiguration;
    }

    public K routingConfiguration(RoutingConfiguration routingConfiguration) {
        this.routingConfiguration = routingConfiguration;
        return self;
    }

    public TrackerConfiguration trackerConfiguration() {
        return trackerConfiguration;
    }

    public K trackerConfiguration(TrackerConfiguration trackerConfiguration) {
        this.trackerConfiguration = trackerConfiguration;
        return self;
    }

    public FutureChannelCreator futureChannelCreator() {
        return futureChannelCreator;
    }

    public K futureChannelCreator(FutureChannelCreator futureChannelCreator) {
        this.futureChannelCreator = futureChannelCreator;
        return self;
    }

    public Set<Number160> knownPeers() {
        return knownPeers;
    }

    public K knownPeers(Set<Number160> knownPeers) {
        this.knownPeers = knownPeers;
        return self;
    }

    public void preBuild(String name) {
        if (domainKey == null) {
            domainKey = DEFAULT_DOMAIN;
        }
        if (routingConfiguration == null) {
            routingConfiguration = new RoutingConfiguration(5, 10, 2);
        }
        if (trackerConfiguration == null) {
            int size = peer.peerMap().size() + 1;
            trackerConfiguration = new TrackerConfiguration(Math.min(size, 3), 5, 3, 30);
        }
        
		if (futureChannelCreator == null
		        || (futureChannelCreator.channelCreator() != null && futureChannelCreator.channelCreator().isShutdown())) {
			futureChannelCreator = peer.peer().connectionBean().reservation()
			        .create(routingConfiguration, trackerConfiguration, this);
		}
        
    }

    public abstract FutureTracker start();

    public RoutingBuilder createBuilder(RoutingConfiguration routingConfiguration2) {
        RoutingBuilder routingBuilder = new RoutingBuilder();
        routingBuilder.parallel(routingConfiguration.parallel());
        routingBuilder.setMaxNoNewInfo(routingConfiguration.maxNoNewInfo(0));
        routingBuilder.maxDirectHits(routingConfiguration.maxDirectHits());
        routingBuilder.maxFailures(routingConfiguration.maxFailures());
        routingBuilder.maxSuccess(routingConfiguration.maxSuccess());
        return routingBuilder;
    }

    /**
     * @return Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     */
    public boolean isSign() {
        return keyPair != null;
    }

    /**
     * @param signMessage
     *            Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     * @return This class
     */
    public K sign(final boolean signMessage) {
        if (signMessage) {
            sign();
        } else {
            this.keyPair = null;
        }
        return self;
    }

    /**
     * @return Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     */
    public K sign() {
        this.keyPair = peer.peer().peerBean().keyPair();
        return self;
    }

    /**
     * @param keyPair
     *            The keyPair to sing the complete message. The key will be attached to the message and stored
     *            potentially with a data object (if there is such an object in the message).
     * @return This class
     */
    public K keyPair(KeyPair keyPair) {
        this.keyPair = keyPair;
        return self;
    }

    /**
     * @return The current keypair to sign the message. If null, no signature is applied.
     */
    public KeyPair keyPair() {
        return keyPair;
    }

	public K addPeerFilter(PeerFilter peerFilter) {
    	if(peerFilters == null) {
    		//most likely we have 1-2 filters
    		peerFilters = new ArrayList<PeerFilter>(2);
    	}
    	peerFilters.add(peerFilter);
    	return self;
    }
    
    public Collection<PeerFilter> peerFilters() {
    	return peerFilters;
    }
}
