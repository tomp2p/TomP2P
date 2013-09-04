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

package net.tomp2p.p2p.builder;

import java.util.Set;

import net.tomp2p.connection2.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.TrackerConfiguration;
import net.tomp2p.peers.Number160;

public abstract class TrackerBuilder<K extends TrackerBuilder<K>> extends DefaultConnectionConfiguration {
    public final static Number160 DEFAULT_DOMAIN = Number160.createHash("default-tracker");

    protected final static FutureTracker FUTURE_TRACKER_SHUTDOWN = new FutureTracker()
            .setFailed("Peer is shutting down");

    protected final Peer peer;

    protected final Number160 locationKey;

    protected Number160 domainKey;

    protected RoutingConfiguration routingConfiguration;

    protected TrackerConfiguration trackerConfiguration;

    protected FutureChannelCreator futureChannelCreator;
    
    private Set<Number160> knownPeers;

    private K self;

    public TrackerBuilder(Peer peer, Number160 locationKey) {
        this.peer = peer;
        this.locationKey = locationKey;
    }

    public void self(K self) {
        this.self = self;
    }

    public Number160 getDomainKey() {
        return domainKey;
    }
    
    public Number160 getLocationKey() {
        return locationKey;
    }

    public K setDomainKey(Number160 domainKey) {
        this.domainKey = domainKey;
        return self;
    }

    public RoutingConfiguration getRoutingConfiguration() {
        return routingConfiguration;
    }

    public K setRoutingConfiguration(RoutingConfiguration routingConfiguration) {
        this.routingConfiguration = routingConfiguration;
        return self;
    }

    public TrackerConfiguration getTrackerConfiguration() {
        return trackerConfiguration;
    }

    public K setTrackerConfiguration(TrackerConfiguration trackerConfiguration) {
        this.trackerConfiguration = trackerConfiguration;
        return self;
    }

    public FutureChannelCreator getFutureChannelCreator() {
        return futureChannelCreator;
    }

    public K setFutureChannelCreator(FutureChannelCreator futureChannelCreator) {
        this.futureChannelCreator = futureChannelCreator;
        return self;
    }
    
    public Set<Number160> getKnownPeers() {
        return knownPeers;
    }

    public K setKnownPeers(Set<Number160> knownPeers) {
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
            int size = peer.getPeerBean().peerMap().size() + 1;
            trackerConfiguration = new TrackerConfiguration(Math.min(size, 3), 5, 3, 30);
        }
        if (futureChannelCreator == null) {
            int conn = Math.max(routingConfiguration.getParallel(), trackerConfiguration.getParallel());
            futureChannelCreator = peer.getConnectionBean().reservation().create(conn, 0);
        }
    }

    public abstract FutureTracker start();

    public RoutingBuilder createBuilder(RoutingConfiguration routingConfiguration2) {
        RoutingBuilder routingBuilder = new RoutingBuilder();
        routingBuilder.setParallel(routingConfiguration.getParallel());
        routingBuilder.setMaxNoNewInfo(routingConfiguration.getMaxNoNewInfo(0));
        routingBuilder.setMaxDirectHits(routingConfiguration.getMaxDirectHits());
        routingBuilder.setMaxFailures(routingConfiguration.getMaxFailures());
        routingBuilder.setMaxSuccess(routingConfiguration.getMaxSuccess());
        return routingBuilder;
    }
}
