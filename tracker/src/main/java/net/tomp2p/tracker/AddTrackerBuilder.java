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

import net.tomp2p.futures.FutureCreator;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;

public class AddTrackerBuilder extends TrackerBuilder<AddTrackerBuilder> {
    private Data attachement;

    private SimpleBloomFilter<Number160> bloomFilter;

    private FutureCreator<FutureTracker> defaultDirectReplication;

    private FutureCreator<FutureLateJoin<FutureResponse>> defaultPEX;

    private boolean tcpPEX = false;
    
    private PeerAddress peerAddressToAnnounce;

    public AddTrackerBuilder(PeerTracker peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public Data attachement() {
        return attachement;
    }

    public AddTrackerBuilder attachement(Data attachement) {
        this.attachement = attachement;
        return this;
    }

    public SimpleBloomFilter<Number160> getBloomFilter() {
        return bloomFilter;
    }

    public AddTrackerBuilder setBloomFilter(SimpleBloomFilter<Number160> bloomFilter) {
        this.bloomFilter = bloomFilter;
        return this;
    }

    public FutureCreator<FutureTracker> getDefaultDirectReplication() {
        return defaultDirectReplication;
    }

    public AddTrackerBuilder setDefaultDirectReplication(FutureCreator<FutureTracker> defaultDirectReplication) {
        this.defaultDirectReplication = defaultDirectReplication;
        return this;
    }

    public FutureCreator<FutureLateJoin<FutureResponse>> getDefaultPEX() {
        return defaultPEX;
    }

    public AddTrackerBuilder setDefaultPEX(FutureCreator<FutureLateJoin<FutureResponse>> defaultPEX) {
        this.defaultPEX = defaultPEX;
        return this;
    }

    public boolean isTcpPEX() {
        return tcpPEX;
    }

    public AddTrackerBuilder setTcpPEX() {
        this.tcpPEX = true;
        return this;
    }

    public AddTrackerBuilder setTcpPEX(boolean tcpPEX) {
        this.tcpPEX = tcpPEX;
        return this;
    }
    
    public PeerAddress peerAddressToAnnounce() {
        return peerAddressToAnnounce;
    }

    public AddTrackerBuilder peerAddressToAnnounce(PeerAddress peerAddressToAnnounce) {
        this.peerAddressToAnnounce = peerAddressToAnnounce;
        return this;
    }

    @Override
    public FutureTracker start() {
        if (peer.peer().isShutdown()) {
            return FUTURE_TRACKER_SHUTDOWN;
        }
        if(attachement == null) {
            forceUDP(true);
        }
        preBuild("add-tracker-build");

        if (bloomFilter == null) {
            bloomFilter = new SimpleBloomFilter<Number160>(1024, 1024);
        }
        
        
        // add myself to my local tracker, since we use a mesh we are part of
        // the tracker mesh as well.
        peer.trackerStorage().put(new Number320(locationKey, domainKey), peer.peerAddress(), keyPair() == null? null: keyPair().getPublic(),
                        attachement);
        final FutureTracker futureTracker = peer.distributedTracker().add(this);

        return futureTracker;
    }
}
