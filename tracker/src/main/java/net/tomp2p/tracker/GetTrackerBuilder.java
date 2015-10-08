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

import java.util.Set;

import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.SimpleBloomFilter;

public class GetTrackerBuilder extends TrackerBuilder<GetTrackerBuilder> {
    
    private Set<Number160> knownPeers;

    private boolean expectAttachment = false;

    public GetTrackerBuilder(PeerTracker peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public boolean isExpectAttachment() {
        return expectAttachment;
    }

    public GetTrackerBuilder expectAttachment() {
        this.expectAttachment = true;
        return this;
    }

    public GetTrackerBuilder expectAttachment(boolean expectAttachment) {
        this.expectAttachment = expectAttachment;
        return this;
    }

    public FutureTracker start() {
        if (peer.peer().isShutdown()) {
            return FUTURE_TRACKER_SHUTDOWN;
        }

        if(!expectAttachment) {
            forceUDP(true);
        }
        
        preBuild("get-tracker-builder");

        if (knownPeers == null) {
            knownPeers = new SimpleBloomFilter<Number160>(1024, 1024);
        }
        
        return peer.distributedTracker().get(this);
    }
}
