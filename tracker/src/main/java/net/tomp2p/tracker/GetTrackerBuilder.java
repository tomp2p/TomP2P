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

import net.tomp2p.p2p.EvaluatingSchemeTracker;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.SimpleBloomFilter;

public class GetTrackerBuilder extends TrackerBuilder<GetTrackerBuilder> {
    private EvaluatingSchemeTracker evaluatingScheme;

    private Set<Number160> knownPeers;

    private boolean expectAttachement = false;

    public GetTrackerBuilder(PeerTracker peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public EvaluatingSchemeTracker evaluatingScheme() {
        return evaluatingScheme;
    }

    public GetTrackerBuilder evaluatingScheme(EvaluatingSchemeTracker evaluatingScheme) {
        this.evaluatingScheme = evaluatingScheme;
        return this;
    }

    public boolean isExpectAttachement() {
        return expectAttachement;
    }

    public GetTrackerBuilder expectAttachement() {
        this.expectAttachement = true;
        return this;
    }

    public GetTrackerBuilder expectAttachement(boolean expectAttachement) {
        this.expectAttachement = expectAttachement;
        return this;
    }

    public FutureTracker start() {
        if (peer.peer().isShutdown()) {
            return FUTURE_TRACKER_SHUTDOWN;
        }

        if(!expectAttachement) {
            forceUDP(true);
        }
        
        preBuild("get-tracker-builder");

        if (knownPeers == null) {
            knownPeers = new SimpleBloomFilter<Number160>(1024, 1024);
        }
        
        if (evaluatingScheme == null) {
        	evaluatingScheme = new VotingSchemeTracker();
        }
        
        
        
        return peer.distributedTracker().get(this);
    }
}
