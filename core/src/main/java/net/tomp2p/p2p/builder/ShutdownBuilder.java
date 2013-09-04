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

package net.tomp2p.p2p.builder;

import net.tomp2p.futures.FutureShutdown;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;

/**
 * Set the configuration options for the shutdown command. The shutdown does first a rounting, searches for its close
 * peers and then send a quit message so that the other peers knows that this peer is offline.
 * 
 * @author Thomas Bocek
 * 
 */
public class ShutdownBuilder extends DHTBuilder<ShutdownBuilder> {
    private static final FutureShutdown FUTURE_SHUTDOWN = new FutureShutdown()
            .setFailed("Peer is shutting down");

    /**
     * Constructor.
     * 
     * @param peer
     *            The peer that runs the routing and quit messages
     */
    public ShutdownBuilder(final Peer peer) {
        super(peer, peer.getPeerID());
        self(this);
    }

    /**
     * Start the shutdown. This method returns immediately with a future object. The future object can be used to block
     * or add a listener.
     * 
     * @return The future object
     */
    public FutureShutdown start() {
        setForceUDP();
        preBuild("shutdown-builder");
        if (peer.isShutdown()) {
            return FUTURE_SHUTDOWN;
        }
        return peer.getDistributedHashMap().quit(this);
    }

    @Override
    public ShutdownBuilder setDomainKey(final Number160 domainKey) {
        throw new IllegalArgumentException("Cannot be set here");
    }
}
