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

import java.util.NavigableSet;
import java.util.SortedSet;

import net.tomp2p.futures.FutureShutdown;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

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

    private Filter filter;

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

    public ShutdownBuilder filter(final Filter filter) {
        this.filter = filter;
        return this;
    }

    public Filter filter() {
        return filter;
    }

    /**
     * Start the shutdown. This method returns immediately with a future object. The future object can be used to block
     * or add a listener.
     * 
     * @return The future object
     */
    public FutureShutdown start() {
        setForceUDP();
        if (routingConfiguration == null) {
            routingConfiguration = new RoutingConfiguration(0, 0, 0);
        }
        if (requestP2PConfiguration == null) {
            requestP2PConfiguration = new RequestP2PConfiguration(10, 0, 10);
        }
        preBuild("shutdown-builder");
        if (peer.isShutdown()) {
            return FUTURE_SHUTDOWN;
        }
        if (filter == null) {
            filter = new Filter() {
                @Override
                public NavigableSet<PeerAddress> filter(NavigableSet<PeerAddress> closePeers) {
                    return closePeers;
                }
            };
        }
        return peer.getDistributedHashMap().quit(this);
    }

    @Override
    public ShutdownBuilder setDomainKey(final Number160 domainKey) {
        throw new IllegalArgumentException("Cannot be set here");
    }

    /**
     * Adds the possibility to filter the peers, to add more or remove peers for the shutdown process.
     * 
     * @param closePeers
     *            The close peers that were found in the peer map.
     * @return The set modified by the user, may be the same set or a new one
     */
    public NavigableSet<PeerAddress> filter(NavigableSet<PeerAddress> closePeers) {
        return filter.filter(closePeers);
    }

    /**
     * The filter can be used to add or remove peers to notify for a shutdown.
     * 
     * @author Thomas Bocek
     * 
     */
    public static interface Filter {
        /**
         * Adds the possibility to filter the peers, to add more or remove peers for the shutdown process
         * 
         * @param closePeers
         *            The close peers that were found in the peer map.
         * @return The set modified by the user, may be the same set or a new one
         */
        public NavigableSet<PeerAddress> filter(NavigableSet<PeerAddress> closePeers);
    }
}
