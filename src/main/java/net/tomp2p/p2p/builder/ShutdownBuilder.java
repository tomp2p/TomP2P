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

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureShutdown;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;

/**
 * Set the configuration options for the shutdown command. The shutdown does first a rounting, searches for its close
 * peers and then send a quit message so that the other peers knows that this peer is offline.
 * 
 * @author Thomas Bocek
 * 
 */
public class ShutdownBuilder implements BasicBuilder<ShutdownBuilder> {
    private static final FutureShutdown FUTURE_SHUTDOWN = new FutureShutdown().setFailed("Peer is shutting down");
    private final Peer peer;

    private RoutingConfiguration routingConfiguration;

    private RequestP2PConfiguration requestP2PConfiguration;

    private boolean signMessage = false;

    private boolean manualCleanup = false;

    private FutureChannelCreator futureChannelCreator;

    private final Number160 locationKey;

    /**
     * Constructor.
     * 
     * @param peer
     *            The peer that runs the routing and quit messages
     */
    public ShutdownBuilder(final Peer peer) {
        this.peer = peer;
        this.locationKey = peer.getPeerID();
    }

    /**
     * Start the shutdown. This method returns immediately with a future object. The future object can be used to block
     * or add a listener.
     * 
     * @return The future object
     */
    public FutureShutdown start() {
        if (peer.isShutdown()) {
            return FUTURE_SHUTDOWN;
        }
        if (routingConfiguration == null) {
            routingConfiguration = new RoutingConfiguration(5, 10, 2);
        }
        if (requestP2PConfiguration == null) {
            requestP2PConfiguration = new RequestP2PConfiguration(3, 5, 3);
        }
        int size = peer.getPeerBean().getPeerMap().size() + 1;
        requestP2PConfiguration = requestP2PConfiguration.adjustMinimumResult(size);
        if (futureChannelCreator == null) {
            futureChannelCreator = peer.reserve(routingConfiguration, requestP2PConfiguration, "shutdown-builder");
        }
        return peer.getDistributedHashMap().quit(peer.getConnectionBean().getConnectionReservation(), this);
    }

    /**
     * @return The configuration for the routing options
     */
    @Override
    public RoutingConfiguration getRoutingConfiguration() {
        return routingConfiguration;
    }

    /**
     * @param routingConfiguration
     *            The configuration for the routing options
     * @return This object
     */
    @Override
    public ShutdownBuilder setRoutingConfiguration(final RoutingConfiguration routingConfiguration) {
        this.routingConfiguration = routingConfiguration;
        return this;
    }

    /**
     * @return The P2P request configuration options
     */
    @Override
    public RequestP2PConfiguration getRequestP2PConfiguration() {
        return requestP2PConfiguration;
    }

    /**
     * @param requestP2PConfiguration
     *            The P2P request configuration options
     * @return This object
     */
    public ShutdownBuilder setRequestP2PConfiguration(final RequestP2PConfiguration requestP2PConfiguration) {
        this.requestP2PConfiguration = requestP2PConfiguration;
        return this;
    }

    /**
     * @return The future of the created channel
     */
    public FutureChannelCreator getFutureChannelCreator() {
        return futureChannelCreator;
    }

    /**
     * @param futureChannelCreator
     *            The future of the created channel
     * @return This object
     */
    public ShutdownBuilder setFutureChannelCreator(final FutureChannelCreator futureChannelCreator) {
        this.futureChannelCreator = futureChannelCreator;
        return this;
    }

    /**
     * @return The location key
     */
    @Override
    public Number160 getLocationKey() {
        return locationKey;
    }

    @Override
    public Number160 getDomainKey() {
        return null;
    }

    @Override
    public ShutdownBuilder setDomainKey(final Number160 domainKey) {
        throw new IllegalArgumentException("Cannot be set here");
    }

    /**
     * @return if message should be signed with PKI
     */
    public boolean isSignMessage() {
        return signMessage;
    }

    /**
     * @param signMessage
     *            if message should be signed with PKI
     * @return This object
     */
    public ShutdownBuilder setSignMessage(final boolean signMessage) {
        this.signMessage = signMessage;
        return this;
    }

    /**
     * 
     * @return if user performs manual cleanup
     */
    public boolean isManualCleanup() {
        return manualCleanup;
    }

    /**
     * 
     * @param manualCleanup
     *            if user performs manual cleanup
     * @return This object
     */
    public ShutdownBuilder setManualCleanup(final boolean manualCleanup) {
        this.manualCleanup = manualCleanup;
        return this;
    }
}
