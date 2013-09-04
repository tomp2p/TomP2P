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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.connection2.Bindings;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureWrappedBootstrap;
import net.tomp2p.futures.FutureWrapper;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Boostraps to a known peer. First channels are reserved, then #discover(PeerAddress) is called to verify this Internet
 * connection settings using the argument peerAddress. Then the routing is initiated to the peers specified in
 * bootstrapTo. Please be aware that in order to boostrap you need to know the peer ID of all peers in the collection
 * bootstrapTo. Passing Number160.ZERO does *not* work.
 * 
 * @param discoveryPeerAddress
 *            The peer address to use for discovery
 * @param bootstrapTo
 *            The peers used to bootstrap
 * @param config
 *            The configuration
 * @return The future bootstrap
 */

public class BootstrapBuilder {
    private static final Logger logger = LoggerFactory.getLogger(BootstrapBuilder.class);

    private static final FutureBootstrap FUTURE_BOOTSTRAP_SHUTDOWN = new FutureWrappedBootstrap<FutureBootstrap>()
            .setFailed("Peer is shutting down");

    private static final FutureBootstrap FUTURE_BOOTSTRAP_NO_ADDRESS = new FutureWrappedBootstrap<FutureBootstrap>()
            .setFailed("You did not provide information where to bootstrap to. "
                    + "This could be also caused if you provided a peer address with a peer ID set to zero.");

    private final Peer peer;

    private Collection<PeerAddress> bootstrapTo;

    private PeerAddress peerAddress;

    private InetAddress inetAddress;

    private int portUDP = Bindings.DEFAULT_PORT;

    private int portTCP = Bindings.DEFAULT_PORT;

    private RoutingConfiguration routingConfiguration;

    private RequestP2PConfiguration requestP2PConfiguration;

    private boolean forceRoutingOnlyToSelf = false;

    private boolean broadcast = false;

    public BootstrapBuilder(Peer peer) {
        this.peer = peer;
    }

    public Collection<PeerAddress> getBootstrapTo() {
        return bootstrapTo;
    }

    public BootstrapBuilder setBootstrapTo(Collection<PeerAddress> bootstrapTo) {
        this.bootstrapTo = bootstrapTo;
        return this;
    }

    public PeerAddress getPeerAddress() {
        return peerAddress;
    }

    /**
     * Set the peer address to bootstrap to. Please note that the peer address needs to know the peerID of the bootstrap
     * peer. If this is not known, use {@link #setInetAddress(InetAddress)} instead.
     * 
     * @param peerAddress
     *            The full address of the peer to bootstrap to (including the peerID of the bootstrap peer).
     * @return this instance
     */
    public BootstrapBuilder setPeerAddress(final PeerAddress peerAddress) {
        if (peerAddress != null && peerAddress.getPeerId().equals(Number160.ZERO)) {
            logger.warn("You provided a peer address with peerID zero. "
                    + "You won't be able to bootstrap since no peer can have a peerID set to zero");
            return this;
        }
        this.peerAddress = peerAddress;
        return this;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public BootstrapBuilder setInetAddress(InetAddress inetAddress) {
        this.inetAddress = inetAddress;
        return this;
    }

    public int getPortUDP() {
        return portUDP;
    }

    public BootstrapBuilder setPortUDP(int portUDP) {
        this.portUDP = portUDP;
        return this;
    }

    public int getPortTCP() {
        return portTCP;
    }

    public BootstrapBuilder setPortTCP(int portTCP) {
        this.portTCP = portTCP;
        return this;
    }

    public BootstrapBuilder setPorts(int port) {
        this.portTCP = port;
        this.portUDP = port;
        return this;
    }

    public RoutingConfiguration getRoutingConfiguration() {
        return routingConfiguration;
    }

    public BootstrapBuilder setRoutingConfiguration(RoutingConfiguration routingConfiguration) {
        this.routingConfiguration = routingConfiguration;
        return this;
    }

    public RequestP2PConfiguration getRequestP2PConfiguration() {
        return requestP2PConfiguration;
    }

    public BootstrapBuilder setRequestP2PConfiguration(RequestP2PConfiguration requestP2PConfiguration) {
        this.requestP2PConfiguration = requestP2PConfiguration;
        return this;
    }

    public boolean isForceRoutingOnlyToSelf() {
        return forceRoutingOnlyToSelf;
    }

    public BootstrapBuilder setForceRoutingOnlyToSelf() {
        this.forceRoutingOnlyToSelf = true;
        return this;
    }

    public BootstrapBuilder setForceRoutingOnlyToSelf(boolean forceRoutingOnlyToSelf) {
        this.forceRoutingOnlyToSelf = forceRoutingOnlyToSelf;
        return this;
    }

    public boolean isBroadcast() {
        return broadcast;
    }

    public BootstrapBuilder setBroadcast() {
        this.broadcast = true;
        return this;
    }

    public BootstrapBuilder setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
        return this;
    }

    public FutureBootstrap start() {
        if (peer.isShutdown()) {
            return FUTURE_BOOTSTRAP_SHUTDOWN;
        }

        if (routingConfiguration == null) {
            routingConfiguration = new RoutingConfiguration(5, 10, 2);
        }
        if (requestP2PConfiguration == null) {
            int size = peer.getPeerBean().peerMap().size() + 1;
            requestP2PConfiguration = new RequestP2PConfiguration(Math.min(size, 3), 5, 3);
        }
        //
        if (broadcast) {
            return broadcast();
        }
        if (peerAddress == null && inetAddress != null && bootstrapTo == null) {
            peerAddress = new PeerAddress(Number160.ZERO, inetAddress, portTCP, portUDP);
            return bootstrapPing(peerAddress);

        } else if (peerAddress != null && bootstrapTo == null) {
            bootstrapTo = new ArrayList<PeerAddress>(1);
            bootstrapTo.add(peerAddress);
            return bootstrap();
        } else if (bootstrapTo != null) {
            return bootstrap();
        } else {
            return FUTURE_BOOTSTRAP_NO_ADDRESS;
        }
    }

    private FutureBootstrap bootstrap() {
        final FutureWrappedBootstrap<FutureWrapper<FutureRouting>> result = new FutureWrappedBootstrap<FutureWrapper<FutureRouting>>();
        result.setBootstrapTo(bootstrapTo);
        int conn = Math.max(routingConfiguration.getParallel(), requestP2PConfiguration.getParallel());
        FutureChannelCreator fcc = peer.getConnectionBean().reservation().create(conn, 0);

        fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator futureChannelCreator) throws Exception {
                if (futureChannelCreator.isSuccess()) {
                    RoutingBuilder routingBuilder = createBuilder(requestP2PConfiguration,
                            routingConfiguration);
                    FutureWrapper<FutureRouting> futureBootstrap = peer.getDistributedRouting().bootstrap(
                            bootstrapTo, routingBuilder, futureChannelCreator.getChannelCreator());
                    Utils.addReleaseListener(futureChannelCreator.getChannelCreator(), futureBootstrap);
                    result.waitFor(futureBootstrap);
                } else {
                    result.setFailed(futureChannelCreator);
                }
            }
        });
        return result;
    }

    private RoutingBuilder createBuilder(RequestP2PConfiguration requestP2PConfiguration,
            RoutingConfiguration routingConfiguration) {
        RoutingBuilder routingBuilder = new RoutingBuilder();
        routingBuilder.setParallel(routingConfiguration.getParallel());
        routingBuilder.setMaxNoNewInfo(routingConfiguration.getMaxNoNewInfo(requestP2PConfiguration
                .getMinimumResults()));
        routingBuilder.setMaxDirectHits(Integer.MAX_VALUE);
        routingBuilder.setMaxFailures(routingConfiguration.getMaxFailures());
        routingBuilder.setMaxSuccess(routingConfiguration.getMaxSuccess());
        routingBuilder.setForceRoutingOnlyToSelf(forceRoutingOnlyToSelf);
        return routingBuilder;
    }

    private FutureWrappedBootstrap<FutureBootstrap> bootstrapPing(PeerAddress address) {
        final FutureWrappedBootstrap<FutureBootstrap> result = new FutureWrappedBootstrap<FutureBootstrap>();
        final FutureResponse tmp = (FutureResponse) peer.ping().setPeerAddress(address).setTcpPing().start();
        tmp.addListener(new BaseFutureAdapter<FutureResponse>() {
            @Override
            public void operationComplete(final FutureResponse future) throws Exception {
                if (future.isSuccess()) {
                    peerAddress = future.getResponse().getSender();
                    bootstrapTo = new ArrayList<PeerAddress>(1);
                    bootstrapTo.add(peerAddress);
                    result.setBootstrapTo(bootstrapTo);
                    result.waitFor(bootstrap());
                } else {
                    result.setFailed("could not reach anyone with bootstrap");
                }
            }
        });
        return result;
    }

    private FutureWrappedBootstrap<FutureBootstrap> broadcast() {
        final FutureWrappedBootstrap<FutureBootstrap> result = new FutureWrappedBootstrap<FutureBootstrap>();
        // limit after
        @SuppressWarnings("unchecked")
        final FutureLateJoin<FutureResponse> tmp = (FutureLateJoin<FutureResponse>) peer.ping()
                .setBroadcast().setPort(portUDP).start();
        tmp.addListener(new BaseFutureAdapter<FutureLateJoin<FutureResponse>>() {
            @Override
            public void operationComplete(final FutureLateJoin<FutureResponse> future) throws Exception {
                if (future.isSuccess()) {
                    FutureResponse futureResponse = future.getLastSuceessFuture();
                    if (futureResponse == null) {
                        result.setFailed("no futures found", future);
                        return;
                    }
                    if (bootstrapTo != null && bootstrapTo.size() > 0) {
                        logger.info("you added peers to bootstrapTo. However with broadcast we found our own peers.");
                    }
                    peerAddress = futureResponse.getResponse().getSender();
                    bootstrapTo = new ArrayList<PeerAddress>(1);
                    bootstrapTo.add(peerAddress);
                    result.setBootstrapTo(bootstrapTo);
                    result.waitFor(bootstrap());
                } else {
                    result.setFailed("could not reach anyone with the broadcast", future);
                }
            }
        });
        return result;
    }

}
