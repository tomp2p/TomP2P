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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.Ports;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePing;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureWrappedBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.IP;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket6Address;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

/**
 * Bootstraps to a known peer. First, channels are reserved, then discover(PeerAddress) is called to verify this Internet
 * connection settings using the "peerAddress" argument . Then the routing is initiated to the peers specified in
 * "bootstrapTo". Please be aware that in order to boostrap, you need to know the peer ID of all peers in the "bootstrapTo" collection.
 * Passing Number160.ZERO does *not* work.
 */

public class BootstrapBuilder {
    private static final Logger logger = LoggerFactory.getLogger(BootstrapBuilder.class);

    private static final FutureBootstrap FUTURE_BOOTSTRAP_SHUTDOWN = new FutureWrappedBootstrap<FutureBootstrap>()
            .failed0("Peer is shutting down.");

    private static final FutureBootstrap FUTURE_BOOTSTRAP_NO_ADDRESS = new FutureWrappedBootstrap<FutureBootstrap>()
            .failed0("No addresses to bootstrap to have been provided. Or maybe, the provided address has peer ID set to zero.");

    private final Peer peer;

    private Collection<PeerAddress> bootstrapTo;

    private PeerAddress peerAddress;

    private InetAddress inetAddress;

    private int portUDP = Ports.DEFAULT_PORT;

    private int portTCP = Ports.DEFAULT_PORT;

    private RoutingConfiguration routingConfiguration;

    private boolean forceRoutingOnlyToSelf = false;

    public BootstrapBuilder(Peer peer) {
        this.peer = peer;
    }
    
    public PeerAddress peerAddress() {
        return peerAddress;
    }

    public Collection<PeerAddress> bootstrapTo() {
        return bootstrapTo;
    }

    public BootstrapBuilder bootstrapTo(Collection<PeerAddress> bootstrapTo) {
        this.bootstrapTo = bootstrapTo;
        return this;
    }

    /**
     * Set the peer address to bootstrap to. Please note that the peer address needs to know the peerID of the bootstrap
     * peer. If this is not known, use {@link #inetAddress(InetAddress)} instead.
     * 
     * @param peerAddress
     *            The full address of the peer to bootstrap to (including the peerID of the bootstrap peer).
     * @return this instance
     */
    public BootstrapBuilder peerAddress(final PeerAddress peerAddress) {
        if (peerAddress != null && peerAddress.peerId().equals(Number160.ZERO)) {
            logger.warn("Peer address with peer ID zero provided. Bootstrapping is impossible, because no peer with peer ID set to zero is allowed to exist.");
            return this;
        }
        this.peerAddress = peerAddress;
        return this;
    }

    public InetAddress inetAddress() {
        return inetAddress;
    }

    public BootstrapBuilder inetAddress(InetAddress inetAddress) {
        this.inetAddress = inetAddress;
        return this;
    }
    
    public BootstrapBuilder inetSocketAddress(InetSocketAddress socket) {
    	this.inetAddress = socket.getAddress();
    	this.portTCP = socket.getPort();
    	this.portUDP = socket.getPort();
	    return this;
    }
    
    public BootstrapBuilder peerSocketAddress(PeerSocket4Address socket) {
    	this.inetAddress = socket.ipv4().toInet4Address();
    	this.portUDP = socket.udpPort();
	    return this;
    }

    public int portUDP() {
        return portUDP;
    }

    public BootstrapBuilder portUDP(int portUDP) {
        this.portUDP = portUDP;
        return this;
    }

    public int portTCP() {
        return portTCP;
    }

    public BootstrapBuilder portTCP(int portTCP) {
        this.portTCP = portTCP;
        return this;
    }

    public BootstrapBuilder ports(int port) {
        this.portTCP = port;
        this.portUDP = port;
        return this;
    }

    public RoutingConfiguration routingConfiguration() {
        return routingConfiguration;
    }

    public BootstrapBuilder routingConfiguration(RoutingConfiguration routingConfiguration) {
        this.routingConfiguration = routingConfiguration;
        return this;
    }

    public boolean isForceRoutingOnlyToSelf() {
        return forceRoutingOnlyToSelf;
    }

    public BootstrapBuilder forceRoutingOnlyToSelf() {
        this.forceRoutingOnlyToSelf = true;
        return this;
    }

    public BootstrapBuilder forceRoutingOnlyToSelf(boolean forceRoutingOnlyToSelf) {
        this.forceRoutingOnlyToSelf = forceRoutingOnlyToSelf;
        return this;
    }

    public FutureBootstrap start() {
        if (peer.isShutdown()) {
            return FUTURE_BOOTSTRAP_SHUTDOWN;
        }

        if (routingConfiguration == null) {
            routingConfiguration = new RoutingConfiguration(8, 10, 2);
        }
        if (peerAddress == null && inetAddress != null && bootstrapTo == null) {
        	if(inetAddress instanceof Inet4Address) {
        		PeerSocket4Address psa = PeerSocket4Address
        				.builder()
        				.ipv4(IP.fromInet4Address((Inet4Address)inetAddress))
        				.udpPort(portUDP)
        				.build();
        		PeerAddress peerAddress = PeerAddress.builder().ipv4Socket(psa).peerId(Number160.ZERO).build();
        	} else {
        		PeerSocket6Address psa = PeerSocket6Address
        				.builder()
        				.ipv6(IP.fromInet6Address((Inet6Address)inetAddress))
        				.udpPort(portUDP)
        				.build();
        		PeerAddress peerAddress = PeerAddress.builder().ipv6Socket(psa).peerId(Number160.ZERO).build();
        	}
            return bootstrapPing(peerAddress);
        } 
        if (peerAddress != null && bootstrapTo == null) {
            bootstrapTo = new ArrayList<PeerAddress>(1);
            bootstrapTo.add(peerAddress);
            return bootstrap();
        } 
        if (bootstrapTo != null) {
            return bootstrap();
        }
        return FUTURE_BOOTSTRAP_NO_ADDRESS;
    }

    private FutureBootstrap bootstrap() {
        final FutureWrappedBootstrap<FutureDone<Pair<FutureRouting,FutureRouting>>> result = new FutureWrappedBootstrap<FutureDone<Pair<FutureRouting,FutureRouting>>>();
        result.bootstrapTo(bootstrapTo);
        int conn = routingConfiguration.parallel();
        FutureChannelCreator fcc = peer.connectionBean().reservation().create(conn);
        Utils.addReleaseListener(fcc, result);
        fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator futureChannelCreator) throws Exception {
                if (futureChannelCreator.isSuccess()) {
                    RoutingBuilder routingBuilder = createBuilder(routingConfiguration, forceRoutingOnlyToSelf);
                    FutureDone<Pair<FutureRouting,FutureRouting>> futureBootstrap = peer.distributedRouting().bootstrap(
                            bootstrapTo, routingBuilder, futureChannelCreator.channelCreator());
                    result.waitFor(futureBootstrap);
                } else {
                    result.failed(futureChannelCreator);
                }
            }
        });
        return result;
    }

    static RoutingBuilder createBuilder(RoutingConfiguration routingConfiguration, boolean forceRoutingOnlyToSelf) {
        RoutingBuilder routingBuilder = new RoutingBuilder();
        routingBuilder.parallel(routingConfiguration.parallel());
        routingBuilder.setMaxNoNewInfo(routingConfiguration.maxNoNewInfoDiff());
        routingBuilder.maxDirectHits(Integer.MAX_VALUE);
        routingBuilder.maxFailures(routingConfiguration.maxFailures());
        routingBuilder.maxSuccess(routingConfiguration.maxSuccess());
        routingBuilder.forceRoutingOnlyToSelf(forceRoutingOnlyToSelf);
        return routingBuilder;
    }

    private FutureWrappedBootstrap<FutureBootstrap> bootstrapPing(PeerAddress address) {
        final FutureWrappedBootstrap<FutureBootstrap> result = new FutureWrappedBootstrap<FutureBootstrap>();
        final FuturePing futurePing = peer.ping().peerAddress(address).start();
        futurePing.addListener(new BaseFutureAdapter<FuturePing>() {
            @Override
            public void operationComplete(final FuturePing future) throws Exception {
                if (future.isSuccess()) {
                    peerAddress = future.remotePeer();
                    bootstrapTo = new ArrayList<PeerAddress>(1);
                    bootstrapTo.add(peerAddress);
                    result.bootstrapTo(bootstrapTo);
                    result.waitFor(bootstrap());
                } else {
                    result.failed("Could not reach anyone with bootstrap");
                }
            }
        });
        return result;
    }
}
