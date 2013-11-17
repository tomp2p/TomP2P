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
import java.util.Collection;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.DiscoverNetworks;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerReachable;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoverBuilder {
    final private static Logger logger = LoggerFactory.getLogger(DiscoverBuilder.class);

    final private static FutureDiscover FUTURE_DISCOVER_SHUTDOWN = new FutureDiscover()
            .setFailed("Peer is shutting down");

    final private Peer peer;

    private InetAddress inetAddress;

    private int portUDP = Bindings.DEFAULT_PORT;

    private int portTCP = Bindings.DEFAULT_PORT;

    private PeerAddress peerAddress;

    private int discoverTimeoutSec = 5;

    private ConnectionConfiguration configuration;

    public DiscoverBuilder(Peer peer) {
        this.peer = peer;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public DiscoverBuilder setInetAddress(InetAddress inetAddress) {
        this.inetAddress = inetAddress;
        return this;
    }

    public int getPortUDP() {
        return portUDP;
    }

    public DiscoverBuilder setPortUDP(int portUDP) {
        this.portUDP = portUDP;
        return this;
    }

    public int getPortTCP() {
        return portTCP;
    }

    public DiscoverBuilder setPortTCP(int portTCP) {
        this.portTCP = portTCP;
        return this;
    }

    public DiscoverBuilder setPorts(int port) {
        this.portTCP = port;
        this.portUDP = port;
        return this;
    }

    public PeerAddress getPeerAddress() {
        return peerAddress;
    }

    public DiscoverBuilder setPeerAddress(PeerAddress peerAddress) {
        this.peerAddress = peerAddress;
        return this;
    }

    public int getDiscoverTimeoutSec() {
        return discoverTimeoutSec;
    }

    public DiscoverBuilder setDiscoverTimeoutSec(int discoverTimeoutSec) {
        this.discoverTimeoutSec = discoverTimeoutSec;
        return this;
    }

    public FutureDiscover start() {
        if (peer.isShutdown()) {
            return FUTURE_DISCOVER_SHUTDOWN;
        }

        if (peerAddress == null && inetAddress != null) {
            peerAddress = new PeerAddress(Number160.ZERO, inetAddress, portTCP, portUDP);
        }
        if (peerAddress == null) {
            throw new IllegalArgumentException("need peeraddress or inetaddress");
        }
        if (configuration == null) {
            configuration = new DefaultConnectionConfiguration();
        }
        return discover(peerAddress, configuration);
    }

    /**
     * Discover attempts to find the external IP address of this peer. This is done by first trying to set UPNP with
     * port forwarding (gives us the external address), query UPNP for the external address, and pinging a well known
     * peer. The fallback is NAT-PMP.
     * 
     * @param peerAddress
     *            The peer address. Since pings are used the peer ID can be Number160.ZERO
     * @return The future discover. This future holds also the real ID of the peer we send the discover request
     */
    private FutureDiscover discover(final PeerAddress peerAddress, final ConnectionConfiguration configuration) {
        final FutureDiscover futureDiscover = new FutureDiscover();
        FutureChannelCreator fcc = peer.getConnectionBean().reservation().create(1, 2);
        fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    discover(futureDiscover, peerAddress, future.getChannelCreator(), configuration);
                } else {
                    futureDiscover.setFailed(future);
                }
            }
        });
        return futureDiscover;
    }

    /**
     * Needs 3 connections. Cleans up ChannelCreator, which means they will be released.
     * 
     * @param peerAddress
     * @param cc
     * @return
     */
    private void discover(final FutureDiscover futureDiscover, final PeerAddress peerAddress,
            final ChannelCreator cc, final ConnectionConfiguration configuration) {

        peer.getHandshakeRPC().addPeerReachableListener(new PeerReachable() {
            private boolean changedUDP = false;

            private boolean changedTCP = false;

            @Override
            public void peerWellConnected(PeerAddress peerAddress, PeerAddress reporter, boolean tcp) {
                if (tcp) {
                    changedTCP = true;
                    futureDiscover.setDiscoveredTCP();
                } else {
                    changedUDP = true;
                    futureDiscover.setDiscoveredUDP();
                }
                if (changedTCP && changedUDP) {
                    futureDiscover.done(peerAddress, reporter);
                }
            }
        });

        final FutureResponse futureResponseTCP = peer.getHandshakeRPC().pingTCPDiscover(peerAddress, cc,
                configuration);

        futureResponseTCP.addListener(new BaseFutureAdapter<FutureResponse>() {
            @Override
            public void operationComplete(FutureResponse future) throws Exception {
                PeerAddress serverAddress = peer.getPeerBean().serverPeerAddress();
                if (futureResponseTCP.isSuccess()) {
                    Collection<PeerAddress> tmp = futureResponseTCP.getResponse().getNeighborsSet(0)
                            .neighbors();
                    if (tmp.size() == 1) {
                        PeerAddress seenAs = tmp.iterator().next();
                        logger.info("I'm seen as " + seenAs + " by peer " + peerAddress + " I see myself as "
                                + peer.getPeerAddress().getInetAddress());
                        if (!peer.getPeerAddress().getInetAddress().equals(seenAs.getInetAddress())) {
                            // check if we have this interface in that we can
                            // listen to
                            Bindings bindings2 = new Bindings(seenAs.getInetAddress());
                            String status = DiscoverNetworks.discoverInterfaces(bindings2);
                            logger.info("2nd interface discovery: " + status);
                            if (bindings2.getFoundAddresses().size() > 0
                                    && bindings2.getFoundAddresses().contains(seenAs.getInetAddress())) {
                                serverAddress = serverAddress.changeAddress(seenAs.getInetAddress());
                                peer.getPeerBean().serverPeerAddress(serverAddress);
                            } else {
                                // now we know our internal IP, where we receive
                                // packets
                                final Bindings bindings = peer.getConnectionBean().channelServer().bindings();
                                if (bindings.isSetExternalPortsManually()
                                        || peer.setupPortForwanding(futureResponseTCP.getResponse()
                                                .getRecipient().getInetAddress().getHostAddress())) {
                                    serverAddress = serverAddress.changePorts(bindings.getOutsideTCPPort(),
                                            bindings.getOutsideUDPPort());
                                    serverAddress = serverAddress.changeAddress(seenAs.getInetAddress());
                                    peer.getPeerBean().serverPeerAddress(serverAddress);
                                } else {
                                    // we need to find a relay, because there is a NAT in the way.
                                    futureDiscover
                                            .setFailedRelayPossible("UPNP, NATPMP could not be setup and the user did not provide any "
                                                    + "port forwarded ports for " + peerAddress);
                                    return;
                                }
                            }
                        }
                        // else -> we announce exactly how the other peer sees
                        // us
                        FutureResponse fr1 = peer.getHandshakeRPC().pingTCPProbe(peerAddress, cc,
                                configuration);
                        FutureResponse fr2 = peer.getHandshakeRPC().pingUDPProbe(peerAddress, cc,
                                configuration);
                        Utils.addReleaseListener(cc, fr1, fr2);
                        // from here we probe, set the timeout here
                        futureDiscover.setTimeout(peer.getConnectionBean().timer(), discoverTimeoutSec);
                        return;
                    } else {
                        // important to release connection if not needed
                        cc.shutdown();
                        futureDiscover.setFailed("Peer " + peerAddress + " did not report our IP address");
                        return;
                    }
                } else {
                    // important to release connection if not needed
                    cc.shutdown();
                    futureDiscover.setFailed("FutureDiscover: We need at least the TCP connection",
                            futureResponseTCP);
                    return;
                }
            }
        });
    }
}
