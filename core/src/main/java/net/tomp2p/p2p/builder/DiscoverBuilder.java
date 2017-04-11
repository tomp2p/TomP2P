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
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.DiscoverNetworks;
import net.tomp2p.connection.DiscoverResults;
import net.tomp2p.connection.Ports;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerReachable;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.peers.IP.IPv4;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoverBuilder extends DefaultConnectionConfiguration {
    final private static Logger LOG = LoggerFactory.getLogger(DiscoverBuilder.class);

    final private static FutureDiscover FUTURE_DISCOVER_SHUTDOWN = new FutureDiscover()
            .failed("Peer is shutting down.");

    final private Peer peer;

    private InetAddress inetAddress;

    private int portUDP = Ports.DEFAULT_PORT;
    private int portTCP = Ports.DEFAULT_PORT;
    //private int portUDT = Ports.DEFAULT_PORT + 1;

    private PeerAddress peerAddress;

    private int discoverTimeoutSec = 5;

    private FutureDiscover futureDiscover;

    private boolean expectManualForwarding;

    public DiscoverBuilder(Peer peer) {
        this.peer = peer;
    }

    public InetAddress inetAddress() {
        return inetAddress;
    }

    public DiscoverBuilder inetAddress(InetAddress inetAddress) {
        this.inetAddress = inetAddress;
        return this;
    }
    
    public DiscoverBuilder inetSocketAddress(InetAddress inetAddress, int port) {
        this.inetAddress = inetAddress;
        this.portTCP = port;
        this.portUDP = port;
        return this;
    }
    
    public DiscoverBuilder inetSocketAddress(InetAddress inetAddress, int portTCP, int portUDP) {
        this.inetAddress = inetAddress;
        this.portTCP = portTCP;
        this.portUDP = portUDP;
        return this;
    }
    
    public DiscoverBuilder peerSocketAddress(PeerSocket4Address peerSocketAddress) {
    	 this.inetAddress = peerSocketAddress.ipv4().toInetAddress();
         this.portTCP = peerSocketAddress.tcpPort();
         this.portUDP = peerSocketAddress.udpPort();
         return this;
	}

    public int portUDP() {
        return portUDP;
    }

    public DiscoverBuilder portUDP(int portUDP) {
        this.portUDP = portUDP;
        return this;
    }

    public int portTCP() {
        return portTCP;
    }

    public DiscoverBuilder portTCP(int portTCP) {
        this.portTCP = portTCP;
        return this;
    }

    public DiscoverBuilder ports(int port) {
        this.portTCP = port;
        this.portUDP = port;
        return this;
    }

    public PeerAddress peerAddress() {
        return peerAddress;
    }

    public DiscoverBuilder peerAddress(PeerAddress peerAddress) {
        this.peerAddress = peerAddress;
        return this;
    }

    public int discoverTimeoutSec() {
        return discoverTimeoutSec;
    }

    public DiscoverBuilder discoverTimeoutSec(int discoverTimeoutSec) {
        this.discoverTimeoutSec = discoverTimeoutSec;
        return this;
    }
    
    public FutureDiscover futureDiscover() {
        return futureDiscover;
    }

    public DiscoverBuilder futureDiscover(FutureDiscover futureDiscover) {
        this.futureDiscover = futureDiscover;
        return this;
    }
    
    public boolean isExpectManualForwarding() {
        return expectManualForwarding;
    }

    public DiscoverBuilder expectManualForwarding() {
        return setExpectManualForwarding(true);
    }

    public DiscoverBuilder setExpectManualForwarding(boolean expectManualForwarding) {
        this.expectManualForwarding = expectManualForwarding;
        return this;
    }

    /**
     * @return The future discover. This future holds also the real ID of the peer we send the discover request
     */
    public FutureDiscover start() {
        if (peer.isShutdown()) {
            return FUTURE_DISCOVER_SHUTDOWN;
        }
        if (forceTCP && forceUDP) {
            throw new IllegalArgumentException("Only one of 'forceTCP' or 'forceUDP' can be chosen.");
        }
        if (peerAddress == null && inetAddress != null) {
        	PeerSocket4Address psa = PeerSocket4Address.builder().ipv4(IPv4.fromInet4Address(inetAddress)).tcpPort(portTCP).udpPort(portUDP).build();
        	peerAddress = PeerAddress.builder().ipv4Socket(psa).peerId(Number160.ZERO).build();
        }
        if (peerAddress == null) {
            throw new IllegalArgumentException("Peer address or inet address required.");
        }
        if (futureDiscover == null) {
        	futureDiscover = new FutureDiscover();
        }
        return discover(peerAddress);
    }

    /**
     * Discover attempts to find the external IP address of this peer. This is done by first trying to set UPNP with
     * port forwarding (gives us the external address), query UPNP for the external address, and pinging a well known
     * peer. The fallback is NAT-PMP.
     * 
     * @param peerAddress
     *            The peer address. Since pings are used the peer ID can be Number160.ZERO
     * @return The future discover.
     */
    private FutureDiscover discover(final PeerAddress peerAddress) {
        FutureChannelCreator fcc;
        if (forceUDP) {
            //Only reserve udp channels
            fcc = peer.connectionBean().reservation().create(2, 0);
        } else if (forceTCP) {
            //Only reserve tcp channels
            fcc = peer.connectionBean().reservation().create(0, 2);
        } else {
            fcc = peer.connectionBean().reservation().create(1, 2);
        }
        Utils.addReleaseListener(fcc, futureDiscover);
        fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    pingDiscover(peerAddress, future.channelCreator());
                } else {
                    futureDiscover.failed(future);
                }
            }
        });
        return futureDiscover;
    }

    /**
     * Starts the discovery process.
     *
     * First launch a pingDiscover to setup our addresses then probe our reachability.
     *
     * Cleans up ChannelCreator, which means they will be released.
     * 
     * @param peerAddress
     * @param cc
     * @return
     */
    private void pingDiscover(final PeerAddress peerAddress, final ChannelCreator cc) {
        LOG.debug("starting discover to {}", peerAddress);

        final PeerReachable reachableListener = new PeerReachable() {

            @Override
            public void peerWellConnected(final PeerAddress peerAddress, final PeerAddress reporter,
                    final boolean tcp) {
                LOG.info("peerWellConnected.");
                if (tcp) {
                    futureDiscover.discoveredTCP();
                    LOG.debug("TCP discovered");
                } else {
                    futureDiscover.discoveredUDP();
                    LOG.debug("UDP discovered");
                }
                if (futureDiscover.isDiscoveredTCP() && forceTCP) {
                    //If we forced TCP, assume we only wait for the TCP response to finish
                    futureDiscover.done(peerAddress, reporter);
                } else if (futureDiscover.isDiscoveredUDP() && forceUDP) {
                    //If we forced UDP, assume we only wait for the UDP response to finish
                    futureDiscover.done(peerAddress, reporter);
                } else if (futureDiscover.isDiscoveredTCP() && futureDiscover.isDiscoveredUDP()) {
                    futureDiscover.done(peerAddress, reporter);
                }
            }
        };
        peer.pingRPC().addPeerReachableListener(reachableListener);
        //Shoudn't the Reachable listener be removed once done() ? 

        final FutureResponse futureResponse;
        if (forceUDP) {
            futureResponse = peer.pingRPC().pingUDPDiscover(peerAddress, cc, this);
        } else {
            //ping discover by default with tcp
            futureResponse = peer.pingRPC().pingTCPDiscover(peerAddress, cc, this);
        }

        futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
            @Override
            public void operationComplete(FutureResponse future) throws Exception {
                PeerAddress serverAddress = peer.peerBean().serverPeerAddress();
                if (futureResponse.isSuccess() && futureResponse.responseMessage().type() == Type.NOT_FOUND) {
                	//this was a ping to myself. This is pointless
                	futureDiscover.failed("FutureDiscover to yourself", futureResponse);
                } else if (futureResponse.isSuccess()) {
                    Collection<PeerAddress> tmp = futureResponse.responseMessage().neighborsSet(0)
                            .neighbors();
                    futureDiscover.reporter(futureResponse.responseMessage().sender());
                    if (tmp.size() == 1) {
                        PeerAddress seenAs = tmp.iterator().next();
                        LOG.info("This peer is seen as {} by peer {}. This peer sees itself as {}.",
                                seenAs, peerAddress, peer.peerAddress());
                        if (!peer.peerAddress().ipv4Socket().equalsWithoutPorts(seenAs.ipv4Socket())) {
                            // check if we have this interface in that we can listen to
                            Bindings bindings2 = new Bindings().addAddress(seenAs.ipv4Socket().ipv4().toInetAddress());

                            DiscoverResults discoverResults = DiscoverNetworks.discoverInterfaces(bindings2);
                            String status = discoverResults.status();
                            LOG.info("2nd interface discovery: {}", status);
                            if (discoverResults.newAddresses().size() > 0
                                    && discoverResults.newAddresses().contains(seenAs.ipv4Socket().ipv4().toInetAddress())) {
                                serverAddress = serverAddress.withIpv4Socket(seenAs.ipv4Socket());
                                peer.peerBean().serverPeerAddress(serverAddress);
                                LOG.info("This peer had the wrong interface. Changed it to {}.", serverAddress);
                            } else {
                                // now we know our internal IP, where we receive packets
                                final Ports ports = peer.connectionBean().channelServer().channelServerConfiguration().portsForwarding();
                                if (ports.isManualPort()) {
                                	final PeerAddress serverAddressOrig = serverAddress;
                                    PeerSocket4Address serverSocket = serverAddress.ipv4Socket();
                                	serverSocket = serverSocket.withTcpPort(ports.tcpPort()).withUdpPort(ports.udpPort());
                                	serverSocket = serverSocket.withIpv4(seenAs.ipv4Socket().ipv4());
                                    //manual port forwarding detected, set flag
                                    peer.peerBean().serverPeerAddress(serverAddress.withIpv4Socket(serverSocket).
                                            withNet4Internal(true).withIpInternalSocket(serverAddressOrig.ipv4Socket()));
                                    LOG.info("manual ports, change it to: {}", serverAddress);
                                } else if(expectManualForwarding) {
                                	final PeerAddress serverAddressOrig = serverAddress;
                                	PeerSocket4Address serverSocket = serverAddress.ipv4Socket();
                                	serverSocket = serverSocket.withIpv4(seenAs.ipv4Socket().ipv4());
                                    peer.peerBean().serverPeerAddress(serverAddress.withIpv4Socket(serverSocket)
                                            .withNet4Internal(true).withIpInternalSocket(serverAddressOrig.ipv4Socket()));
                                    LOG.info("we were manually forwarding, change it to: {}", serverAddress);
                                } else {
                                    // we need to find a relay, because there is a NAT in the way.
                                	// we cannot use futureResponseTCP.responseMessage().recipient() as this may return also IPv6 addresses
                                	LOG.info("We are most likely behind NAT, try to UPNP, NATPMP or relay. PeerAddress: {}, ServerAddress: {}, Seen as: {}" + peerAddress, serverAddress, seenAs);
                                    futureDiscover.externalHost("We are most likely behind NAT, try to UPNP, NATPMP or relay. Using peerAddress " + peerAddress, serverAddress.ipv4Socket(), seenAs.ipv4Socket());
                                }
                            }
                        }
                        //Ask distant peer to reach us now that our internal and external addresses are (hopefully) correct
                        pingProbe(cc);
                        // from here we probe, set the timeout
                        futureDiscover.timeout(serverAddress, peer.connectionBean().timer(), discoverTimeoutSec);
                    } else {
                        futureDiscover.failed("Peer " + peerAddress + " did not report our IP address.");
                    }
                } else {
                    futureDiscover.failed("FutureDiscover (1): we need at least the first pingDiscover", futureResponse);
                }
            }
        });
    }

    /**
     * Once the pingDiscover succeeded, probe our reachability with either TCP, UDP or both.
     *
     * @param cc
     */
    private void pingProbe(final ChannelCreator cc) {
        if (forceUDP) {
            //Only probe with UDP and finish discovery on success
            FutureResponse frUdp = peer.pingRPC().pingUDPProbe(peerAddress, cc, DiscoverBuilder.this);
            frUdp.addListener(new BaseFutureAdapter<FutureResponse>() {
                @Override
                public void operationComplete(FutureResponse future) throws Exception {
                    if (future.isFailed()) {
                        LOG.warn("FutureDiscover (2): We need the UDP connection with forceUDP {} - {}",
                                future, futureDiscover.failedReason());
                        futureDiscover.failed("FutureDiscover (2): We need the UDP connection with forceUDP", future);
                    }
                }
            });
        } else if (forceTCP) {
            //Only probe with TCP and finish discovery on success
            FutureResponse frTcp = peer.pingRPC().pingTCPProbe(peerAddress, cc, DiscoverBuilder.this);
            frTcp.addListener(new BaseFutureAdapter<FutureResponse>() {
                @Override
                public void operationComplete(FutureResponse future) throws Exception {
                    if (future.isFailed()) {
                        LOG.warn("FutureDiscover (2): We need the TCP connection with forceTCP {} - {}",
                                future, futureDiscover.failedReason());
                        futureDiscover.failed("FutureDiscover (2): We need the TCP connection with forceTCP", future);
                    }
                }
            });
        } else {
            //both TCP and UDP. Finish discovery after both calls returned, ignoring UDP failure
            FutureResponse fr1 = peer.pingRPC().pingTCPProbe(peerAddress, cc, DiscoverBuilder.this);
            fr1.addListener(new BaseFutureAdapter<FutureResponse>() {
                @Override
                public void operationComplete(FutureResponse future) throws Exception {
                    if (future.isFailed()) {
                        LOG.warn("FutureDiscover (2): We need at least the TCP connection {} - {}",
                                future, futureDiscover.failedReason());
                        futureDiscover.failed("FutureDiscover (2): We need at least the TCP connection", future);
                    }
                }
            });
            FutureResponse fr2 = peer.pingRPC().pingUDPProbe(peerAddress, cc, DiscoverBuilder.this);
            fr2.addListener(new BaseFutureAdapter<FutureResponse>() {
                @Override
                public void operationComplete(FutureResponse future) throws Exception {
                    if (future.isFailed()) {
                        LOG.warn("FutureDiscover (2): UDP failed connection {} - {}",
                                future, futureDiscover.failedReason());
                    }
                }
            });
        }
    }

}
