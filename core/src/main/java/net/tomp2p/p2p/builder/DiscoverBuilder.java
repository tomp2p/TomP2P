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
import java.util.Collection;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.DiscoverNetworks;
import net.tomp2p.connection.DiscoverResults;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.network.KCP;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerReachable;
import net.tomp2p.peers.Number256;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket6Address;
import net.tomp2p.peers.IP;

import net.tomp2p.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoverBuilder {
    final private static Logger LOG = LoggerFactory.getLogger(DiscoverBuilder.class);

    final private static FutureDiscover FUTURE_DISCOVER_SHUTDOWN = new FutureDiscover()
            .failed("Peer is shutting down.");

    final private Peer peer;

    private InetAddress inetAddress;

    private int port = 7700;

    private PeerAddress peerAddress;

    private int discoverTimeoutSec = 5;

    private ConnectionConfiguration configuration;
    
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
        this.port = port;
        return this;
    }
    
    public DiscoverBuilder peerSocketAddress(PeerSocket4Address peerSocketAddress) {
    	 this.inetAddress = peerSocketAddress.ipv4().toInet4Address();
         this.port = peerSocketAddress.udpPort();
         return this;
	}

    public int port() {
        return port;
    }

    public DiscoverBuilder port(int port) {
        this.port = port;
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

    public FutureDiscover start() {
        if (peer.isShutdown()) {
            return FUTURE_DISCOVER_SHUTDOWN;
        }

        if (peerAddress == null && inetAddress != null) {
        	if(inetAddress instanceof Inet4Address) {
        		PeerSocket4Address psa = PeerSocket4Address
        				.builder()
        				.ipv4(IP.fromInet4Address((Inet4Address)inetAddress))
        				.udpPort(port)
        				.build();
        		peerAddress = PeerAddress.builder().ipv4Socket(psa).peerId(Number256.ZERO).build();
        	} else {
        		PeerSocket6Address psa = PeerSocket6Address
        				.builder()
        				.ipv6(IP.fromInet6Address((Inet6Address)inetAddress))
        				.udpPort(port)
        				.build();
        		peerAddress = PeerAddress.builder().ipv6Socket(psa).peerId(Number256.ZERO).build();
        	}
        }
        if (peerAddress == null) {
            throw new IllegalArgumentException("Peer address or inet address required.");
        }
        if (configuration == null) {
            configuration = new DefaultConnectionConfiguration();
        }
        if (futureDiscover == null) {
        	futureDiscover = new FutureDiscover();
        }
        return discover(peerAddress, configuration, futureDiscover);
    }

    /**
     * Discover attempts to find the external IP address of this peer. This is done by first trying to set UPNP with
     * port forwarding (gives us the external address), query UPNP for the external address, and pinging a well known
     * peer. The fallback is NAT-PMP.
     * 
     * @param peerAddress
     *            The peer address. Since pings are used the peer ID can be Number256.ZERO
     * @return The future discover. This future holds also the real ID of the peer we send the discover request
     */
    private FutureDiscover discover(final PeerAddress peerAddress, final ConnectionConfiguration configuration, 
    		final FutureDiscover futureDiscover) {
        discover(futureDiscover, peerAddress, configuration);
        return futureDiscover;
    }

    /**
     * Needs 3 connections. Cleans up ChannelCreator, which means they will be released.
     * 
     * @param peerAddress
     * @return
     */
    private void discover(final FutureDiscover futureDiscover, final PeerAddress peerAddress,
            final ConnectionConfiguration configuration) {
    	LOG.debug("starting discover to {}",peerAddress);
    	final FutureDone<Void> pingDone = new FutureDone<Void>();

        peer.pingRPC().addPeerReachableListener(new PeerReachable() {
            @Override
            public void peerWellConnected(final PeerAddress peerAddress, final PeerAddress reporter, final boolean tcp) {
            	pingDone.addListener(new BaseFutureAdapter<FutureDone<Void>>() {
					@Override
					public void operationComplete(FutureDone<Void> future) throws Exception {
						futureDiscover.done(peerAddress, reporter);
					}
				});
            }
        });

        
        Pair<FutureDone<Message>, KCP> p = peer.pingRPC().pingDiscover(peerAddress);
        
         
        
        p.element0().addListener(new BaseFutureAdapter<FutureDone<Message>>() {
            @Override
            public void operationComplete(FutureDone<Message> future) throws Exception {
                PeerAddress serverAddress = peer.peerBean().serverPeerAddress();
                if (future.isSuccess() && future.object().type() == Type.NOT_FOUND) {
                	//this was a ping to myself. This is pointless
                	futureDiscover.failed("FutureDiscover to yourself", future);
                    return;
                }
                else if (future.isSuccess()) {
                	//now we know our internal address, set it as it could be a wrong one, e.g. 127.0.0.1
                	serverAddress = serverAddress.withIpv4Socket(future.object().recipient().ipv4Socket());
                	
                    /*Collection<PeerAddress> tmp = future.object().neighborsSet(0)
                            .neighbors();
                    futureDiscover.reporter(future.object().sender());
                    if (tmp.size() == 1) {
                        PeerAddress seenAs = tmp.iterator().next();
                        LOG.info("This peer is seen as {} by peer {}. This peer sees itself as {}.",
                                seenAs, peerAddress, peer.peerAddress());
                        if (!peer.peerAddress().ipv4Socket().equalsWithoutPorts(seenAs.ipv4Socket())) {
                            // check if we have this interface in that we can
                            // listen to
                            Bindings bindings2 = new Bindings().addAddress(seenAs.ipv4Socket().ipv4().toInet4Address());
                          
                            DiscoverResults discoverResults = DiscoverNetworks.discoverInterfaces(bindings2);
                            String status = discoverResults.status();
                            LOG.info("2nd interface discovery: {}", status);
                            if (discoverResults.newAddresses().size() > 0
                                    && discoverResults.newAddresses().contains(seenAs.ipv4Socket().ipv4().toInet4Address())) {
                                serverAddress = serverAddress.withIpv4Socket(seenAs.ipv4Socket());
                                peer.peerBean().serverPeerAddress(serverAddress);
                                LOG.info("This peer had the wrong interface. Changed it to {}.", serverAddress);
                            } else {
                                // now we know our internal IP, where we receive
                                // packets
                                final int port = peer.connectionBean().channelServer().channelServerConfiguration().portForwarding();
                                if (port > 0) {
                                	final PeerAddress serverAddressOrig = serverAddress;
                                	PeerSocket4Address serverSocket = serverAddress.ipv4Socket();
                                	serverSocket = serverSocket.withUdpPort(port);
                                	serverSocket = serverSocket.withIpv4(seenAs.ipv4Socket().ipv4());
                                    //manual port forwarding detected, set flag
                                    //peer.peerBean().serverPeerAddress(serverAddress.withIpv4Socket(serverSocket).withIpInternalSocket(serverAddressOrig.ipv4Socket()));
                                    LOG.info("manual ports, change it to: {}", serverAddress);
                                } else if(expectManualForwarding) {
                                	final PeerAddress serverAddressOrig = serverAddress;
                                	PeerSocket4Address serverSocket = serverAddress.ipv4Socket();
                                	serverSocket = serverSocket.withIpv4(seenAs.ipv4Socket().ipv4());
                                	//peer.peerBean().serverPeerAddress(serverAddress.withIpv4Socket(serverSocket).withIpInternalSocket(serverAddressOrig.ipv4Socket()));
                                    LOG.info("we were manually forwarding, change it to: {}", serverAddress);
                                }
                                else {
                                    // we need to find a relay, because there is a NAT in the way.
                                	// we cannot use futureResponseTCP.responseMessage().recipient() as this may return also IPv6 addresses
                                	LOG.info("We are most likely behind NAT, try to UPNP, NATPMP or relay. PeerAddress: {}, ServerAddress: {}, Seen as: {}" + peerAddress, serverAddress, seenAs);
                                    futureDiscover.externalHost("We are most likely behind NAT, try to UPNP, NATPMP or relay. Using peerAddress " + peerAddress, serverAddress.ipv4Socket(), seenAs.ipv4Socket());
                                    return;
                                }
                            }
                        }
                        // else -> we announce exactly how the other peer sees
                        // us
                        
                        Pair<FutureDone<Message>, KCP> p = peer.pingRPC().pingUDPProbe(peerAddress,
                                configuration);
                        
                        p.element0().addListener(new BaseFutureAdapter<FutureResponse>() {
							@Override
                            public void operationComplete(FutureResponse future) throws Exception {
	                            if(future.isFailed() ) {
	                            	LOG.warn("FutureDiscover (2): We need at least the UDP connection {} - {}", future, futureDiscover.failedReason());
	                            	futureDiscover.failed("FutureDiscover (2): We need at least the TCP connection", future);
	                            } else {
	                            	pingDone.done();	
	                            }
                            }
						});
                        // from here we probe, set the timeout here
                        futureDiscover.timeout(serverAddress, peer.connectionBean().timer(), discoverTimeoutSec);
                        return;
                    } else {
                        futureDiscover.failed("Peer " + peerAddress + " did not report our IP address.");
                        return;
                    }*/
                } else {
                    futureDiscover.failed("FutureDiscover (1): We need at least the TCP connection",
                            future);
                    return;
                }
            }
        });
    }
}
