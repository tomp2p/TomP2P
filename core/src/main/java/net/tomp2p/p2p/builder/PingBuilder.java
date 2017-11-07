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
import java.net.InetSocketAddress;

import net.tomp2p.connection.ClientChannel;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.DiscoverResults;
import net.tomp2p.connection.Ports;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FuturePing;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.IP.IPv4;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

public class PingBuilder {
    private static final FuturePing FUTURE_PING_SHUTDOWN = new FuturePing().failed("Peer is shutting down.");

    private final Peer peer;

    private PeerAddress peerAddress;

    private InetAddress inetAddress;

    private int port = Ports.DEFAULT_PORT;
    
    private boolean broadcast = false;

    private ConnectionConfiguration connectionConfiguration;

    public PingBuilder(Peer peer) {
        this.peer = peer;
    }
    
    public PingBuilder notifyAutomaticFutures(BaseFuture future) {
        this.peer.notifyAutomaticFutures(future);
        return this;
    }

    public PeerAddress getPeerAddress() {
        return peerAddress;
    }

    public PingBuilder peerAddress(PeerAddress peerAddress) {
        this.peerAddress = peerAddress;
        return this;
    }

    public InetAddress inetAddress() {
        return inetAddress;
    }

    public PingBuilder inetAddress(InetAddress inetAddress) {
        this.inetAddress = inetAddress;
        return this;
    }
    
    public PingBuilder inetSocketAddress(InetSocketAddress socket) {
    	this.inetAddress = socket.getAddress();
    	this.port = socket.getPort();
	    return this;
    }

    public int port() {
        return port;
    }

    public PingBuilder port(int port) {
        this.port = port;
        return this;
    }
    
    public boolean isBroadcast() {
        return broadcast;
    }

    public PingBuilder broadcast() {
        this.broadcast = true;
        return this;
    }

    public PingBuilder broadcast(boolean broadcast) {
        this.broadcast = broadcast;
        return this;
    }

    public FuturePing start() {
        if (peer.isShutdown()) {
            return FUTURE_PING_SHUTDOWN;
        }

        if (connectionConfiguration == null) {
            connectionConfiguration = new DefaultConnectionConfiguration();
        }

        if (broadcast) {
            return pingBroadcast(port);
        } else if (peerAddress != null) {
			return ping(peerAddress);
		} else if (inetAddress != null) {
			return ping(inetAddress, port, Number160.ZERO);
		} else {
           throw new IllegalArgumentException("Cannot ping. Peer address or inet address required.");
		}
    }

    

    /**
     * Pings a peer.
     * 
     * @param address
     *            The address of the remote peer.
     * @param isUDP
     *            Set to true if UDP should be used, false for TCP.
     * @return The future response
     */
    private FuturePing ping(final InetAddress address, int port, final Number160 peerId) {
    	PeerSocket4Address psa = PeerSocket4Address.builder().ipv4(IPv4.fromInet4Address(address)).udpPort(port).build();
    	PeerAddress peerAddress = PeerAddress.builder().ipv4Socket(psa).peerId(peerId).build();
        return ping(peerAddress);
    }
    
    /**
     * Pings a peer.
     * 
     * @param peerAddress
     *            The peer address of the remote peer.
     * @param isUDP
     *            Set to true if UDP should be used, false for TCP.
     * @return The future response
     */
	private FuturePing ping(PeerAddress peerAddress) {
		final FuturePing futurePing = new FuturePing();

		FutureChannelCreator fcc = peer.connectionBean().reservation().create(1);
		Utils.addReleaseListener(fcc, futurePing);
		fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(final FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					Pair<FutureDone<Message>, FutureDone<ClientChannel>> p = peer.pingRPC().pingUDP(peerAddress,
							future.channelCreator(), connectionConfiguration);
					addPingListener(futurePing, p.element0());
				} else {
					futurePing.failed(future);
				}
			}

		});

		return futurePing;
	}
    
    private FuturePing pingBroadcast(final int port) {
    	final FuturePing futurePing = new FuturePing();
        final DiscoverResults discoverResults = peer.connectionBean().channelServer().discoverNetworks().currentDiscoverResults();
        final int size = discoverResults.existingBroadcastAddresses().size();
        final FutureLateJoin<FutureDone<Message>> futureLateJoin = new FutureLateJoin<FutureDone<Message>>(size, 1);
        if (size > 0) {
            FutureChannelCreator fcc = peer.connectionBean().reservation().create(size);
            Utils.addReleaseListener(fcc, futurePing);
            fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        addPingListener(futurePing, futureLateJoin);
                        for (InetAddress broadcastAddress: discoverResults.existingBroadcastAddresses()) {
                        	
                        	PeerSocket4Address psa = PeerSocket4Address.builder().ipv4(IPv4.fromInet4Address(broadcastAddress)).udpPort(port).build();
                        	PeerAddress peerAddress = PeerAddress.builder().ipv4Socket(psa).peerId(Number160.ZERO).build();
                        	Pair<FutureDone<Message>, FutureDone<ClientChannel>> p = peer.pingRPC().pingUDP(peerAddress,
        							future.channelCreator(), connectionConfiguration);
                            if (!futureLateJoin.add(p.element0())) {
                                // the latejoin future is finished if the add returns false
                                break;
                            }
                        }
                    } else {
                    	futurePing.failed(future);
                    }
                }		
            });
        } else {
        	futurePing.failed("No broadcast address found. Cannot ping nothing");
        }
        return futurePing;
    }

    private void addPingListener(final FuturePing futurePing, final BaseFuture baseFuture) {
    	baseFuture.addListener(new BaseFutureAdapter<FutureDone<Message>>() {
        	@Override
            public void operationComplete(FutureDone<Message> future) throws Exception {
                if(future.isSuccess()) {
                	futurePing.done(future.object().sender());
                } else {
                	futurePing.failed(future);
                }
                
            }
        });
    }
}
