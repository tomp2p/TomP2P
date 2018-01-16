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

import net.sctp4nat.core.SctpChannelFacade;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.Ports;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePing;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.IP;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket6Address;
import net.tomp2p.utils.Pair;

public class PingBuilder {
    private static final FuturePing FUTURE_PING_SHUTDOWN = new FuturePing().failed("Peer is shutting down.");

    private final Peer peer;

    private PeerAddress peerAddress;

    private InetAddress inetAddress;

    private int port = Ports.DEFAULT_PORT;

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

    public FuturePing start() {
        if (peer.isShutdown()) {
            return FUTURE_PING_SHUTDOWN;
        }

        if (connectionConfiguration == null) {
            connectionConfiguration = new DefaultConnectionConfiguration();
        }

        if (peerAddress != null) {
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
    	if(address instanceof Inet4Address) {
    		PeerSocket4Address psa = PeerSocket4Address
    				.builder()
    				.ipv4(IP.fromInet4Address((Inet4Address)address))
    				.udpPort(port)
    				.build();
    		PeerAddress peerAddress = PeerAddress.builder().ipv4Socket(psa).peerId(peerId).build();
    	} else {
    		PeerSocket6Address psa = PeerSocket6Address
    				.builder()
    				.ipv6(IP.fromInet6Address((Inet6Address)address))
    				.udpPort(port)
    				.build();
    		PeerAddress peerAddress = PeerAddress.builder().ipv6Socket(psa).peerId(peerId).build();
    	}
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

		Pair<FutureDone<Message>, FutureDone<SctpChannelFacade>> p = peer.pingRPC().pingUDP(peerAddress);
					addPingListener(futurePing, p.element0());
				

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
