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

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Ports;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

public class PingBuilder {
    private static final BaseFuture FUTURE_PING_SHUTDOWN = new FutureDone<Void>().failed("Peer is shutting down");

    private final Peer peer;

    private PeerAddress peerAddress;

    private InetAddress inetAddress;

    private int port = Ports.DEFAULT_PORT;

    private boolean broadcast = false;

    private boolean tcpPing = false;
    
    private PeerConnection peerConnection;

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

    public boolean isTcpPing() {
        return tcpPing;
    }

    public PingBuilder tcpPing() {
        this.tcpPing = true;
        return this;
    }

    public PingBuilder tcpPing(boolean tcpPing) {
        this.tcpPing = tcpPing;
        return this;
    }
    
    public PingBuilder peerConnection(PeerConnection peerConnection) {
        this.peerConnection = peerConnection;
        return this;
    }
    
    public PeerConnection peerConnection() {
        return peerConnection;
    }

    public BaseFuture start() {
        if (peer.isShutdown()) {
            return FUTURE_PING_SHUTDOWN;
        }

        if (connectionConfiguration == null) {
            connectionConfiguration = new DefaultConnectionConfiguration();
        }

        if (broadcast) {
            return pingBroadcast(port);
        } else {
            if (peerAddress != null) {
                if (tcpPing) {
                	return ping(peerAddress, false);
                } else {
                	return ping(peerAddress, true);
                }
            } else if (inetAddress != null) {
                if (tcpPing) {
                    return ping(new InetSocketAddress(inetAddress, port), Number160.ZERO, false);
                } else {
                    return ping(new InetSocketAddress(inetAddress, port), Number160.ZERO, true);
                }
            } else if (peerConnection != null) {
                return pingPeerConnection(peerConnection);
            } else {
                throw new IllegalArgumentException("cannot ping, need to know peer address or inet address");
            } 
        }
    }

    FutureLateJoin<FutureResponse> pingBroadcast(final int port) {
        final Bindings bindings = peer.connectionBean().sender().channelClientConfiguration().externalBindings();
        final int size = bindings.broadcastAddresses().size();
        final FutureLateJoin<FutureResponse> futureLateJoin = new FutureLateJoin<FutureResponse>(size, 1);
        if (size > 0) {

            FutureChannelCreator fcc = peer.connectionBean().reservation().create(size, 0);

            fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        Utils.addReleaseListener(future.channelCreator(), futureLateJoin);
                        for (int i = 0; i < size; i++) {
                            final InetAddress broadcastAddress = bindings.broadcastAddresses().get(i);
                            final PeerAddress peerAddress = new PeerAddress(Number160.ZERO, broadcastAddress,
                                    port, port);
                            FutureResponse validBroadcast = peer.pingRPC().pingBroadcastUDP(
                                    peerAddress, future.channelCreator(), connectionConfiguration);
                            if (!futureLateJoin.add(validBroadcast)) {
                                // the latejoin future is fininshed if the add returns false
                                break;
                            }
                        }
                    } else {
                        futureLateJoin.failed(future);
                    }
                }
            });
        } else {
            futureLateJoin.failed("No broadcast address found. Cannot ping nothing");
        }
        return futureLateJoin;
    }

    /**
     * Pings a peer. Default is to use UDP
     * 
     * @param address
     *            The address of the remote peer.
     * @return The future response
     */
    public FutureResponse ping(final InetSocketAddress address) {
        return ping(address, Number160.ZERO, true);
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
    public FutureResponse ping(final InetSocketAddress address, final Number160 peerId, final boolean isUDP) {
        return ping(new PeerAddress(peerId, address), isUDP);
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
    public FutureResponse ping(PeerAddress peerAddress, final boolean isUDP) {
        final RequestHandler<FutureResponse> request = peer.pingRPC().ping(peerAddress, connectionConfiguration);
        if (isUDP) {
            FutureChannelCreator fcc = peer.connectionBean().reservation().create(1, 0);
            fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(final FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        FutureResponse futureResponse = request.sendUDP(future.channelCreator());
                        Utils.addReleaseListener(future.channelCreator(), futureResponse);
                    } else {
                        request.futureResponse().failed(future);
                    }
                }
            });
        } else {
            FutureChannelCreator fcc = peer.connectionBean().reservation().create(0, 1);
            fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(final FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        FutureResponse futureResponse = request.sendTCP(future.channelCreator());
                        Utils.addReleaseListener(future.channelCreator(), futureResponse);
                    } else {
                        request.futureResponse().failed(future);
                    }
                }
            });
        }
        return request.futureResponse();
    }
    
    public FutureResponse pingPeerConnection(final PeerConnection peerConnection) {
        final RequestHandler<FutureResponse> request = peer.pingRPC().ping(
                peerConnection.remotePeer(), connectionConfiguration);
        FutureChannelCreator futureChannelCreator = peerConnection.acquire(request.futureResponse());
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {

            @Override
            public void operationComplete(FutureChannelCreator future) throws Exception {
                if(future.isSuccess()) {
                    request.futureResponse().request().keepAlive(true);
                    request.sendTCP(peerConnection);
                } else {
                    request.futureResponse().failed(future);
                }
            }
        });
        
        return request.futureResponse();
    }
}
