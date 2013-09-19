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

import net.tomp2p.connection2.Bindings;
import net.tomp2p.connection2.ConnectionConfiguration;
import net.tomp2p.connection2.DefaultConnectionConfiguration;
import net.tomp2p.connection2.RequestHandler;
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
    private static final BaseFuture FUTURE_PING_SHUTDOWN = new FutureDone<Void>().setFailed("Peer is shutting down");

    private final Peer peer;

    private PeerAddress peerAddress;

    private InetAddress inetAddress;

    private int port = Bindings.DEFAULT_PORT;

    private boolean broadcast = false;

    private boolean tcpPing = false;

    private ConnectionConfiguration connectionConfiguration;

    public PingBuilder(Peer peer) {
        this.peer = peer;
    }

    public PeerAddress getPeerAddress() {
        return peerAddress;
    }

    public PingBuilder setPeerAddress(PeerAddress peerAddress) {
        this.peerAddress = peerAddress;
        return this;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public PingBuilder setInetAddress(InetAddress inetAddress) {
        this.inetAddress = inetAddress;
        return this;
    }

    public int getPort() {
        return port;
    }

    public PingBuilder setPort(int port) {
        this.port = port;
        return this;
    }

    public boolean isBroadcast() {
        return broadcast;
    }

    public PingBuilder setBroadcast() {
        this.broadcast = true;
        return this;
    }

    public PingBuilder setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
        return this;
    }

    public boolean isTcpPing() {
        return tcpPing;
    }

    public PingBuilder setTcpPing() {
        this.tcpPing = true;
        return this;
    }

    public PingBuilder setTcpPing(boolean tcpPing) {
        this.tcpPing = tcpPing;
        return this;
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
                    return ping(peerAddress.createSocketTCP(), true);
                } else {
                    return ping(peerAddress.createSocketUDP(), false);
                }
            } else if (inetAddress != null) {
                if (tcpPing) {
                    return ping(new InetSocketAddress(inetAddress, port), true);
                } else {
                    return ping(new InetSocketAddress(inetAddress, port), false);
                }
            } else {
                throw new IllegalArgumentException("cannot ping, need to know peer address or inet address");
            }
        }
    }

    FutureLateJoin<FutureResponse> pingBroadcast(final int port) {
        final Bindings bindings = peer.getConnectionBean().channelServer().bindings();
        final int size = bindings.getBroadcastAddresses().size();
        final FutureLateJoin<FutureResponse> futureLateJoin = new FutureLateJoin<FutureResponse>(size, 1);
        if (size > 0) {

            FutureChannelCreator fcc = peer.getConnectionBean().reservation().create(size, 0);

            fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        Utils.addReleaseListener(future.getChannelCreator(), futureLateJoin);
                        for (int i = 0; i < size; i++) {
                            final InetAddress broadcastAddress = bindings.getBroadcastAddresses().get(i);
                            final PeerAddress peerAddress = new PeerAddress(Number160.ZERO, broadcastAddress,
                                    port, port);
                            FutureResponse validBroadcast = peer.getHandshakeRPC().pingBroadcastUDP(
                                    peerAddress, future.getChannelCreator(), connectionConfiguration);
                            if (!futureLateJoin.add(validBroadcast)) {
                                // the latejoin future is fininshed if the add returns false
                                break;
                            }
                        }
                    } else {
                        futureLateJoin.setFailed(future);
                    }
                }
            });
        } else {
            futureLateJoin.setFailed("No broadcast address found. Cannot ping nothing");
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
        return ping(address, true);
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
    public FutureResponse ping(final InetSocketAddress address, final boolean isUDP) {
        final RequestHandler<FutureResponse> request = peer.getHandshakeRPC().ping(
                new PeerAddress(Number160.ZERO, address), connectionConfiguration);
        if (isUDP) {
            FutureChannelCreator fcc = peer.getConnectionBean().reservation().create(1, 0);
            fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(final FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        FutureResponse futureResponse = request.sendUDP(future.getChannelCreator());
                        Utils.addReleaseListener(future.getChannelCreator(), futureResponse);
                    } else {
                        request.futureResponse().setFailed(future);
                    }
                }
            });
        } else {
            FutureChannelCreator fcc = peer.getConnectionBean().reservation().create(0, 1);
            fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(final FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        FutureResponse futureResponse = request.sendTCP(future.getChannelCreator());
                        Utils.addReleaseListener(future.getChannelCreator(), futureResponse);
                    } else {
                        request.futureResponse().setFailed(future);
                    }
                }
            });
        }
        return request.futureResponse();
    }
}
