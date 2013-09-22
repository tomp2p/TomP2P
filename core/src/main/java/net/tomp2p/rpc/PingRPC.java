/*
 * Copyright 2009 Thomas Bocek
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
package net.tomp2p.rpc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.connection2.ConnectionConfiguration;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.connection2.RequestHandler;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message2.Type;
import net.tomp2p.message.Message2;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.PeerReachable;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Ping message handler. Also used for NAT detection and other things.
 * 
 * @author Thomas Bocek
 * 
 */
public class PingRPC extends DispatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PingRPC.class);

    public static final byte PING_COMMAND = 0;
    public static final int WAIT_TIME = 10 * 1000;

    private final List<PeerReachable> listeners = new CopyOnWriteArrayList<PeerReachable>();

    // used for testing and debugging
    private final boolean enable;
    private final boolean wait;

    /**
     * Creates an new handshake RPC with listeners.
     * 
     * @param peerBean
     *            The peer bean
     * @param connectionBean
     *            The connection bean
     */
    public PingRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
        this(peerBean, connectionBean, true, true, false);
    }

    /**
     * Constructor that is only called from this class or from testcases.
     * 
     * @param peerBean
     *            The peer bean
     * @param connectionBean
     *            The connection bean
     * @param listeners
     *            The listeners to notify when this peer address (IP) is changed
     * @param enable
     *            Used for test cases, set to true in production
     * @param register
     *            Used for test cases, set to true in production
     * @param wait
     *            Used for test cases, set to false in production
     */
    PingRPC(final PeerBean peerBean, final ConnectionBean connectionBean,
            final boolean enable, final boolean register,
            final boolean wait) {
        super(peerBean, connectionBean);
        this.enable = enable;
        this.wait = wait;
        if (register) {
            connectionBean.dispatcher().registerIoHandler(peerBean.serverPeerAddress().getPeerId(), this,
                    PING_COMMAND);
        }
    }

    /**
     * Ping with UDP or TCP, but do not send yet.
     * 
     * @param remotePeer
     *            The destination peer
     * @return the request handler, where we can call sendUDP(), or sendTCP()
     */
    public RequestHandler<FutureResponse> ping(final PeerAddress remotePeer,
            final ConnectionConfiguration configuration) {
        return createHandler(remotePeer, Type.REQUEST_1, configuration);
    }

    /**
     * Ping a UDP peer.
     * 
     * @param remotePeer
     *            The destination peer
     * @param channelCreator
     *            The channel creator where we create a UPD channel
     * @return The future that will be triggered when we receive an answer or something fails.
     */
    public FutureResponse pingUDP(final PeerAddress remotePeer, final ChannelCreator channelCreator,
            final ConnectionConfiguration configuration) {
        return ping(remotePeer, configuration).sendUDP(channelCreator);
    }

    /**
     * Ping a UDP peer using layer 2 broadcast.
     * 
     * @param remotePeer
     *            The destination peer
     * @param channelCreator
     *            The channel creator where we create a UPD channel
     * @return The future that will be triggered when we receive an answer or something fails.
     */
    public FutureResponse pingBroadcastUDP(final PeerAddress remotePeer, final ChannelCreator channelCreator,
            final ConnectionConfiguration configuration) {
        return ping(remotePeer, configuration).sendBroadcastUDP(channelCreator);
    }

    /**
     * Ping a TCP peer.
     * 
     * @param remotePeer
     *            The destination peer
     * @param channelCreator
     *            The channel creator where we create a UPD channel
     * @return The future that will be triggered when we receive an answer or something fails.
     */
    public FutureResponse pingTCP(final PeerAddress remotePeer, final ChannelCreator channelCreator,
            final ConnectionConfiguration configuration) {
        return ping(remotePeer, configuration).sendTCP(channelCreator);
    }

    /**
     * Ping a UDP peer, but don't expect an answer.
     * 
     * @param remotePeer
     *            The destination peer
     * @param channelCreator
     *            The channel creator where we create a UPD channel
     * @return The future that will be triggered when we receive an answer or something fails.
     */
    public FutureResponse fireUDP(final PeerAddress remotePeer, final ChannelCreator channelCreator,
            final ConnectionConfiguration configuration) {
        return createHandler(remotePeer, Type.REQUEST_FF_1, configuration).fireAndForgetUDP(channelCreator);
    }

    /**
     * Ping a TCP peer, but don't expect an answer.
     * 
     * @param remotePeer
     *            The destination peer
     * @param channelCreator
     *            The channel creator where we create a UPD channel
     * @return The future that will be triggered when we receive an answer or something fails.
     */
    public FutureResponse fireTCP(final PeerAddress remotePeer, final ChannelCreator channelCreator,
            final ConnectionConfiguration configuration) {
        return createHandler(remotePeer, Type.REQUEST_FF_1, configuration).sendTCP(channelCreator);
    }

    /**
     * Ping a UDP peer, and find out how the other peer sees us.
     * 
     * @param remotePeer
     *            The destination peer
     * @param channelCreator
     *            The channel creator where we create a UPD channel
     * @return The future that will be triggered when we receive an answer or something fails.
     */
    public FutureResponse pingUDPDiscover(final PeerAddress remotePeer, final ChannelCreator channelCreator,
            final ConnectionConfiguration configuration) {
        final FutureResponse futureResponse = createDiscoverHandler(remotePeer);
        return new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), configuration)
                .sendUDP(channelCreator);
    }

    /**
     * Ping a TCP peer, and find out how the other peer sees us.
     * 
     * @param remotePeer
     *            The destination peer
     * @param channelCreator
     *            The channel creator where we create a UPD channel
     * @return The future that will be triggered when we receive an answer or something fails.
     */
    public FutureResponse pingTCPDiscover(final PeerAddress remotePeer, final ChannelCreator channelCreator,
            final ConnectionConfiguration configuration) {
        final FutureResponse futureResponse = createDiscoverHandler(remotePeer);
        return new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), configuration)
                .sendTCP(channelCreator);
    }

    /**
     * Ping a UDP peer, and request the other peer to ping us on our public address with a fire and forget message.
     * 
     * @param remotePeer
     *            The destination peer
     * @param channelCreator
     *            The channel creator where we create a UPD channel
     * @return The future that will be triggered when we receive an answer or something fails.
     */
    public FutureResponse pingUDPProbe(final PeerAddress remotePeer, final ChannelCreator channelCreator,
            final ConnectionConfiguration configuration) {
        final Message2 message = createMessage(remotePeer, PING_COMMAND, Type.REQUEST_3);
        FutureResponse futureResponse = new FutureResponse(message);
        return new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), configuration)
                .sendUDP(channelCreator);
    }

    /**
     * Ping a TCP peer, and request the other peer to ping us on our public address with a fire and forget message.
     * 
     * @param remotePeer
     *            The destination peer
     * @param channelCreator
     *            The channel creator where we create a UPD channel
     * @return The future that will be triggered when we receive an answer or something fails.
     */
    public FutureResponse pingTCPProbe(final PeerAddress remotePeer, final ChannelCreator channelCreator,
            final ConnectionConfiguration configuration) {
        final Message2 message = createMessage(remotePeer, PING_COMMAND, Type.REQUEST_3);
        FutureResponse futureResponse = new FutureResponse(message);
        return new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), configuration)
                .sendTCP(channelCreator);
    }

    /**
     * Create a request handler.
     * 
     * @param remotePeer
     *            The destination peer
     * @param type
     *            The type of the request
     * @return The request handler
     */
    private RequestHandler<FutureResponse> createHandler(final PeerAddress remotePeer, final Type type,
            final ConnectionConfiguration configuration) {
        final Message2 message = createMessage(remotePeer, PING_COMMAND, type);
        final FutureResponse futureResponse = new FutureResponse(message);
        return new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), configuration);
    }

    /**
     * Create a discover handler.
     * 
     * @param remotePeer
     *            The destination peer
     * @return The future of this discover handler
     */
    private FutureResponse createDiscoverHandler(final PeerAddress remotePeer) {
        final Message2 message = createMessage(remotePeer, PING_COMMAND, Type.REQUEST_2);
        message.setNeighborsSet(createNeighborSet(peerBean().serverPeerAddress()));
        return new FutureResponse(message);
    }

    /**
     * Create a neighbor set with one peer. We only support sending a neighbor set, so we need this wrapper class.
     * 
     * @param self
     *            The peer that should be stored in the neighborset
     * @return The neighborset with exactly one peer
     */
    private NeighborSet createNeighborSet(final PeerAddress self) {
        Collection<PeerAddress> tmp = new ArrayList<PeerAddress>();
        tmp.add(self);
        return new NeighborSet(-1, tmp);
    }

    @Override
    public Message2 handleResponse(final Message2 message, final boolean sign) throws Exception {
        if (!((message.getType() == Type.REQUEST_FF_1 || message.getType() == Type.REQUEST_1
                || message.getType() == Type.REQUEST_2 || message.getType() == Type.REQUEST_3) && message
                .getCommand() == PING_COMMAND)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        final Message2 responseMessage;
        // probe
        if (message.getType() == Type.REQUEST_3) {
            LOG.debug("reply to probing, fire message to {}", message.getSender());

            responseMessage = createResponseMessage(message, Type.OK);

            if (message.isUdp()) {
                connectionBean().reservation().create(1, 0)
                        .addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                            @Override
                            public void operationComplete(final FutureChannelCreator future) throws Exception {
                                if (future.isSuccess()) {
                                    FutureResponse futureResponse = fireUDP(message.getSender(), future
                                            .getChannelCreator(), connectionBean().channelServer()
                                            .channelServerConfiguration());
                                    Utils.addReleaseListener(future.getChannelCreator(), futureResponse);
                                } else {
                                    LOG.warn("handleResponse for REQUEST_3 failed (UDP) {}",
                                            future.getFailedReason());
                                }
                            }
                        });
            } else {
                connectionBean().reservation().create(0, 1)
                        .addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                            @Override
                            public void operationComplete(final FutureChannelCreator future) throws Exception {
                                if (future.isSuccess()) {
                                    FutureResponse futureResponse = fireTCP(message.getSender(), future
                                            .getChannelCreator(), connectionBean().channelServer()
                                            .channelServerConfiguration());
                                    Utils.addReleaseListener(future.getChannelCreator(), futureResponse);
                                } else {
                                    LOG.warn("handleResponse for REQUEST_3 failed (TCP) {}",
                                            future.getFailedReason());
                                }
                            }
                        });
            }
        } else if (message.getType() == Type.REQUEST_2) { // discover
            LOG.debug("reply to discover, found {}", message.getSender());
            responseMessage = createResponseMessage(message, Type.OK);
            responseMessage.setNeighborsSet(createNeighborSet(message.getSender()));
        } else if (message.getType() == Type.REQUEST_1) { // regular ping
            LOG.debug("reply to regular ping {}", message.getSender());
            // test if this is a broadcast message to ourselves. If it is, do not
            // reply.
            if (message.isUdp() && message.getSender().getPeerId().equals(peerBean().serverPeerAddress().getPeerId())
                    && message.getRecipient().getPeerId().equals(Number160.ZERO)) {
                LOG.debug("don't reply, we are on the same peer");
                return message;
            }
            if (enable) {
                responseMessage = createResponseMessage(message, Type.OK);
                if (wait) {
                    Timings.sleepUninterruptibly(WAIT_TIME);
                }
            } else {
                responseMessage = null;
                LOG.debug("do not reply");
                // used for debugging
                if (wait) {
                    Timings.sleepUninterruptibly(WAIT_TIME);
                }
            }
        } else { // fire and forget - if (message.getType() == Type.REQUEST_FF_1)
            // we received a fire and forget ping. This means we are reachable
            // from outside
            //responseMessage = null;
            PeerAddress serverAddress = peerBean().serverPeerAddress();
            if (message.isUdp()) {
                PeerAddress newServerAddress = serverAddress.changeFirewalledUDP(false);
                peerBean().serverPeerAddress(newServerAddress);
                synchronized (listeners) {
                    for (PeerReachable listener : listeners) {
                        listener.peerWellConnected(newServerAddress, message.getSender(), false);
                    }
                }
                responseMessage = message;
            } else {
                PeerAddress newServerAddress = serverAddress.changeFirewalledTCP(false);
                peerBean().serverPeerAddress(newServerAddress);
                synchronized (listeners) {
                    for (PeerReachable listener : listeners) {
                        listener.peerWellConnected(newServerAddress, message.getSender(), true);
                    }
                }
                responseMessage = createResponseMessage(message, Type.OK);
            }
        }
        return responseMessage;
    }

    public void addPeerReachableListener(PeerReachable peerReachable) {
        listeners.add(peerReachable);
    }
    
    public void removePeerReachableListener(PeerReachable peerReachable) {
        listeners.remove(peerReachable);
    }
}
