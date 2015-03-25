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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerReachable;
import net.tomp2p.p2p.PeerReceivedBroadcastPing;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
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

	public static final int WAIT_TIME = 10 * 1000;

	private final List<PeerReachable> reachableListeners = new ArrayList<PeerReachable>(1);
	private final List<PeerReceivedBroadcastPing> receivedBroadcastPingListeners = new ArrayList<PeerReceivedBroadcastPing>(
			1);

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
	 * @param enable
	 *            Used for test cases, set to true in production
	 * @param register
	 *            Used for test cases, set to true in production
	 * @param wait
	 *            Used for test cases, set to false in production
	 */
	PingRPC(final PeerBean peerBean, final ConnectionBean connectionBean, final boolean enable, final boolean register,
			final boolean wait) {
		super(peerBean, connectionBean);
		this.enable = enable;
		this.wait = wait;
		if (register) {
			connectionBean.dispatcher().registerIoHandler(peerBean.serverPeerAddress().peerId(),
					peerBean.serverPeerAddress().peerId(), this, RPC.Commands.PING.getNr());
		}
	}

	/**
	 * Ping with UDP or TCP, but do not send yet.
	 * 
	 * @param remotePeer
	 *            The destination peer
	 * @return the request handler, where we can call sendUDP(), or sendTCP()
	 */
	public RequestHandler<FutureResponse> ping(final PeerAddress remotePeer, final ConnectionConfiguration configuration) {
		return createHandler(remotePeer, Type.REQUEST_1, configuration);
	}

	/**
	 * Ping a UDP peer.
	 * 
	 * @param remotePeer
	 *            The destination peer
	 * @param channelCreator
	 *            The channel creator where we create a UPD channel
	 * @return The future that will be triggered when we receive an answer or
	 *         something fails.
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
	 * @return The future that will be triggered when we receive an answer or
	 *         something fails.
	 */
	public FutureResponse pingBroadcastUDP(final PeerAddress remotePeer, final ChannelCreator channelCreator,
			final ConnectionConfiguration configuration) {
		return createHandler(remotePeer, Type.REQUEST_4, configuration).sendBroadcastUDP(channelCreator);
	}

	/**
	 * Ping a TCP peer.
	 * 
	 * @param remotePeer
	 *            The destination peer
	 * @param channelCreator
	 *            The channel creator where we create a UPD channel
	 * @return The future that will be triggered when we receive an answer or
	 *         something fails.
	 */
	public FutureResponse pingTCP(final PeerAddress remotePeer, final ChannelCreator channelCreator,
			final ConnectionConfiguration configuration) {
		LOG.debug("ping the remote peer {}", remotePeer);
		return ping(remotePeer, configuration).sendTCP(channelCreator);
	}

	/**
	 * Ping a UDP peer, but don't expect an answer.
	 * 
	 * @param remotePeer
	 *            The destination peer
	 * @param channelCreator
	 *            The channel creator where we create a UPD channel
	 * @return The future that will be triggered when we receive an answer or
	 *         something fails.
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
	 * @return The future that will be triggered when we receive an answer or
	 *         something fails.
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
	 * @return The future that will be triggered when we receive an answer or
	 *         something fails.
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
	 * @return The future that will be triggered when we receive an answer or
	 *         something fails.
	 */
	public FutureResponse pingTCPDiscover(final PeerAddress remotePeer, final ChannelCreator channelCreator,
			final ConnectionConfiguration configuration) {
		final FutureResponse futureResponse = createDiscoverHandler(remotePeer);
		return new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), configuration)
				.sendTCP(channelCreator);
	}

	/**
	 * Ping a UDP peer, and request the other peer to ping us on our public
	 * address with a fire and forget message.
	 * 
	 * @param remotePeer
	 *            The destination peer
	 * @param channelCreator
	 *            The channel creator where we create a UPD channel
	 * @return The future that will be triggered when we receive an answer or
	 *         something fails.
	 */
	public FutureResponse pingUDPProbe(final PeerAddress remotePeer, final ChannelCreator channelCreator,
			final ConnectionConfiguration configuration) {
		final Message message = createMessage(remotePeer, RPC.Commands.PING.getNr(), Type.REQUEST_3);
		FutureResponse futureResponse = new FutureResponse(message);
		return new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), configuration)
				.sendUDP(channelCreator);
	}

	/**
	 * Ping a TCP peer, and request the other peer to ping us on our public
	 * address with a fire and forget message.
	 * 
	 * @param remotePeer
	 *            The destination peer
	 * @param channelCreator
	 *            The channel creator where we create a UPD channel
	 * @return The future that will be triggered when we receive an answer or
	 *         something fails.
	 */
	public FutureResponse pingTCPProbe(final PeerAddress remotePeer, final ChannelCreator channelCreator,
			final ConnectionConfiguration configuration) {
		final Message message = createMessage(remotePeer, RPC.Commands.PING.getNr(), Type.REQUEST_3);
		FutureResponse futureResponse = new FutureResponse(message);
		return new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), configuration)
				.sendTCP(channelCreator);
	}

	/**
	 * Ping a peer, and request the other peer to return the source port of our
	 * socket used to send the message. This method needs to do the setup of a
	 * ChannelFuture manually since we need to know the port number of the
	 * assigned socket.
	 * 
	 * @param remotePeer
	 *            The destination peer
	 * @param channelCreator
	 *            The channel creator where we create a UPD channel
	 * @return The future that will be triggered when we receive an answer or
	 *         something fails.
	 */
	public FutureDone<List<PeerSocketAddress>> pingNATType(final PeerAddress remotePeer,
			final ChannelCreator channelCreator, final ConnectionConfiguration configuration, final Peer peer) {
		final FutureDone<List<PeerSocketAddress>> fDone = new FutureDone<List<PeerSocketAddress>>();
		final List<PeerSocketAddress> peerSocketAddresses = new ArrayList<PeerSocketAddress>(2);
		final Message message = createMessage(remotePeer, RPC.Commands.PING.getNr(), Type.REQUEST_5);
		final FutureResponse futureResponse = new FutureResponse(message);
		final SimpleChannelInboundHandler<Message> inbound = new SimpleChannelInboundHandler<Message>() {

			@Override
			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
				if (!msg.peerSocketAddresses().isEmpty() && msg.type() == Type.OK) {
					peerSocketAddresses.add(msg.peerSocketAddresses().get(0));
					fDone.done(peerSocketAddresses);
					ctx.close();
				}
			}
		};
		Utils.addReleaseListener(channelCreator, futureResponse);
		final ChannelFuture cF = channelCreator.createUDP(false,
				peer.connectionBean().sender().configureHandlers(inbound, futureResponse, 30, false), futureResponse);
		cF.addListener(new GenericFutureListener<ChannelFuture>() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (future.isSuccess()) {
					InetSocketAddress srcAddress = (InetSocketAddress) future.channel().localAddress();
					peerSocketAddresses.add(new PeerSocketAddress(srcAddress.getAddress(), srcAddress.getPort(),
							srcAddress.getPort()));
					peer.connectionBean().sender().afterConnect(futureResponse, message, future, false);
				}
			}
		});
		return fDone;
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
		final Message message = createMessage(remotePeer, RPC.Commands.PING.getNr(), type);
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
		final Message message = createMessage(remotePeer, RPC.Commands.PING.getNr(), Type.REQUEST_2);
		message.neighborsSet(createNeighborSet(peerBean().serverPeerAddress()));
		return new FutureResponse(message);
	}

	/**
	 * Create a neighbor set with one peer. We only support sending a neighbor
	 * set, so we need this wrapper class.
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
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign,
			Responder responder) throws Exception {
		if (!((message.type() == Type.REQUEST_FF_1 || message.type() == Type.REQUEST_1
				|| message.type() == Type.REQUEST_2 || message.type() == Type.REQUEST_3
				|| message.type() == Type.REQUEST_4 || message.type() == Type.REQUEST_5) && message.command() == RPC.Commands.PING
				.getNr())) {
			throw new IllegalArgumentException("Message content is wrong");
		}
		final Message responseMessage;
		// probe
		if (message.type() == Type.REQUEST_3) {
			LOG.debug("reply to probing, fire message to {}", message.sender());

			responseMessage = createResponseMessage(message, Type.OK);

			if (message.isUdp()) {
				connectionBean().reservation().create(1, 0).addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						if (future.isSuccess()) {
							LOG.debug("fire UDP to {}", message.sender());
							FutureResponse futureResponse = fireUDP(message.sender(), future.channelCreator(),
									connectionBean().channelServer().channelServerConfiguration());
							Utils.addReleaseListener(future.channelCreator(), futureResponse);
						} else {
							Utils.addReleaseListener(future.channelCreator());
							LOG.warn("handleResponse for REQUEST_3 failed (UDP) {}", future.failedReason());
						}
					}
				});
			} else {
				connectionBean().reservation().create(0, 1).addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						if (future.isSuccess()) {
							LOG.debug("fire TCP to {}", message.sender());
							FutureResponse futureResponse = fireTCP(message.sender(), future.channelCreator(),
									connectionBean().channelServer().channelServerConfiguration());
							Utils.addReleaseListener(future.channelCreator(), futureResponse);
						} else {
							Utils.addReleaseListener(future.channelCreator());
							LOG.warn("handleResponse for REQUEST_3 failed (TCP) {}", future.failedReason());
						}
					}
				});
			}
		} else if (message.type() == Type.REQUEST_2) { // discover
			LOG.debug("reply to discover, found {}", message.sender());
			responseMessage = createResponseMessage(message, Type.OK);
			responseMessage.neighborsSet(createNeighborSet(message.sender()));
		} else if (message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_4) { // regular
																							// ping
			LOG.debug("reply to regular ping {}", message.sender());
			// test if this is a broadcast message to ourselves. If it is, do
			// not
			// reply.
			if (message.isUdp() && message.sender().peerId().equals(peerBean().serverPeerAddress().peerId())
					&& message.recipient().peerId().equals(Number160.ZERO)) {
				LOG.warn("don't reply, we are on the same peer, you should not make this call");
				responder.responseFireAndForget();
			}
			if (enable) {
				responseMessage = createResponseMessage(message, Type.OK);
				if (wait) {
					Thread.sleep(WAIT_TIME);
				}
			} else {
				LOG.debug("do not reply");
				// used for debugging
				if (wait) {
					Thread.sleep(WAIT_TIME);
				}
				return;
			}
			if (message.type() == Type.REQUEST_4) {
				synchronized (receivedBroadcastPingListeners) {
					for (PeerReceivedBroadcastPing listener : receivedBroadcastPingListeners) {
						listener.broadcastPingReceived(message.sender());
					}
				}
			}
		} else if (message.type() == Message.Type.REQUEST_5) {
			LOG.debug("reply to natTypePing {}");
			// simply sends back the sourceport on which the peer sent the ping
			// from
			responseMessage = createResponseMessage(message, Type.OK);
			int udpPort = -1;
			int tcpPort = -1;
			if (message.isUdp()) {
				udpPort = message.senderSocket().getPort();
			} else {
				tcpPort = message.senderSocket().getPort();
			}
			PeerSocketAddress pAddress = new PeerSocketAddress(message.senderSocket().getAddress(), tcpPort, udpPort);
			responseMessage.peerSocketAddresses().clear();
			Collection<PeerSocketAddress> list = new ArrayList<PeerSocketAddress>(2);
			list.add(pAddress);
			responseMessage.peerSocketAddresses(list);
			responseMessage.intValue(message.sender().peerSocketAddress().udpPort());
		} else { // fire and forget - if (message.getType() ==
					// Type.REQUEST_FF_1)
			// we received a fire and forget ping. This means we are reachable
			// from outside
			// responseMessage = null;
			PeerAddress serverAddress = peerBean().serverPeerAddress();
			if (message.isUdp()) {
				PeerAddress newServerAddress = serverAddress.changeFirewalledUDP(false);
				peerBean().serverPeerAddress(newServerAddress);
				synchronized (reachableListeners) {
					for (PeerReachable listener : reachableListeners) {
						listener.peerWellConnected(newServerAddress, message.sender(), false);
					}
				}
				responseMessage = message;
			} else {
				PeerAddress newServerAddress = serverAddress.changeFirewalledTCP(false);
				peerBean().serverPeerAddress(newServerAddress);
				synchronized (reachableListeners) {
					for (PeerReachable listener : reachableListeners) {
						listener.peerWellConnected(newServerAddress, message.sender(), true);
					}
				}
				responseMessage = createResponseMessage(message, Type.OK);
			}
		}
		responder.response(responseMessage);
	}

	public void addPeerReachableListener(PeerReachable peerReachable) {
		synchronized (reachableListeners) {
			reachableListeners.add(peerReachable);
		}
	}

	public void removePeerReachableListener(PeerReachable peerReachable) {
		synchronized (reachableListeners) {
			reachableListeners.remove(peerReachable);
		}
	}

	public void addPeerReceivedBroadcastPingListener(PeerReceivedBroadcastPing peerReceivedBroadcastPing) {
		receivedBroadcastPingListeners.add(peerReceivedBroadcastPing);
	}

	public void removePeerReceivedBroadcastPingListener(PeerReceivedBroadcastPing peerReceivedBroadcastPing) {
		receivedBroadcastPingListeners.remove(peerReceivedBroadcastPing);
	}
}
