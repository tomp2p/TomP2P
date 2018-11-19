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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.connection.ChannelSender;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.network.KCP;
import net.tomp2p.p2p.PeerReachable;
import net.tomp2p.p2p.PeerReceivedBroadcastPing;
import net.tomp2p.peers.Number256;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Pair;

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
	 * Creates a new handshake RPC with listeners.
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
			connectionBean.dispatcher().registerIoHandler(
					peerBean.serverPeerAddress().peerId(),
					peerBean.serverPeerAddress().peerId(),
					this,
					RPC.Commands.PING.getNr(),
					RPC.Commands.PING_DISCOVER.getNr(),
					RPC.Commands.PING_PROBE.getNr(),
					RPC.Commands.PING_NOACK.getNr() );
		}
	}

	/**
	 * Ping a UDP peer.
	 * 
	 * @param remotePeer
	 *            The destination peer
	 *
	 * @return The future that will be triggered when we receive an answer or something fails.
	 */
	public Pair<FutureDone<Message>, KCP> ping(final PeerAddress remotePeer) {
		LOG.debug("Pinging the remote peer {}.", remotePeer);
		final Message message = createMessage(remotePeer, RPC.Commands.PING.getNr(), Type.REQUEST);
		return connectionBean().channelServer().sendUDP(message);
	}

	public Pair<FutureDone<Message>, KCP> pingNoAck(final PeerAddress remotePeer) {
		LOG.debug("Pinging (noack) the remote peer {}.", remotePeer);
		final Message message = createMessage(remotePeer, RPC.Commands.PING_NOACK.getNr(), Type.REQUEST);
		return connectionBean().channelServer().sendUDP(message);
	}

	/**
	 * Ping a UDP peer and find out how the other peer sees us.
	 * 
	 * @param remotePeer
	 *            The destination peer
	 * @return The future that will be triggered when we receive an answer or something fails.
	 */
	public Pair<FutureDone<Message>, KCP> pingDiscover(final PeerAddress remotePeer) {
		LOG.debug("Pinging (discover) the remote peer {}.", remotePeer);
		final Message message = createMessage(remotePeer, RPC.Commands.PING_DISCOVER.getNr(), Type.REQUEST);
		return connectionBean().channelServer().sendUDP(message);
	}

	/**
	 * Ping a UDP peer and request the other peer to ping us on our public address with a fire and forget
	 * message.
	 * 
	 * @param remotePeer
	 *            The destination peer
	 * @return The future that will be triggered when we receive an answer or something fails.
	 */
	public Pair<FutureDone<Message>, KCP> pingProbe(final PeerAddress remotePeer) {
		LOG.debug("Pinging (probe) the remote peer {}.", remotePeer);
		final Message message = createMessage(remotePeer, RPC.Commands.PING_PROBE.getNr(), Type.REQUEST);
		return connectionBean().channelServer().sendUDP(message);
	}


	@Override
	public void handleResponse(Responder r, final Message message, final boolean sign, KCP kcp, ChannelSender sender) throws Exception {
		if (!message.isRequest()) {
			throw new IllegalArgumentException("Request message type or command is wrong for this handler.");
		}
		final Message responseMessage;
		// probe
		if (message.command() == RPC.Commands.PING_PROBE.getNr()) {
			LOG.debug("Respond to probing. Firing message to {}.", message.sender());
			if(message.isSendSelf()) {
				responseMessage = createResponseMessage(message, Type.NOT_FOUND);
				LOG.warn("Sending probe ping request to yourself? If those are two different peers, messages may be dropped");
			} else {
				responseMessage = createResponseMessage(message, Type.OK);
				LOG.debug("Fire UDP to {}.", message.sender());
				pingNoAck(message.sender());
			}
		} else if (message.command() == RPC.Commands.PING_DISCOVER.getNr()) { // discover
			LOG.debug("Respond to discovering. Found {}.", message.sender());
			if(message.isSendSelf()) {
				responseMessage = createResponseMessage(message, Type.NOT_FOUND);
				LOG.warn("Sending discover ping request to yourself? If those are two different peers, messages may be dropped");
			} else {
				responseMessage = createResponseMessage(message, Type.OK);
				final int port = message.senderSocket().getPort();
				ByteBuffer bb = ByteBuffer.allocate(PeerAddress.MAX_SIZE);
				message.sender().encode(bb);
				bb.flip();
				responseMessage.payload(bb);
			}
		} else if (message.command() == RPC.Commands.PING.getNr() ) { // regular ping
			LOG.debug("Respond to regular ping {}.", message.sender());
			// test if this is a broadcast message to ourselves. If it is, do
			// not
			// reply.
			if (message.sender().peerId().equals(peerBean().serverPeerAddress().peerId())
					&& message.recipient().peerId().equals(Number256.ZERO)) {
				LOG.warn("Don't respond. We are on the same peer, you should make this call.");
				return;
			}
			if (enable) {
                    responseMessage = createResponseMessage(message, Type.OK);
				if (wait) {
					Thread.sleep(WAIT_TIME);
				}
			} else {
				LOG.debug("Don't respond.");
				// used for debugging
				if (wait) {
					Thread.sleep(WAIT_TIME);
				}
				return;
			}
		} else if (message.command() == RPC.Commands.PING_NOACK.getNr() ) { // fire and forget ping
			responseMessage = null;
		} else {
			throw new IllegalArgumentException("Request message type or command is wrong for this handler.");

		}
		r.response(responseMessage);
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
