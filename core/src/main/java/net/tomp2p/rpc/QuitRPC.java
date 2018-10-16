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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import net.tomp2p.connection.ChannelSender;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerException;
import net.tomp2p.connection.PeerException.AbortCause;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.network.KCP;
import net.tomp2p.p2p.builder.ShutdownBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Quit RPC is used to send friendly shutdown messages by peers that are
 * shutdown regularly.
 * 
 * @author Thomas Bocek
 * 
 */
public class QuitRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(QuitRPC.class);

	private final List<PeerStatusListener> listeners = new ArrayList<PeerStatusListener>();

	/**
	 * Constructor that registers this RPC with the message handler.
	 * 
	 * @param peerBean
	 *            The peer bean that contains data that is unique for each peer
	 * @param connectionBean
	 *            The connection bean that is unique per connection (multiple
	 *            peers can share a single connection)
	 */
	public QuitRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
		super(peerBean, connectionBean);
		register(RPC.Commands.QUIT.getNr());
	}

	/**
	 * Add a peer status listener that gets notified when a peer is offline.
	 * 
	 * @param listener
	 *            The listener to be added
	 * @return This class
	 */
	public QuitRPC addPeerStatusListener(final PeerStatusListener listener) {
		listeners.add(listener);
		return this;
	}

	/**
	 * Sends a message that indicates this peer is about to quit. This is an
	 * RPC.
	 * 
	 * @param remotePeer
	 *            The remote peer to send this request
	 * @param shutdownBuilder
	 *            Used for the sign and force TCP flag Set if the message should
	 *            be signed
	 * @return The future response to keep track of future events
	 */
	public Pair<FutureDone<Message>, KCP> quit(final PeerAddress remotePeer, final ShutdownBuilder shutdownBuilder) {
		final Message message = createMessage(remotePeer, RPC.Commands.QUIT.getNr(), Type.REQUEST_FF_1);
		if (shutdownBuilder.sign()) {
			message.publicKeyAndSign(shutdownBuilder.keyPair());
		}
		LOG.debug("send QUIT message {}.", message);
		return connectionBean().channelServer().sendUDP(message);
	}

	@Override
	public void handleResponse(Responder r, final Message message, final boolean sign, KCP kcp, ChannelSender sender) throws Exception {
		if (!(message.type() == Type.REQUEST_FF_1 && message.command() == RPC.Commands.QUIT.getNr())) {
			throw new IllegalArgumentException("Message content is wrong for this handler.");
		}
		LOG.debug("received QUIT message {}", message);
		synchronized (peerBean().peerStatusListeners()) {
			for (PeerStatusListener peerStatusListener : peerBean().peerStatusListeners()) {
				peerStatusListener.peerFailed(message.sender(), new PeerException(AbortCause.SHUTDOWN, "shutdown"));
			}
		}
		r.response(null);
	}
}
