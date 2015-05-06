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

import java.security.KeyPair;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.SendDirectBuilderI;
import net.tomp2p.utils.Utils;

public class SendDirectBuilder implements ConnectionConfiguration, SendDirectBuilderI,
        SignatureBuilder<SendDirectBuilder> {
	private static final FutureDirect FUTURE_REQUEST_SHUTDOWN = new FutureDirect("Peer is shutting down.");

	private final Peer peer;

	private final PeerAddress recipientAddress;

	private Buffer buffer;

	private FuturePeerConnection recipientConnection;
	private PeerConnection peerConnection;

	private Object object;

	private FutureChannelCreator futureChannelCreator;

	private boolean streaming = false;

	private boolean forceUDP = false;

	private KeyPair keyPair = null;

	private int idleTCPSeconds = ConnectionBean.DEFAULT_TCP_IDLE_SECONDS;
	private int idleUDPSeconds = ConnectionBean.DEFAULT_UDP_IDLE_SECONDS;
	private int connectionTimeoutTCPMillis = ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP;
	private int slowResponseTimeoutSeconds = ConnectionBean.DEFAULT_SLOW_RESPONSE_TIMEOUT_SECONDS;

	private boolean forceTCP = false;

	public SendDirectBuilder(Peer peer, PeerAddress recipientAddress) {
		this.peer = peer;
		this.recipientAddress = recipientAddress;
		this.recipientConnection = null;
	}

	public SendDirectBuilder(Peer peer, FuturePeerConnection recipientConnection) {
		this.peer = peer;
		this.recipientAddress = null;
		this.recipientConnection = recipientConnection;
	}

	public SendDirectBuilder(Peer peer, PeerConnection peerConnection) {
		this.peer = peer;
		this.recipientAddress = null;
		this.peerConnection = peerConnection;
    }

	public PeerAddress recipient() {
		return recipientAddress;
	}

	public Buffer buffer() {
		return buffer;
	}

	public SendDirectBuilder buffer(Buffer buffer) {
		this.buffer = buffer;
		return this;
	}

	public FuturePeerConnection connection() {
		return recipientConnection;
	}

	public SendDirectBuilder connection(FuturePeerConnection connection) {
		this.recipientConnection = connection;
		return this;
	}

	public PeerConnection peerConnection() {
		return peerConnection;
	}

	public SendDirectBuilder peerConnection(PeerConnection peerConnection) {
		this.peerConnection = peerConnection;
		return this;
	}

	public Object object() {
		return object;
	}

	public SendDirectBuilder object(Object object) {
		this.object = object;
		return this;
	}

	public FutureChannelCreator futureChannelCreator() {
		return futureChannelCreator;
	}

	public SendDirectBuilder futureChannelCreator(FutureChannelCreator futureChannelCreator) {
		this.futureChannelCreator = futureChannelCreator;
		return this;
	}

	public SendDirectBuilder streaming(boolean streaming) {
		this.streaming = streaming;
		return this;
	}

	public boolean isStreaming() {
		return streaming;
	}

	public SendDirectBuilder streaming() {
		this.streaming = true;
		return this;
	}

	public boolean isRaw() {
		return object == null;
	}

	public FutureDirect start() {
		if (peer.isShutdown()) {
			return FUTURE_REQUEST_SHUTDOWN;
		}

		final boolean keepAlive;
		final PeerAddress remotePeer;
		if (recipientAddress != null && recipientConnection == null) {
			keepAlive = false;
			remotePeer = recipientAddress;
		} else if (recipientAddress == null && recipientConnection != null) {
			keepAlive = true;
			remotePeer = recipientConnection.remotePeer();
		} else if (peerConnection != null) {
			keepAlive = true;
			remotePeer = peerConnection.remotePeer();
		} else {
			throw new IllegalArgumentException("Either the recipient address or peer connection has to be set.");
		}

		if (futureChannelCreator == null) {
			futureChannelCreator = peer.connectionBean().reservation()
			        .create(isForceUDP() ? 1 : 0, isForceUDP() ? 0 : 1);
		}

		final RequestHandler<FutureResponse> request = peer.directDataRPC().sendInternal(remotePeer, this);
		if (keepAlive) {
			if (peerConnection != null) {
				sendDirectRequest(request, peerConnection);
			} else {
				recipientConnection.addListener(new BaseFutureAdapter<FuturePeerConnection>() {
					@Override
					public void operationComplete(final FuturePeerConnection future) throws Exception {
						if (future.isSuccess()) {
							sendDirectRequest(request, future.peerConnection());
						} else {
							request.futureResponse().failed("Could not acquire channel (1).", future);
						}
					}
				});
			}

		} else {
			Utils.addReleaseListener(futureChannelCreator, request.futureResponse());
			futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
				@Override
				public void operationComplete(final FutureChannelCreator future) throws Exception {
					if (future.isSuccess()) {
						if (isForceUDP()) {
							request.sendUDP(future.channelCreator());
						} else {
							request.sendTCP(future.channelCreator());
						}
					} else {
						request.futureResponse().failed("Could not create channel.", future);
					}
				}
			});
		}

		return new FutureDirect(request.futureResponse());
	}

	private static void sendDirectRequest(final RequestHandler<FutureResponse> request, final PeerConnection peerConnection) {
		FutureChannelCreator futureChannelCreator2 = peerConnection.acquire(request.futureResponse());
		futureChannelCreator2.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					request.futureResponse().request().keepAlive(true);
					request.sendTCP(peerConnection.channelCreator(), peerConnection);
				} else {
					request.futureResponse().failed("Could not acquire channel (2).", future);
				}
			}

		});
	}

	public boolean isForceUDP() {
		return forceUDP;
	}

	public SendDirectBuilder forceUDP(final boolean forceUDP) {
		this.forceUDP = forceUDP;
		return this;
	}

	public SendDirectBuilder forceUDP() {
		this.forceUDP = true;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.tomp2p.p2p.builder.ConnectionConfiguration#idleTCPSeconds()
	 */
	@Override
	public int idleTCPSeconds() {
		return idleTCPSeconds;
	}

	/**
	 * @param idleTCPSeconds
	 *            The time that a connection can be idle before its considered
	 *            not active for short-lived connections
	 * @return This class
	 */
	public SendDirectBuilder idleTCPSeconds(final int idleTCPSeconds) {
		this.idleTCPSeconds = idleTCPSeconds;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.tomp2p.p2p.builder.ConnectionConfiguration#idleUDPSeconds()
	 */
	@Override
	public int idleUDPSeconds() {
		return idleUDPSeconds;
	}

	/**
	 * @param idleUDPSeconds
	 *            The time that a connection can be idle before its considered
	 *            not active for short-lived connections
	 * @return This class
	 */
	public SendDirectBuilder idleUDPSeconds(final int idleUDPSeconds) {
		this.idleUDPSeconds = idleUDPSeconds;
		return this;
	}

	/**
	 * @param connectionTimeoutTCPMillis
	 *            The time a TCP connection is allowed to be established
	 * @return This class
	 */
	public SendDirectBuilder connectionTimeoutTCPMillis(final int connectionTimeoutTCPMillis) {
		this.connectionTimeoutTCPMillis = connectionTimeoutTCPMillis;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * net.tomp2p.p2p.builder.ConnectionConfiguration#connectionTimeoutTCPMillis
	 * ()
	 */
	@Override
	public int connectionTimeoutTCPMillis() {
		return connectionTimeoutTCPMillis;
	}

	/**
	 * @return Set to true if the communication should be TCP, default is UDP
	 *         for routing
	 */
	public boolean isForceTCP() {
		return forceTCP;
	}

	/**
	 * @param forceTCP
	 *            Set to true if the communication should be TCP, default is UDP
	 *            for routing
	 * @return This class
	 */
	public SendDirectBuilder forceTCP(final boolean forceTCP) {
		this.forceTCP = forceTCP;
		return this;
	}

	/**
	 * @return Set to true if the communication should be TCP, default is UDP
	 *         for routing
	 */
	public SendDirectBuilder forceTCP() {
		this.forceTCP = true;
		return this;
	}
	

	@Override
	public int slowResponseTimeoutSeconds() {
		return slowResponseTimeoutSeconds;
	}
	
	/**
	 * @param slowResponseTimeoutSeconds the amount of seconds a requester waits for the final answer of a
	 *            slow peer. If the slow peer does not answer within this time, the request fails.
	 * @return This class
	 */
	public SendDirectBuilder slowResponseTimeoutSeconds(final int slowResponseTimeoutSeconds) {
		this.slowResponseTimeoutSeconds = slowResponseTimeoutSeconds;
		return this;
	}

	/**
	 * @return Set to true if the message should be signed. For protecting an
	 *         entry, this needs to be set to true.
	 */
	public boolean isSign() {
		return keyPair != null;
	}

	/**
	 * @param signMessage
	 *            Set to true if the message should be signed. For protecting an
	 *            entry, this needs to be set to true.
	 * @return This class
	 */
	public SendDirectBuilder sign(final boolean signMessage) {
		if (signMessage) {
			sign();
		} else {
			this.keyPair = null;
		}
		return this;
	}

	/**
	 * @return Set to true if the message should be signed. For protecting an
	 *         entry, this needs to be set to true.
	 */
	public SendDirectBuilder sign() {
		this.keyPair = peer.peerBean().keyPair();
		return this;
	}

	/**
	 * @param keyPair
	 *            The keyPair to sing the complete message. The key will be
	 *            attached to the message and stored potentially with a data
	 *            object (if there is such an object in the message).
	 * @return This class
	 */
	public SendDirectBuilder keyPair(KeyPair keyPair) {
		this.keyPair = keyPair;
		return this;
	}

	/**
	 * @return The current keypair to sign the message. If null, no signature is
	 *         applied.
	 */
	public KeyPair keyPair() {
		return keyPair;
	}
}
