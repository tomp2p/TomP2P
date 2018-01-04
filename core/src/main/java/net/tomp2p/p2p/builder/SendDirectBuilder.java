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

import io.netty.buffer.ByteBuf;
import java.security.KeyPair;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.SendDirectBuilderI;
import net.tomp2p.utils.Utils;

public class SendDirectBuilder implements ConnectionConfiguration, SendDirectBuilderI,
        SignatureBuilder<SendDirectBuilder> {
	private static final FutureDirect FUTURE_REQUEST_SHUTDOWN = new FutureDirect(null, false).failed("Peer is shutting down.");

	private final Peer peer;

	private final PeerAddress recipientAddress;

	private ByteBuf dataBuffer;

	private Object object;

	private boolean streaming = false;

	private boolean forceUDP = false;
	
	private boolean forceSctp = false;

	private KeyPair keyPair = null;

	private int idleTCPMillis = ConnectionBean.DEFAULT_TCP_IDLE_MILLIS;
	private int idleUDPMillis = ConnectionBean.DEFAULT_UDP_IDLE_MILLIS;
	private int connectionTimeoutTCPMillis = ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP;
	private int heartBeatSeconds = ConnectionBean.DEFAULT_HEARTBEAT_SECONDS;

	private boolean forceTCP = false;

	public SendDirectBuilder(Peer peer, PeerAddress recipientAddress) {
		this.peer = peer;
		this.recipientAddress = recipientAddress;
	}


	public PeerAddress recipient() {
		return recipientAddress;
	}

	public ByteBuf dataBuffer() {
		return dataBuffer;
	}

	public SendDirectBuilder dataBuffer(ByteBuf dataBuffer) {
		this.dataBuffer = dataBuffer;
		return this;
	}

	public Object object() {
		return object;
	}

	public SendDirectBuilder object(Object object) {
		this.object = object;
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
		if (recipientAddress != null) {
			keepAlive = true; //FIXME jwa set automatically to true
			remotePeer = recipientAddress;
		} else {
			throw new IllegalArgumentException("Either the recipient address or peer connection has to be set.");
		}

		//TODO: SCTP
		
		/*Message message = peer.directDataRPC().sendInternal0(remotePeer, this);
    	final FutureDirect futureResponse = new FutureDirect(message, isRaw());
        futureResponse.request().keepAlive(keepAlive);

		final RequestHandler request = peer.directDataRPC().sendInternal(futureResponse, this);

                    FutureChannelCreator futureChannelCreator = peer.connectionBean().reservation()
			        .create(1);
			Utils.addReleaseListener(futureChannelCreator, request.futureResponse());
			futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
				@Override
				public void operationComplete(final FutureChannelCreator future) throws Exception {
					if (future.isSuccess()) {
						request.sendUDP(future.channelCreator());

					} else {
						request.futureResponse().failed("Could not create channel.", future);
					}
				}
			});*/


		return null;
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

	/**
	 *            The time that a connection can be idle before its considered
	 *            not active for short-lived connections
	 * @return This class
	 */
	public SendDirectBuilder idleTCPMillis(final int idleTCPMillis) {
		this.idleTCPMillis = idleTCPMillis;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.tomp2p.p2p.builder.ConnectionConfiguration#idleUDPSeconds()
	 */
	@Override
	public int idleUDPMillis() {
		return idleUDPMillis;
	}

	/**
	 *            The time that a connection can be idle before its considered
	 *            not active for short-lived connections
	 * @return This class
	 */
	public SendDirectBuilder idleUDPMillis(final int idleUDPMillis) {
		this.idleUDPMillis = idleUDPMillis;
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
	 *//*Message message = peer.directDataRPC().sendInternal0(remotePeer, this);
	final FutureDirect futureResponse = new FutureDirect(message, isRaw());
    futureResponse.request().keepAlive(keepAlive);

	final RequestHandler request = peer.directDataRPC().sendInternal(futureResponse, this);

                FutureChannelCreator futureChannelCreator = peer.connectionBean().reservation()
		        .create(1);
		Utils.addReleaseListener(futureChannelCreator, request.futureResponse());
		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(final FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					request.sendUDP(future.channelCreator());

				} else {
					request.futureResponse().failed("Could not create channel.", future);
				}
			}
		});*/
	public SendDirectBuilder forceTCP() {
		this.forceTCP = true;
		return this;
	}
	

	@Override
	public int heartBeatSeconds() {
		return heartBeatSeconds;
	}
	
	/**
	 * @return This class
	 */
	public SendDirectBuilder heartBeatSeconds(final int heartBeatSeconds) {
		this.heartBeatSeconds = heartBeatSeconds;
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
