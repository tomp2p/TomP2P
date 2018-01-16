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
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;

public class SendDirectBuilder extends DefaultConnectionConfiguration {
	private static final FutureDirect FUTURE_REQUEST_SHUTDOWN = new FutureDirect(null, false).failed("Peer is shutting down.");

	private final Peer peer;

	private final PeerAddress recipientAddress;

	private ByteBuf dataBuffer;

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
}
