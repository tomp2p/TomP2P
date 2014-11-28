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

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Quit RPC is used to send friendly shutdown messages by peers that are
 * shutdown regularly.
 * 
 * @author Thomas Bocek
 * 
 */
public class AnnounceRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(AnnounceRPC.class);

	/**
	 * Constructor that registers this RPC with the message handler.
	 * 
	 * @param peerBean
	 *            The peer bean that contains data that is unique for each peer
	 * @param connectionBean
	 *            The connection bean that is unique per connection (multiple
	 *            peers can share a single connection)
	 */
	public AnnounceRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
		super(peerBean, connectionBean);
		register(RPC.Commands.LOCAL_ANNOUNCE.getNr());
	}

	public FutureResponse broadcast(final PeerAddress remotePeer, final ConnectionConfiguration configuration,
	        final ChannelCreator channelCreator) {
		final Message message = createMessage(remotePeer, RPC.Commands.LOCAL_ANNOUNCE.getNr(), Type.REQUEST_FF_1);
		
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse,
		        peerBean(), connectionBean(), configuration);
		LOG.debug("send ANNOUNCE BROADCAST message {}", message);
		return requestHandler.fireAndForgetBroadcastUDP(channelCreator);
	}
	
	public FutureResponse ping(final PeerAddress remotePeer, final ConnectionConfiguration configuration,
	        final ChannelCreator channelCreator) {
		final Message message = createMessage(remotePeer, RPC.Commands.LOCAL_ANNOUNCE.getNr(), Type.REQUEST_1);
		
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse,
		        peerBean(), connectionBean(), configuration);
		LOG.debug("send ANNOUNCE PING message {}", message);
		futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
			@Override
            public void operationComplete(FutureResponse future) throws Exception {
	            if(future.isSuccess()) {
	            	peerBean().localMap().peerFound(message.recipient(), null);
	            }
            }
		});
		return requestHandler.sendBroadcastUDP(channelCreator);
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign,
	        Responder responder) throws Exception {
		if (!(message.command() == RPC.Commands.LOCAL_ANNOUNCE.getNr())) {
			throw new IllegalArgumentException("Message content is wrong");
		}
		LOG.debug("received ANNOUNCE message {}", message);
		if(message.type() == Type.REQUEST_FF_1) {
			responder.responseFireAndForget();
			//add to local map
			peerBean().localMap().peerFound(message.sender(), message.sender());
			//this will eventually trigger maintenance
		} else if(message.type() == Type.REQUEST_1) {
			responder.response(createResponseMessage(message, Type.OK));
			//add to local map
			peerBean().localMap().peerFound(message.sender(), message.sender());
		}
	}
}
