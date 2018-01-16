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


import org.jdeferred.DoneCallback;
import org.jdeferred.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.origin.SctpDataCallback;
import net.sctp4nat.util.SctpInitException;
import net.tomp2p.connection.ChannelSender;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Pair;

//This will use SCTP!

@Getter @Setter @Accessors(fluent = true)
public class DirectDataRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(DirectDataRPC.class);
	
	private volatile DataCallbackUDP dataCallbackUDP = null;
	private volatile SctpDataCallback dataCallbackSCTP = null;

	public DirectDataRPC(PeerBean peerBean, ConnectionBean connectionBean) {
		super(peerBean, connectionBean);
		register(RPC.Commands.DIRECT_DATA.getNr());
	}

	public Pair<FutureDone<Message>, FutureDone<SctpChannelFacade>> send(final PeerAddress remotePeer, final ConnectionConfiguration conf) throws SctpInitException {
		Message message = createMessage(remotePeer, RPC.Commands.DIRECT_DATA.getNr(), Type.REQUEST_1);
		if (conf.sign()) {
			message.publicKeyAndSign(conf.keyPair());
		}
		// TODO: this flag comes from the sendirectbuilder
		message.sctp(true);
		return connectionBean().channelServer().sendUDP(message);
	}

	@Override
	public void handleResponse(Responder r, Message message, boolean sign, Promise<SctpChannelFacade, Exception, Void> p, ChannelSender sender) throws Exception {
		if(p!=null) {
			p.done(new DoneCallback<SctpChannelFacade>() {
				@Override
				public void onDone(SctpChannelFacade result) {
					if(dataCallbackSCTP != null) {
						result.setSctpDataCallback(dataCallbackSCTP);
					}
				}
			});
		}
		if (dataCallbackUDP != null) {
			Message m2 = dataCallbackUDP.data(message, r, this);
			if(m2 != null) {
				r.response(m2);
			} else {
				LOG.debug("udp callback sent it back");
			}
		} else {
			LOG.debug("no udp callback set, default ok");
			Message m2 = createResponseMessage(message, Type.OK);
			if(message.sctp()) {
				m2.sctp(true);
			}
			r.response(m2);
		}
	}

}
