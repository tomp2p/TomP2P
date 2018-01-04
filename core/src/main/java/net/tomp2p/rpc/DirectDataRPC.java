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

import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.origin.SctpDataCallback;
import net.sctp4nat.util.SctpInitException;
import net.tomp2p.connection.ChannelSender;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Triple;

//This will use SCTP!

public class DirectDataRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(DirectDataRPC.class);

	public DirectDataRPC(PeerBean peerBean, ConnectionBean connectionBean) {
		super(peerBean, connectionBean);
		register(RPC.Commands.DIRECT_DATA.getNr());
	}

	public Message sendInternal0(final PeerAddress remotePeer, final SendDirectBuilderI sendDirectBuilder) {
		return createMessage(remotePeer, RPC.Commands.DIRECT_DATA.getNr(), Type.REQUEST_1);
	}

	public Triple<FutureDone<Message>, FutureDone<SctpChannelFacade>, FutureDone<Void>> send(final PeerAddress remotePeer, final SendDirectBuilderI sendDirectBuilder) throws SctpInitException {
		Message message = sendInternal0(remotePeer, sendDirectBuilder);
		if (sendDirectBuilder.isSign()) {
			message.publicKeyAndSign(sendDirectBuilder.keyPair());
		}
		// TODO: this flag comes from the sendirectbuilder
		message.keepAlive(true);
		//message.sctp(true);
		return connectionBean().channelServer().sendUDP(message);
	}

	@Override
	public void handleResponse(Responder r, Message message, boolean sign, Promise<SctpChannelFacade, Exception, Void> p, ChannelSender sender) throws Exception {
		if (message.type() == Type.REQUEST_1) {
			
			//message.sctpChannel(c);

		}
		
		if(p!=null) {
			p.done(new DoneCallback<SctpChannelFacade>() {
				@Override
				public void onDone(SctpChannelFacade result) {
					result.setSctpDataCallback(new SctpDataCallback() {
					
						@Override
						public void onSctpPacket(byte[] data, int sid, int ssn, int tsn, long ppid, int context, int flags,
							SctpChannelFacade so) {
							System.err.println("got packet: "+data.length);
							so.send(new byte[200], true, 0, 0);
						}
					});
				}
		});}
		
		Message m2 = createResponseMessage(message, Type.OK);
		m2.keepAlive(true);
		r.response(m2);
	}

}
