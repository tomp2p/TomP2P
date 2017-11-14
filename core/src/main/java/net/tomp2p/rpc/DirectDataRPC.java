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

import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.net.InetSocketAddress;

import net.sctp4nat.connection.SctpChannel;
import net.sctp4nat.connection.SctpUtils;
import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.core.SctpInitException;
import net.sctp4nat.core.SctpPorts;
import net.sctp4nat.core.SctpSocketAdapter;
import net.sctp4nat.core.SctpSocketBuilder;
import net.sctp4nat.origin.SctpNotification;
import net.sctp4nat.origin.SctpSocket;
import net.sctp4nat.origin.SctpSocket.NotificationListener;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ClientChannel;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	/**
	 * Sends data directly to a peer. Make sure you have set up a reply handler.
	 * This is an RPC.
	 * 
	 * @param remotePeer
	 *            The remote peer to store the data
	 */
	private void sendInternal(Message message, final SendDirectBuilderI sendDirectBuilder) {
		if (sendDirectBuilder.isSign()) {
			message.publicKeyAndSign(sendDirectBuilder.keyPair());
		}
		// TODO: this flag comes from the sendirectbuilder
		message.keepAlive(true);

	}

	public FutureDone<SctpChannelFacade> send(final PeerAddress remotePeer, final SendDirectBuilderI sendDirectBuilder,
			final ChannelCreator channelCreator) throws SctpInitException {
		Message message = sendInternal0(remotePeer, sendDirectBuilder);
		sendInternal(message, sendDirectBuilder);

		// int localSctpPort = SctpPorts.getInstance().generateDynPort();
		int localSctpPort = 51000;

		InetSocketAddress a = remotePeer.ipv4Socket().createUDPSocket();
		final SctpSocketAdapter socket = new SctpSocketBuilder().localSctpPort(localSctpPort)
				.remoteAddress(a.getAddress()).remotePort(a.getPort()).mapper(SctpUtils.getMapper()).build();
		socket.listen();

		System.err.println("client listen: " + localSctpPort);
		final FutureDone<SctpChannelFacade> futureDone = new FutureDone<>();
		socket.setNotificationListener(new NotificationListener() {

			@Override
			public void onSctpNotification(SctpSocket socket2, SctpNotification notification) {
				LOG.debug(notification.toString());
				if (notification.toString().indexOf("COMM_UP") >= 0) {
					futureDone.done((SctpChannelFacade) socket);
				}
			}
		});

		message.sctpSocketAdapter(socket);
		Pair<FutureDone<Message>, FutureDone<ClientChannel>> pair = channelCreator.sendUDP(message, localSctpPort);

		return futureDone;
	}

	@Override
	public Message handleResponse(Message message, boolean sign) throws Exception {
		if (message.type() == Type.REQUEST_1) {
			System.err.println("start SCTP build channel");
			SctpChannel c = SctpChannel.builder().local(message.recipientSocket()).remote(message.senderSocket())
					.localSctpPort(message.senderSocket().getPort()).build();
			message.sctpChannel(c);

		}
		return createResponseMessage(message, Type.OK);
	}

}
