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
import java.util.Collection;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandshakeRPC extends ReplyHandler
{
	final private static Logger logger = LoggerFactory.getLogger(HandshakeRPC.class);
	final private boolean enable;

	public HandshakeRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		this(peerBean, connectionBean, true, true);
	}

	HandshakeRPC(PeerBean peerBean, ConnectionBean connectionBean, final boolean enable,
			final boolean register)
	{
		super(peerBean, connectionBean);
		this.enable = enable;
		if (register)
			registerIoHandler(Command.PING);
	}

	public FutureResponse pingBroadcastUDP(final PeerAddress remoteNode)
	{
		return createHandler(remoteNode).sendBroadcastUDP();
	}

	public FutureResponse pingUDP(final PeerAddress remoteNode)
	{
		return createHandler(remoteNode).sendUDP();
	}

	public FutureResponse pingTCP(final PeerAddress remoteNode)
	{
		return createHandler(remoteNode).sendTCP();
	}

	public FutureResponse fireUDP(final PeerAddress remoteNode)
	{
		return createHandler(remoteNode).fireAndForgetUDP();
	}

	public FutureResponse fireTCP(final PeerAddress remoteNode)
	{
		return createHandler(remoteNode).fireAndForgetTCP();
	}

	private RequestHandler createHandler(final PeerAddress remoteNode)
	{
		final Message message = createMessage(remoteNode, Command.PING, Type.REQUEST_1);
		return new RequestHandler(peerBean, connectionBean, message);
	}

	public FutureResponse pingUDPDiscover(final PeerAddress remoteNode)
	{
		final Message message = createMessage(remoteNode, Command.PING, Type.REQUEST_2);
		Collection<PeerAddress> self = new ArrayList<PeerAddress>();
		self.add(peerBean.getServerPeerAddress());
		message.setNeighbors(self);
		return new RequestHandler(peerBean, connectionBean, message).sendUDP();
	}

	public FutureResponse pingTCPDiscover(final PeerAddress remoteNode)
	{
		final Message message = createMessage(remoteNode, Command.PING, Type.REQUEST_2);
		Collection<PeerAddress> self = new ArrayList<PeerAddress>();
		self.add(peerBean.getServerPeerAddress());
		message.setNeighbors(self);
		return new RequestHandler(peerBean, connectionBean, message).sendTCP();
	}

	public FutureResponse pingUDPProbe(final PeerAddress remoteNode)
	{
		final Message message = createMessage(remoteNode, Command.PING, Type.REQUEST_3);
		return new RequestHandler(peerBean, connectionBean, message).sendUDP();
	}

	public FutureResponse pingTCPProbe(final PeerAddress remoteNode)
	{
		final Message message = createMessage(remoteNode, Command.PING, Type.REQUEST_3);
		return new RequestHandler(peerBean, connectionBean, message).sendTCP();
	}

	@Override
	public boolean checkMessage(final Message message)
	{
		return (message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2 || message
				.getType() == Type.REQUEST_3)
				&& message.getCommand() == Command.PING;
	}

	@Override
	public Message handleResponse(final Message message) throws Exception
	{
		if (message.getType() == Type.REQUEST_3)
		{
			final Message responseMessage = createMessage(message.getSender(), Command.PING,
					Type.OK);
			responseMessage.setMessageId(message.getMessageId());
			if (message.isUDP())
				fireUDP(message.getSender());
			else
				fireTCP(message.getSender());
			return responseMessage;
		}
		else if (message.getType() == Type.REQUEST_2)
		{
			final Message responseMessage = createMessage(message.getSender(), Command.PING,
					Type.OK);
			responseMessage.setMessageId(message.getMessageId());
			Collection<PeerAddress> self = new ArrayList<PeerAddress>();
			self.add(message.getRealSender());
			message.setNeighbors(self);
			return responseMessage;
		}
		else
		{
			if (enable)
			{
				final Message responseMessage = createMessage(message.getSender(), Command.PING,
						Type.OK);
				responseMessage.setMessageId(message.getMessageId());
				return responseMessage;
			}
			else
			{
				logger.debug("do not reply");
				return null;
			}
		}
	}
}