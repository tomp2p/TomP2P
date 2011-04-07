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

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;

public class QuitRPC extends ReplyHandler
{
	public QuitRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		super(peerBean, connectionBean);
		registerIoHandler(Command.QUIT);
	}

	public FutureResponse quit(final PeerAddress remoteNode)
	{
		final Message message = createMessage(remoteNode, Command.QUIT, Type.REQUEST_1);
		final RequestHandlerUDP requestHandler = new RequestHandlerUDP(peerBean, connectionBean, message);
		return requestHandler.fireAndForgetUDP();
	}

	@Override
	public boolean checkMessage(final Message message)
	{
		return message.getType() == Type.REQUEST_1 && message.getCommand() == Command.QUIT;
	}

	@Override
	public Message handleResponse(final Message message) throws Exception
	{
		peerBean.getPeerMap().peerOffline(message.getSender(), true);
		//TODO: test this
		return message;
	}
}
