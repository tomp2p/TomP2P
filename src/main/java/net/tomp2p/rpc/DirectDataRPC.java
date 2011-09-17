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
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureData;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class DirectDataRPC extends ReplyHandler
{
	private volatile RawDataReply rawDataReply;
	private volatile ObjectDataReply objectDataReply;

	public DirectDataRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		super(peerBean, connectionBean);
		registerIoHandler(Command.DIRECT_DATA);
	}
	
	public FutureData send(final PeerAddress remoteNode, final ChannelBuffer buffer,
			boolean raw, ChannelCreator cc)
	{
		RequestHandlerTCP request=send(remoteNode, buffer, raw);
		return (FutureData) request.sendTCP(cc);
	}

	public RequestHandlerTCP send(final PeerAddress remoteNode, final ChannelBuffer buffer,
			boolean raw)
	{
		final Message message = createMessage(remoteNode, Command.DIRECT_DATA, raw ? Type.REQUEST_1 : Type.REQUEST_2);
		message.setPayload(buffer);
		final FutureData futureData = new FutureData(message, raw);
		final RequestHandlerTCP requestHandler = new RequestHandlerTCP(futureData, peerBean, connectionBean, message);
		return requestHandler;
	}

	public void setReply(final RawDataReply rawDataReply)
	{
		this.rawDataReply = rawDataReply;
	}

	public void setReply(ObjectDataReply objectDataReply)
	{
		this.objectDataReply = objectDataReply;
	}

	@Override
	public boolean checkMessage(final Message message)
	{
		return (message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2)
				&& message.getCommand() == Command.DIRECT_DATA;
	}

	public boolean hasRawDataReply()
	{
		return rawDataReply != null;
	}

	public boolean hasObjectDataReply()
	{
		return objectDataReply != null;
	}

	@Override
	public Message handleResponse(final Message message, boolean sign) throws Exception
	{
		final Message responseMessage = createMessage(message.getSender(), Command.DIRECT_DATA, Type.OK);
		if(sign) {
    		responseMessage.setPublicKeyAndSign(peerBean.getKeyPair());
    	}
		responseMessage.setMessageId(message.getMessageId());
		final RawDataReply rawDataReply2 = rawDataReply;
		final ObjectDataReply objectDataReply2 = objectDataReply;
		if (message.getType() == Type.REQUEST_1 && rawDataReply2 == null)
			responseMessage.setType(Type.NOT_FOUND);
		else if (message.getType() == Type.REQUEST_2 && objectDataReply2 == null)
			responseMessage.setType(Type.NOT_FOUND);
		else
		{
			final ChannelBuffer requestBuffer = message.getPayload1();
			// the user can reply with null, indicating not found. Or
			// returning the request buffer, which means nothing is
			// returned. Or an exception can be thrown
			if (message.getType() == Type.REQUEST_1)
			{
				final ChannelBuffer replyBuffer = rawDataReply2.reply(message.getSender(), requestBuffer);
				if (replyBuffer == null)
					responseMessage.setType(Type.NOT_FOUND);
				else if (replyBuffer == requestBuffer)
					responseMessage.setType(Type.OK);
				else
					responseMessage.setPayload(replyBuffer);
			}
			else
			{
				Object obj = Utils.decodeJavaObject(requestBuffer.array(), requestBuffer.arrayOffset(),
						requestBuffer.capacity());
				Object reply = objectDataReply2.reply(message.getSender(), obj);
				if (reply == null)
					responseMessage.setType(Type.NOT_FOUND);
				else if (reply == obj)
					responseMessage.setType(Type.OK);
				else
				{
					byte[] me = Utils.encodeJavaObject(reply);
					responseMessage.setPayload(ChannelBuffers.wrappedBuffer(me));
				}
			}
		}
		return responseMessage;
	}
}