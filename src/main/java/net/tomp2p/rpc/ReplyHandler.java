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
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ReplyHandler extends SimpleChannelHandler
{
	final private static Logger logger = LoggerFactory.getLogger(ReplyHandler.class);
	final private PeerBean peerBean;
	final private ConnectionBean connectionBean;
	private boolean sign = false;

	public ReplyHandler(PeerBean peerBean, ConnectionBean connectionBean)
	{
		this.peerBean = peerBean;
		this.connectionBean = connectionBean;
	}

	protected void registerIoHandler(Command... names)
	{
		getConnectionBean().getDispatcherRequest().registerIoHandler(getPeerBean().getServerPeerAddress().getID(), this,
				names);
	}

	public Message createMessage(PeerAddress recipient, Command name, Type type)
	{
		Message m = new Message();
		m.setRecipient(recipient);
		m.setSender(getPeerBean().getServerPeerAddress());
		m.setCommand(name);
		m.setType(type);
		m.setVersion(getConnectionBean().getP2PID());
		return m;
	}
	
	public Message createResponseMessage(Message message, Type type)
	{
		Message m = new Message();
		m.setRecipient(message.getSender());
		m.setSender(getPeerBean().getServerPeerAddress());
		m.setCommand(message.getCommand());
		m.setType(type);
		m.setVersion(getConnectionBean().getP2PID());
		m.setMessageId(message.getMessageId());
		return m;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
	{
		logger.error("error in reply " + e.toString());
		if (logger.isDebugEnabled())
		{
			e.getCause().printStackTrace();
		}
		// TODO: we never attach the message to the context!
		Message message = (Message) ctx.getAttachment();
		if (message != null)
			getPeerBean().getPeerMap().peerOffline(message.getSender(), true);
		ctx.getChannel().close();
	}

	public Message forwardMessage(Message message)
	{
		// here we need a referral, since we got contacted and we don't know
		// if we can contact the peer with its address. The peer may be
		// behind a NAT
		getPeerBean().getPeerMap().peerFound(message.getSender(), message.getSender());
		try
		{//
			Message reply = handleResponse(message, sign);
			return reply;
		}
		catch (Throwable e)
		{
			getPeerBean().getPeerMap().peerOffline(message.getSender(), true);
			logger.error("Exception in custom handler: " + e.toString());
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * If the message is OK, that has been previously checked by the user using
	 * checkMessage, a reply to the message is generated here.
	 * 
	 * @param ch Channel
	 * @param message Request message
	 * @throws Exception
	 */
	public abstract Message handleResponse(Message message, boolean sign) throws Exception;

	void nullCheck(Object... objs)
	{
		for (Object obj : objs)
			if (obj == null)
				throw new IllegalArgumentException("Object cannot be null");
	}

	public void setSignReply(boolean sign)
	{
		this.sign = sign;
	}

	public PeerBean getPeerBean()
	{
		return peerBean;
	}

	public ConnectionBean getConnectionBean()
	{
		return connectionBean;
	}

}
