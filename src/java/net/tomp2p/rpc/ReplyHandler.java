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
	final PeerBean peerBean;
	final ConnectionBean connectionBean;
	private boolean sign = false;

	public ReplyHandler(PeerBean peerBean, ConnectionBean connectionBean)
	{
		this.peerBean = peerBean;
		this.connectionBean = connectionBean;
	}

	protected void registerIoHandler(Command... names)
	{
		connectionBean.getDispatcher().registerIoHandler(peerBean.getServerPeerAddress(), this,
				names);
	}

	// public void removeTemporarly(PeerAddress sender)
	// /{
	// peerBean.getPeerMap().peerOffline(sender);
	// }
	public Message createMessage(PeerAddress recipient, Command name, Type type)
	{
		Message m = new Message();
		return m.setRecipient(recipient).setSender(peerBean.getServerPeerAddress())
				.setCommand(name).setType(type).setVersion(connectionBean.getP2PID());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
	{
		logger.equals("error in reply " + e.toString());
		if (logger.isErrorEnabled())
			e.getCause().printStackTrace();
		// TODO: we never attach the message to the context!
		Message message = (Message) ctx.getAttachment();
		if (message != null)
			peerBean.getPeerMap().peerOffline(message.getSender(), true);
		ctx.getChannel().close();
	}

	public Message forwardMessage(Message message)
	{
		if (checkMessage(message))
		{
			// here we need a referral, since we got contacted and we dont know
			// if we can contact the peer with its address. The peer may be
			// behind a NAT
			peerBean.getPeerMap().peerOnline(message.getSender(), message.getSender());
			try
			{
				Message reply = handleResponse(message);
				if (sign)
					reply.setPublicKeyAndSign(peerBean.getKeyPair());
				return reply;
			}
			catch (Throwable e)
			{
				if (logger.isWarnEnabled())
					logger.error("Exception in custom handler: " + e.toString());
			}
		}
		peerBean.getPeerMap().peerOffline(message.getSender(), true);
		logger.error("Check failed: " + message);
		return null;
	}

	/**
	 * Before a reply can be done, the message needs to be checked. If you
	 * return false, then the peer is removed from the map and the channel is
	 * closed.
	 * 
	 * @param message Request message
	 * @return True if a request should be generated, false if channel should be
	 *         closed and nothing should be replied
	 */
	public abstract boolean checkMessage(Message message);

	/**
	 * If the message is OK, that has been previously checked by the user using
	 * checkMessage, a reply to the message is generated here.
	 * 
	 * @param ch Channel
	 * @param message Request message
	 * @throws Exception
	 */
	public abstract Message handleResponse(Message message) throws Exception;

	void nullCheck(Object... objs)
	{
		for (Object obj : objs)
			if (obj == null)
				throw new IllegalArgumentException("Object cannot be null");
	}

	public void setSign(boolean sign)
	{
		this.sign = sign;
	}

	public boolean isSign()
	{
		return sign;
	}
}
