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
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.MessageID;
import net.tomp2p.peers.PeerMap;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Is able to send messages (as a request) and processes incoming replies.
 * 
 * @author Thomas Bocek
 * 
 */
public class RequestHandlerTCP implements ChannelUpstreamHandler
{
	final private static Logger logger = LoggerFactory.getLogger(RequestHandlerTCP.class);
	// The future response which is currently be waited for
	final private FutureResponse futureResponse;
	// The node this request handler is associated with
	final private PeerBean peerBean;
	final private ConnectionBean connectionBean;
	final private Message message;
	final private AtomicBoolean handlingMessage = new AtomicBoolean(false);
	final private MessageID sendMessageID;

	/**
	 * 
	 * @param objectHolder the bean representing the node this handler belongs
	 *        to
	 */
	public RequestHandlerTCP(FutureResponse futureResponse, PeerBean peerBean,
			ConnectionBean connectionBean, Message message)
	{
		this.peerBean = peerBean;
		this.connectionBean = connectionBean;
		this.futureResponse = futureResponse;
		this.message = message;
		this.sendMessageID = new MessageID(message);
	}

	public FutureResponse getFutureResponse()
	{
		return futureResponse;
	}
	
	public FutureResponse sendTCP(ChannelCreator channelCreator)
	{
		return sendTCP(channelCreator, connectionBean.getConfiguration().getIdleTCPMillis());
	}

	public FutureResponse sendTCP(ChannelCreator channelCreator, int idleTCPMillis)
	{
		connectionBean.getSender().sendTCP(this, futureResponse, message, channelCreator, idleTCPMillis);
		return futureResponse;
	}

	public FutureResponse fireAndForgetTCP(ChannelCreator channelCreator)
	{
		connectionBean.getSender().sendTCP(null, futureResponse, message, channelCreator, connectionBean.getConfiguration().getIdleTCPMillis());
		return futureResponse;
	}

	protected PeerMap getPeerMap()
	{
		return peerBean.getPeerMap();
	}
	
	public void setKeepAlive(boolean isKeepAlive)
	{
		message.setKeepAlive(isKeepAlive);
	}

	
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent ce) throws Exception {
		
		if (!(ce instanceof MessageEvent)) 
		{
			//here we get message types such as OPEN, or EXCEPTION
			if (ce instanceof ExceptionEvent)
			{
				String msg = "Exception received: " + ((ExceptionEvent)ce).getCause();
				closeFail(msg, ctx.getChannel(), futureResponse);
				return;
	        }
			else
			{
				ctx.sendUpstream(ce);
				return;
			}
		}
		MessageEvent e=(MessageEvent) ce;
		if (!(e.getMessage() instanceof Message))
		{
			String msg = "Message received, but not of type Message: " + e.getMessage();
			closeFail(msg, ctx.getChannel(), futureResponse);
			return;
		}
		final Message message = (Message) e.getMessage();
		if (handlingMessage.compareAndSet(false, true))
		{
			futureResponse.cancelTimeout();
		}
		MessageID recvMessageID = new MessageID(message);
		if (message.getType() == Message.Type.UNKNOWN_ID)
		{
			String msg = "Message was not delivered successfully: " + this.message;
			getPeerMap().peerOffline(futureResponse.getRequest().getRecipient(), true);
			closeFail(msg, ctx.getChannel(), futureResponse);
		}
		else if (message.getType() == Message.Type.EXCEPTION)
		{
			String msg = "Message caused an exception on the other side, handle as peer_abort: "
					+ this.message;
			closeFail(msg, ctx.getChannel(), futureResponse);
		}
		else if (!sendMessageID.equals(recvMessageID))
		{
			String msg = "Message [" + message
					+ "] sent to the node is not the same as we expect (TCP). We sent ["
					+ this.message + "]";
			closeFail(msg, ctx.getChannel(), futureResponse);
		}
		else
		{
			if (logger.isDebugEnabled())
				logger.debug("perfect: " + message);
			// We got a good answer, let's mark the sender as alive
			if (message.isOk() || message.isNotOk())
				getPeerMap().peerFound(message.getSender(), null);
			// connection is closed by other peer
			futureResponse.setResponse(message);
		}
	}

	public void closeFail(final String cause, final Channel channel, final FutureResponse futureResponse)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug(cause);
		}
		futureResponse.setFailed(cause);
		channel.close();
	}
}