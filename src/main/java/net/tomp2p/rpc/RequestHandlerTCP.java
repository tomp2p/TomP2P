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
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.MessageID;
import net.tomp2p.peers.PeerMap;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
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
				reportFail(msg, ctx.getChannel(), futureResponse);
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
			reportFail(msg, ctx.getChannel(), futureResponse);
			return;
		}
		final Message responseMessage = (Message) e.getMessage();
		MessageID recvMessageID = new MessageID(responseMessage);
		if (responseMessage.getType() == Message.Type.UNKNOWN_ID)
		{
			String msg = "Message was not delivered successfully: " + this.message;
			getPeerMap().peerOffline(futureResponse.getRequest().getRecipient(), true);
			reportFail(msg, ctx.getChannel(), futureResponse);
		}
		else if (responseMessage.getType() == Message.Type.EXCEPTION)
		{
			String msg = "Message caused an exception on the other side, handle as peer_abort: "
					+ this.message;
			reportFail(msg, ctx.getChannel(), futureResponse);
		}
		else if (!sendMessageID.equals(recvMessageID))
		{
			String msg = "Message [" + responseMessage
					+ "] sent to the node is not the same as we expect (TCP). We sent ["
					+ this.message + "]";
			reportFail(msg, ctx.getChannel(), futureResponse);
		}
		else
		{
			if (logger.isDebugEnabled())
				logger.debug("perfect: " + responseMessage);
			// We got a good answer, let's mark the sender as alive
			if (responseMessage.isOk() || responseMessage.isNotOk())
				getPeerMap().peerFound(responseMessage.getSender(), null);
			// connection is closed by other peer
			reportResult(ctx.getChannel(), futureResponse, responseMessage);
		}
	}

	private void reportFail(final String cause, final Channel channel, final FutureResponse futureResponse)
	{
		if (logger.isDebugEnabled()) {
			logger.debug(cause);
		}
		channel.close();
		//this needs to be the last listener added
		channel.getCloseFuture().addListener(new ChannelFutureListener() 
		{
			@Override
			public void operationComplete(ChannelFuture arg0) throws Exception 
			{
				futureResponse.setFailed(cause);
			}
		});	
	}
	
	private void reportResult(final Channel channel, final FutureResponse futureResponse, final Message responseMessage)
	{
		if(message.isKeepAlive())
		{
			futureResponse.setResponse(responseMessage);
		}
		else
		{
			// the other side closes the channel
			// this needs to be the last listener added
			channel.getCloseFuture().addListener(new ChannelFutureListener() 
			{
				@Override
				public void operationComplete(ChannelFuture arg0) throws Exception 
				{
					futureResponse.setResponse(responseMessage);
				}
			});
		}
	}
}