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
package net.tomp2p.connection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.tomp2p.message.Message;
import net.tomp2p.message.MessageID;
import net.tomp2p.rpc.RequestHandlerTCP;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class DispatcherReply extends IdleStateAwareChannelHandler
{
	final private static Logger logger = LoggerFactory.getLogger(DispatcherReply.class);
	final private Map<MessageID, RequestHandlerTCP> waitingForAnswer = new LinkedHashMap<MessageID, RequestHandlerTCP>();
	final private int tcpIdleTimeoutMillis;
	final private DispatcherRequest dispatcherRequest;
	final private ChannelGroup channelGroup;
	//
	final Timer timer;
	private volatile Timeout idleTimeout;
	// private volatile boolean used = false;
	// debug, remove me
	private Channel channel;

	//
	public DispatcherReply(Timer timer, int tcpIdleTimeoutMillis,
			DispatcherRequest dispatcherRequest, ChannelGroup channelGroup)
	{
		this.timer = timer;
		this.tcpIdleTimeoutMillis = tcpIdleTimeoutMillis;
		this.dispatcherRequest = dispatcherRequest;
		this.channelGroup = channelGroup;
		idleTimeout = timer.newTimeout(new TimeoutTask(), tcpIdleTimeoutMillis,
				TimeUnit.MILLISECONDS);
	}

	public void shutdown(String message)
	{
		timeoutAll(message);
		if (idleTimeout != null)
			idleTimeout.cancel();
		idleTimeout = null;
	}

	public void add(Message message, RequestHandlerTCP requestHandler)
	{
		synchronized (waitingForAnswer)
		{
			if (logger.isDebugEnabled())
				logger.debug("adding message " + message);
			waitingForAnswer.put(new MessageID(message), requestHandler);
		}
	}

	@Override
	public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) throws Exception
	{
		logger.info("closing channel (idle)");
		if (ctx.getChannel().isOpen())
			ctx.getChannel().close();
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
	{
		// We only want to receive messages
		if (!(e.getMessage() instanceof Message))
		{
			logger.error("Message received, but not of type Message: " + e.getMessage());
			close(ctx);
			return;
		}
		final Message message = (Message) e.getMessage();
		// check if its a request or reply. This is implemented to have the TCP channel opened both ways
		if (message.isRequest())
		{
			dispatcherRequest.messageReceived(ctx, e);
			return;
		}
		if (logger.isDebugEnabled())
			logger.debug("received reply " + message);
		MessageID messageID = new MessageID(message);
		RequestHandlerTCP requestHandler;
		synchronized (waitingForAnswer)
		{
			requestHandler = waitingForAnswer.remove(messageID);
		}
		if (requestHandler == null)
		{
			logger.warn("Message received, but too late (ignoring): " + e.getMessage());
			return;
		}
		try
		{
			requestHandler.messageReceived(message);
		}
		catch (PeerException pe)
		{
			logger.error("Error in RequestHandler TCP: " + pe.getMessage());
			close(ctx);
			return;
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
	{
		String cause = e.getCause().getMessage() == null ? null : e.getCause().getMessage()
				.toString();
		// do not show connection reset by peer!
		if (!"Connection reset by peer".equals(cause))
		{
			logger.warn("error in dispatcher reply" + e.toString());
			if (logger.isDebugEnabled())
				e.getCause().printStackTrace();
		}
		shutdown(e.toString());
	}

	@Override
	public void channelOpen(final ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
	{
		channel = ctx.getChannel();
		channelGroup.add(ctx.getChannel());
		ctx.sendUpstream(e);
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
	{
		shutdown(e.toString());
		ctx.sendUpstream(e);
	}

	private void timeoutAll(String reason)
	{
		synchronized (waitingForAnswer)
		{
			for (Iterator<Map.Entry<MessageID, RequestHandlerTCP>> iterator = waitingForAnswer
					.entrySet().iterator(); iterator.hasNext();)
			{
				Map.Entry<MessageID, RequestHandlerTCP> entry = iterator.next();
				iterator.remove();
				entry.getValue().getFutureResponse().setFailed(
						"Timeout all: " + reason + " / " + entry.getKey());
			}
		}
	}

	private static void close(ChannelHandlerContext ctx)
	{
		if (!(ctx.getChannel() instanceof DatagramChannel))
			ctx.getChannel().close();
	}
	private final class TimeoutTask implements TimerTask
	{
		public void run(Timeout timeout) throws Exception
		{
			if (timeout.isCancelled())
			{
				return;
			}
			synchronized (waitingForAnswer)
			{
				for (Iterator<Map.Entry<MessageID, RequestHandlerTCP>> iterator = waitingForAnswer
						.entrySet().iterator(); iterator.hasNext();)
				{
					Map.Entry<MessageID, RequestHandlerTCP> entry = iterator.next();
					long now = System.currentTimeMillis();
					long requestTimeout = entry.getValue().getFutureResponse().getReplyTimeout();
					if (now > requestTimeout)
					{
						entry.getValue().getFutureResponse().setFailed(
								"Timeout by " + (now - requestTimeout) + " for "
										+ entry.getValue().getFutureResponse().getRequest());
						iterator.remove();
					}
					else
					{
						long nextDelay = requestTimeout - now;
						idleTimeout = timer.newTimeout(this, nextDelay, TimeUnit.MILLISECONDS);
						return;
					}
				}
			}
			idleTimeout = timer.newTimeout(this, tcpIdleTimeoutMillis, TimeUnit.MILLISECONDS);
		}
	}

	public boolean isWaiting()
	{
		synchronized (waitingForAnswer)
		{
			boolean isWaiting = !waitingForAnswer.isEmpty();
			if (!isWaiting)
			{
				if (logger.isDebugEnabled())
					logger.debug("I'm not waiting " + channel);
			}
			return isWaiting;
		}
	}
	// public void setUsed(boolean used)
	// {
	// this.used = used;
	// }
}
