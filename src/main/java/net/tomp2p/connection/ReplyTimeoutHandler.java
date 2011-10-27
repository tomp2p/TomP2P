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
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.PeerException.AbortCause;
import net.tomp2p.futures.Cancellable;
import net.tomp2p.peers.PeerAddress;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DefaultExceptionEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code inspired by Netty's TimeoutHandlers from Trustin Lee and
 * adapted/extended to a reply timeout handler. Timer starts if write has been
 * called and is reset if a read occurs afterwards. Since we initialize if the
 * channel is open, a TCP timeout will also include the connection attempt.
 */
public class ReplyTimeoutHandler extends SimpleChannelHandler implements Cancellable
{
	final private static Logger logger = LoggerFactory.getLogger(ReplyTimeoutHandler.class);
	private final Timer timer;
	private final long allIdleTimeMillis;
	private final PeerAddress remotePeer;
	private volatile Timeout allIdleTimeout;
	private volatile long lastReadTime;
	private volatile long lastWriteTime;

	public ReplyTimeoutHandler(Timer timer, long timeoutMillis, PeerAddress remotePeer)
	{
		if (timer == null)
			throw new NullPointerException("timer");
		if (timeoutMillis < 0)
			throw new IllegalArgumentException("timout need to be larger than 0");
		this.timer = timer;
		this.allIdleTimeMillis = timeoutMillis;
		this.remotePeer = remotePeer;
	}

	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
	{
		initialize(ctx);
		ctx.sendUpstream(e);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
	{
		lastReadTime = System.currentTimeMillis();
		ctx.sendUpstream(e);
	}

	@Override
	public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception
	{
		if (e.getWrittenAmount() > 0)
			lastWriteTime = System.currentTimeMillis();
		ctx.sendUpstream(e);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
	{
		logger.equals("error in timeout " + e.toString());
		if (logger.isDebugEnabled())
			e.getCause().printStackTrace();
		ctx.sendUpstream(e);
	}

	private void initialize(ChannelHandlerContext ctx)
	{
		lastReadTime = lastWriteTime = System.currentTimeMillis();
		if (allIdleTimeMillis > 0)
		{
			allIdleTimeout = timer.newTimeout(new AllIdleTimeoutTask(ctx), allIdleTimeMillis,
					TimeUnit.MILLISECONDS);
		}
	}
	private final class AllIdleTimeoutTask implements TimerTask
	{
		private final ChannelHandlerContext ctx;

		AllIdleTimeoutTask(ChannelHandlerContext ctx)
		{
			this.ctx = ctx;
		}

		public void run(Timeout timeout) throws Exception
		{
			if (timeout.isCancelled() || !ctx.getChannel().isOpen())
			{
				return;
			}
			long currentTime = System.currentTimeMillis();
			long lastIoTime = Math.max(lastReadTime, lastWriteTime);
			long nextDelay = allIdleTimeMillis - (currentTime - lastIoTime);
			if (nextDelay <= 0)
			{
				try
				{
					ctx.sendUpstream(new DefaultExceptionEvent(ctx.getChannel(), new PeerException(
							AbortCause.TIMEOUT, "Timeout exception for peer " + remotePeer)));
				}
				catch (Throwable t)
				{
					ctx.sendUpstream(new DefaultExceptionEvent(ctx.getChannel(), t));
				}
			}
			else
			{
				// Either read or write occurred before the timeout - set a new
				// timeout with shorter delay.
				allIdleTimeout = timer.newTimeout(this, nextDelay, TimeUnit.MILLISECONDS);
			}
		}

		public void abort() 
		{
			try
			{
				ctx.sendUpstream(new DefaultExceptionEvent(ctx.getChannel(), new PeerException(
						AbortCause.USER_ERROR, "Abort exception for peer " + remotePeer)));
			}
			catch (Throwable t)
			{
				ctx.sendUpstream(new DefaultExceptionEvent(ctx.getChannel(), t));
			}
			
		}
	}

	@Override
	public void cancel()
	{
		if (allIdleTimeout != null)
		{	
			allIdleTimeout.cancel();
		}
		allIdleTimeout = null;
	}

	public void abort() 
	{
		if (allIdleTimeout != null)
		{
			allIdleTimeout.cancel();
			((AllIdleTimeoutTask)allIdleTimeout.getTask()).abort();
		}
		allIdleTimeout = null;
	}
}
