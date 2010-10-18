/*
 * Copyright 2009 Red Hat, Inc.
 * 
 * Red Hat licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
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
import static org.jboss.netty.channel.Channels.*;

import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.LifeCycleAwareChannelHandler;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.handler.timeout.DefaultIdleStateEvent;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.util.ExternalResourceReleasable;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Triggers an {@link IdleStateEvent} when a {@link Channel} has not performed
 * read, write, or both operation for a while.
 * 
 * <h3>Supported idle states</h3>
 * <table border="1">
 * <tr>
 * <th>Property</th>
 * <th>Meaning</th>
 * </tr>
 * <tr>
 * <td>{@code readerIdleTime}</td>
 * <td>an {@link IdleStateEvent} whose state is {@link IdleState#READER_IDLE}
 * will be triggered when no read was performed for the specified period of
 * time. Specify {@code 0} to disable.</td>
 * </tr>
 * <tr>
 * <td>{@code writerIdleTime}</td>
 * <td>an {@link IdleStateEvent} whose state is {@link IdleState#WRITER_IDLE}
 * will be triggered when no write was performed for the specified period of
 * time. Specify {@code 0} to disable.</td>
 * </tr>
 * <tr>
 * <td>{@code allIdleTime}</td>
 * <td>an {@link IdleStateEvent} whose state is {@link IdleState#ALL_IDLE} will
 * be triggered when neither read nor write was performed for the specified
 * period of time. Specify {@code 0} to disable.</td>
 * </tr>
 * </table>
 * 
 * <pre>
 * // An example that sends a ping message when there is no outbound traffic
 * // for 30 seconds.  The connection is closed when there is no inbound traffic
 * // for 60 seconds.
 * public class MyPipelineFactory implements {@link ChannelPipelineFactory} {
 *     private final {@link Timer} timer;
 *     public MyPipelineFactory({@link Timer} timer) {
 *         this.timer = timer;
 *     }
 *     public {@link ChannelPipeline} getPipeline() {
 *         return {@link Channels}.pipeline(
 *             <b>new {@link IdleStateHandler}(timer, 60, 30, 0), // timer must be shared.</b>
 *             new MyHandler());
 *     }
 * }
 * // Handler should handle the {@link IdleStateEvent} triggered by {@link IdleStateHandler}.
 * public class MyHandler extends {@link IdleStateAwareChannelHandler} {
 *     {@code @Override}
 *     public void channelIdle({@link ChannelHandlerContext} ctx, {@link IdleStateEvent} e) {
 *         if (e.getState() == {@link IdleState}.READER_IDLE) {
 *             e.getChannel().close();
 *         } else if (e.getState() == {@link IdleState}.WRITER_IDLE) {
 *             e.getChannel().write(new PingMessage());
 *         }
 *     }
 * }
 * {@link ServerBootstrap} bootstrap = ...;
 * {@link Timer} timer = new {@link HashedWheelTimer}();
 * ...
 * bootstrap.setPipelineFactory(new MyPipelineFactory(timer));
 * ...
 * </pre>
 * 
 * The {@link Timer} which was specified when the {@link ReadTimeoutHandler} is
 * created should be stopped manually by calling
 * {@link #releaseExternalResources()} or {@link Timer#stop()} when your
 * application shuts down.
 * 
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @version $Rev: 2224 $, $Date: 2010-03-30 17:02:32 +0900 (Tue, 30 Mar 2010) $
 * 
 * @see ReadTimeoutHandler
 * @see WriteTimeoutHandler
 * 
 * @apiviz.landmark
 * @apiviz.uses org.jboss.netty.util.HashedWheelTimer
 * @apiviz.has org.jboss.netty.handler.timeout.IdleStateEvent oneway - -
 *             triggers
 */
public class IdleStateHandler extends SimpleChannelUpstreamHandler implements
		LifeCycleAwareChannelHandler, ExternalResourceReleasable
{
	final private static Logger logger = LoggerFactory.getLogger(IdleStateHandler.class);
	final Timer timer;
	final long readerIdleTimeMillis;
	volatile long lastReadTime;
	final long writerIdleTimeMillis;
	volatile long lastWriteTime;
	final long allIdleTimeMillis;
	volatile Timeout allIdleTimeout;

	/**
	 * Creates a new instance.
	 * 
	 * @param timer the {@link Timer} that is used to trigger the scheduled
	 *        event. The recommended {@link Timer} implementation is
	 *        {@link HashedWheelTimer}.
	 * @param readerIdleTimeSeconds an {@link IdleStateEvent} whose state is
	 *        {@link IdleState#READER_IDLE} will be triggered when no read was
	 *        performed for the specified period of time. Specify {@code 0} to
	 *        disable.
	 * @param writerIdleTimeSeconds an {@link IdleStateEvent} whose state is
	 *        {@link IdleState#WRITER_IDLE} will be triggered when no write was
	 *        performed for the specified period of time. Specify {@code 0} to
	 *        disable.
	 * @param allIdleTimeSeconds an {@link IdleStateEvent} whose state is
	 *        {@link IdleState#ALL_IDLE} will be triggered when neither read nor
	 *        write was performed for the specified period of time. Specify
	 *        {@code 0} to disable.
	 */
	public IdleStateHandler(Timer timer, int allIdleTimeSeconds)
	{
		this(timer, allIdleTimeSeconds, TimeUnit.SECONDS);
	}

	/**
	 * Creates a new instance.
	 * 
	 * @param timer the {@link Timer} that is used to trigger the scheduled
	 *        event. The recommended {@link Timer} implementation is
	 *        {@link HashedWheelTimer}.
	 * @param readerIdleTime an {@link IdleStateEvent} whose state is
	 *        {@link IdleState#READER_IDLE} will be triggered when no read was
	 *        performed for the specified period of time. Specify {@code 0} to
	 *        disable.
	 * @param writerIdleTime an {@link IdleStateEvent} whose state is
	 *        {@link IdleState#WRITER_IDLE} will be triggered when no write was
	 *        performed for the specified period of time. Specify {@code 0} to
	 *        disable.
	 * @param allIdleTime an {@link IdleStateEvent} whose state is
	 *        {@link IdleState#ALL_IDLE} will be triggered when neither read nor
	 *        write was performed for the specified period of time. Specify
	 *        {@code 0} to disable.
	 * @param unit the {@link TimeUnit} of {@code readerIdleTime}, {@code
	 *        writeIdleTime}, and {@code allIdleTime}
	 */
	public IdleStateHandler(Timer timer, long allIdleTime, TimeUnit unit)
	{
		if (timer == null)
		{
			throw new NullPointerException("timer");
		}
		if (unit == null)
		{
			throw new NullPointerException("unit");
		}
		this.timer = timer;
		readerIdleTimeMillis = 0;
		writerIdleTimeMillis = 0;
		if (allIdleTime <= 0)
		{
			allIdleTimeMillis = 0;
		}
		else
		{
			allIdleTimeMillis = Math.max(unit.toMillis(allIdleTime), 1);
		}
	}

	/**
	 * Stops the {@link Timer} which was specified in the constructor of this
	 * handler. You should not call this method if the {@link Timer} is in use
	 * by other objects.
	 */
	public void releaseExternalResources()
	{
		timer.stop();
	}

	public void beforeAdd(ChannelHandlerContext ctx) throws Exception
	{
		if (ctx.getPipeline().isAttached())
		{
			// channelOpen event has been fired already, which means
			// this.channelOpen() will not be invoked.
			// We have to initialize here instead.
			initialize(ctx);
		}
		else
		{
			// channelOpen event has not been fired yet.
			// this.channelOpen() will be invoked and initialization will occur
			// there.
		}
	}

	public void afterAdd(ChannelHandlerContext ctx) throws Exception
	{
		// NOOP
	}

	public void beforeRemove(ChannelHandlerContext ctx) throws Exception
	{
		destroy();
	}

	public void afterRemove(ChannelHandlerContext ctx) throws Exception
	{
		// NOOP
	}

	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
	{
		// This method will be invoked only if this handler was added
		// before channelOpen event is fired. If a user adds this handler
		// after the channelOpen event, initialize() will be called by
		// beforeAdd().
		initialize(ctx);
		ctx.sendUpstream(e);
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
	{
		destroy();
		ctx.sendUpstream(e);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
	{
		logger.debug("set read " + ctx.getChannel());
		lastReadTime = System.currentTimeMillis();
		ctx.sendUpstream(e);
	}

	@Override
	public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception
	{
		if (e.getWrittenAmount() > 0)
		{
			logger.debug("set write " + ctx.getChannel());
			lastWriteTime = System.currentTimeMillis();
		}
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

	private void destroy()
	{
		if (allIdleTimeout != null)
		{
			allIdleTimeout.cancel();
			allIdleTimeout = null;
		}
	}

	protected void channelIdle(ChannelHandlerContext ctx, IdleState state,
			long lastActivityTimeMillis) throws Exception
	{
		ctx
				.sendUpstream(new DefaultIdleStateEvent(ctx.getChannel(), state,
						lastActivityTimeMillis));
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
				// Both reader and writer are idle - set a new timeout and
				// notify the callback.
				allIdleTimeout = timer.newTimeout(this, allIdleTimeMillis, TimeUnit.MILLISECONDS);
				try
				{
					logger.debug("timeout here " + ctx.getChannel());
					channelIdle(ctx, IdleState.ALL_IDLE, lastIoTime);
				}
				catch (Throwable t)
				{
					t.printStackTrace();
					fireExceptionCaught(ctx, t);
				}
			}
			else
			{
				// Either read or write occurred before the timeout - set a new
				// timeout with shorter delay.
				allIdleTimeout = timer.newTimeout(this, nextDelay, TimeUnit.MILLISECONDS);
			}
		}
	}

	public void reset()
	{
		lastReadTime = lastWriteTime = System.currentTimeMillis();
	}
}
