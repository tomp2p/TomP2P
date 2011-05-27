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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Semaphore;

import net.tomp2p.message.TomP2PDecoderTCP;
import net.tomp2p.message.TomP2PDecoderUDP;
import net.tomp2p.message.TomP2PEncoderTCP;
import net.tomp2p.message.TomP2PEncoderUDP;
import net.tomp2p.utils.Utils;

import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictor;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The connection pool limits the connection used in the application. If too
 * many connections are open, the application has to wait until a connection is
 * closed.
 * 
 * @author Thomas Bocek
 */
public class ConnectionCollector
{
	// always called from synchronized block
	private boolean disposeTCP = false;
	private boolean disposeUDP = false;
	final private ChannelGroup channelsTCP = new DefaultChannelGroup("TomP2P ConnectionPool TCP");
	final private ChannelGroup channelsUDP = new DefaultChannelGroup("TomP2P ConnectionPool UDP");
	final private Semaphore semaphoreUDPMessages;
	final private MySemaphoreTCP semaphoreTCPMessages;
	final private static Logger logger = LoggerFactory.getLogger(ConnectionCollector.class);
	final private int maxMessageSize;
	final private ChannelFactory tcpClientChannelFactory;
	final private ChannelFactory udpChannelFactory;
	final private ExecutionHandler executionHandlerSender;
	final private MessageLogger messageLoggerFilter;

	public ConnectionCollector(ChannelFactory tcpClientChannelFactory,
			ChannelFactory udpChannelFactory, ConnectionConfiguration configuration,
			ExecutionHandler executionHandlerSender,
			 MessageLogger messageLoggerFilter)
	{
		this.tcpClientChannelFactory = tcpClientChannelFactory;
		this.udpChannelFactory = udpChannelFactory;
		this.semaphoreUDPMessages = new Semaphore(configuration.getMaxOutgoingUDP(), true);
		this.semaphoreTCPMessages = new MySemaphoreTCP(configuration.getMaxOutgoingTCP());
		this.maxMessageSize = configuration.getMaxMessageSize();
		this.executionHandlerSender = executionHandlerSender;
		this.messageLoggerFilter = messageLoggerFilter;
	}

	/**
	 * Returns a channel that is managed by this pool. Once the channel is
	 * closed, the channel will be added to the pool.
	 * 
	 * @param channelChache
	 * 
	 * @param connectTimeout
	 * 
	 * @param ioHandler The handler that is
	 * @return A channel with the handler or null if disposed or interrupted
	 * @throws InterruptedException
	 */
	public ChannelFuture channelTCP(ChannelHandler timeoutHandler, ChannelHandler dispatcherReply,
			SocketAddress remoteAddress, int connectTimeoutMillis, TCPChannelCache channelChache)
			throws ChannelException, InterruptedException
	{
		// do this one at a time
		boolean acquired = false;
		long start = System.currentTimeMillis();
		long waitTime = 0;
		synchronized (semaphoreTCPMessages)
		{
			while (!acquired && waitTime < connectTimeoutMillis)
			{
				acquired = semaphoreTCPMessages.tryAcquire();
				if (!acquired)
				{
					channelChache.expireCache();
					waitTime = System.currentTimeMillis() - start;
					//waitTime=0;
					semaphoreTCPMessages.wait(connectTimeoutMillis / 2);
				}
			}
			if (!acquired)
				return null;
		}
		// System.err.println("HERE1:"
		// +semaphoreTCPMessages.availablePermits());
		int failCounter = 0;
		for (;;)
		{
			Channel channel;
			synchronized (channelsTCP)
			{
				if (disposeTCP)
				{
					logger.warn("tpc disposed, not returning a channel");
					synchronized (semaphoreTCPMessages)
					{
						semaphoreTCPMessages.release();
						semaphoreTCPMessages.notifyAll();
					}
					throw new ChannelException("tpc disposed, not returning a channel");
				}
				try
				{
					ChannelFuture channelFuture = createChannelTCP(timeoutHandler, dispatcherReply,
							remoteAddress, new InetSocketAddress(0), connectTimeoutMillis);
					channel = channelFuture.getChannel();
					channelsTCP.add(channel);
					channel.getCloseFuture().addListener(new ChannelFutureListener()
					{
						@Override
						public void operationComplete(ChannelFuture future) throws Exception
						{
							// no need to remove from channel group, as this is
							// already done in channel group,
							// channelsTCP.remove(channelFuture.getChannel());
							synchronized (semaphoreTCPMessages)
							{
								semaphoreTCPMessages.release();
								semaphoreTCPMessages.notifyAll();
							}
						}
					});
					return channelFuture;
				}
				catch (ChannelException ce)
				{
					logger.warn("tried " + failCounter + " times " + ce.toString());
					// wait a bit and try it again
					Utils.sleep(100);
					failCounter++;
					// give up
					if (failCounter > 5)
					{
						logger.error("tried 5 times " + ce.toString());
						ce.printStackTrace();
						synchronized (semaphoreTCPMessages)
						{
							semaphoreTCPMessages.release();
							semaphoreTCPMessages.notifyAll();
						}
						throw ce;
					}
				}
			}
		}
	}

	public Channel channelUDP(ChannelHandler timeoutHandler, ChannelHandler replyHandler,
			boolean allowBroadcast) throws ChannelException
	{
		semaphoreUDPMessages.acquireUninterruptibly();
		// System.err.println("HERE2:"
		// +semaphoreTCPMessages.availablePermits());
		int failCounter = 0;
		for (;;)
		{
			synchronized (channelsUDP)
			{
				if (disposeUDP)
				{
					logger.warn("upd disposed, not returning a channel");
					semaphoreUDPMessages.release();
					throw new ChannelException("upd disposed, not returning a channel");
				}
				try
				{
					Channel channel = createChannelUDP(timeoutHandler, replyHandler, allowBroadcast);
					channelsUDP.add(channel);
					channel.getCloseFuture().addListener(new ChannelFutureListener()
					{
						@Override
						public void operationComplete(ChannelFuture future) throws Exception
						{
							// no need to remove from channel group, as this is
							// already done in channel group,
							// channelsUDP.remove(channel);
							semaphoreUDPMessages.release();
						}
					});
					return channel;
				}
				catch (ChannelException ce)
				{
					logger.warn("tried " + failCounter + " times " + ce.toString());
					// wait a bit and try it again
					Utils.sleep(100);
					failCounter++;
					// give up
					if (failCounter > 5)
					{
						logger.error("tried 5 times " + ce.toString());
						ce.printStackTrace();
						semaphoreUDPMessages.release();
						throw ce;
					}
				}
			}// no need to remove from channel group, as this is already done in
			// channel group,
		}
	}

	/**
	 * Close all open connections and prevent creating new ones.
	 */
	public void shutdown()
	{
		synchronized (channelsTCP)
		{
			disposeTCP = true;
			channelsTCP.close().awaitUninterruptibly();
		}
		synchronized (channelsUDP)
		{
			disposeUDP = true;
			channelsUDP.close().awaitUninterruptibly();
		}
	}

	/**
	 * Creates a new TCP channel the Netty way.
	 * 
	 * @param handler The handler
	 * @return The newly created handler
	 */
	private ChannelFuture createChannelTCP(ChannelHandler timeoutHandler,
			ChannelHandler dispatcherReply, SocketAddress remoteAddress,
			SocketAddress localAddress, int connectionTimoutMillis)
	{
		ClientBootstrap bootstrap = new ClientBootstrap(tcpClientChannelFactory);
		bootstrap.setOption("connectTimeoutMillis", connectionTimoutMillis);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("soLinger", 0);
		// bootstrap.setOption("reuseAddress", true);
		// bootstrap.setOption("keepAlive", true);
		setupBootstrap(bootstrap, timeoutHandler, dispatcherReply, new TomP2PDecoderTCP(maxMessageSize), new TomP2PEncoderTCP(), new ChunkedWriteHandler());
		return bootstrap.connect(remoteAddress);
	}

	private Channel createChannelUDP(ChannelHandler timeoutHandler, ChannelHandler replyHandler,
			boolean allowBroadcast)
	{
		ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(udpChannelFactory);
		setupBootstrap(bootstrap, timeoutHandler, replyHandler, new TomP2PDecoderUDP(), new TomP2PEncoderUDP(), null);
		// enable per default, as we support a broadcast ping to find other
		// peers.
		bootstrap.setOption("broadcast", allowBroadcast ? true : false);
		bootstrap.setOption("receiveBufferSizePredictor", new FixedReceiveBufferSizePredictor(
				ConnectionHandler.UDP_LIMIT));
		Channel c = bootstrap.bind(new InetSocketAddress(0));
		return c;
	}

	private void setupBootstrap(Bootstrap bootstrap, ChannelHandler timeoutHandler,
			ChannelHandler dispatcherReply, ChannelUpstreamHandler decoder, ChannelDownstreamHandler encoder, ChunkedWriteHandler streamer)
	{
		ChannelPipeline pipe = bootstrap.getPipeline();
		if (timeoutHandler != null)
			pipe.addLast("timeout", timeoutHandler);
		if (streamer != null)
			pipe.addLast("streamer", streamer);
		pipe.addLast("encoder", encoder);
		pipe.addLast("decoder", decoder);
		if (messageLoggerFilter != null)
			pipe.addLast("loggerUpstream", messageLoggerFilter);
		if (dispatcherReply != null)
		{
			pipe.addLast("executor", executionHandlerSender);
			pipe.addLast("reply", dispatcherReply);
		}
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("collector enabled = tcp:");
		sb.append(!disposeTCP).append(", upd:").append(!disposeUDP);
		sb.append("; available permits = tcp:").append(semaphoreTCPMessages.availablePermits());
		sb.append(", udp:").append(semaphoreUDPMessages.availablePermits());
		return sb.toString();
	}
	private static class MySemaphoreTCP
	{
		final private int maxPermits;
		private int currentPermits;

		public MySemaphoreTCP(int maxPermits)
		{
			this.maxPermits = maxPermits;
			this.currentPermits = 0;
		}

		public Object availablePermits()
		{
			return maxPermits-currentPermits;
		}

		public void release()
		{
			currentPermits--;
		}

		public boolean tryAcquire()
		{
			if (currentPermits < maxPermits)
			{
				currentPermits++;
				return true;
			}
			else
				return false;
		}
	}
}
