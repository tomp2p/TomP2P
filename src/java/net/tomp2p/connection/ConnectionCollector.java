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
import java.util.concurrent.TimeUnit;

import net.tomp2p.message.TomP2PDecoderTCP;
import net.tomp2p.message.TomP2PDecoderUDP;
import net.tomp2p.message.TomP2PEncoderStage1;
import net.tomp2p.message.TomP2PEncoderStage2;
import net.tomp2p.utils.Utils;

import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
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
	final private static ChannelHandler encoder1 = new TomP2PEncoderStage1();
	final private static ChannelHandler encoder2 = new TomP2PEncoderStage2();
	final private Semaphore semaphoreUDPMessages;
	final private Semaphore semaphoreTCPMessages;
	final private static Logger logger = LoggerFactory.getLogger(ConnectionCollector.class);
	final private int maxMessageSize;
	final private ChannelFactory tcpClientChannelFactory;
	final private ChannelFactory udpChannelFactory;
	final private ExecutionHandler executionHandlerSender;

	public ConnectionCollector(ChannelFactory tcpClientChannelFactory,
			ChannelFactory udpChannelFactory, ConnectionConfiguration configuration,
			ExecutionHandler executionHandlerSender)
	{
		this.tcpClientChannelFactory = tcpClientChannelFactory;
		this.udpChannelFactory = udpChannelFactory;
		this.semaphoreUDPMessages = new Semaphore(configuration.getMaxOutgoingUDP());
		this.semaphoreTCPMessages = new Semaphore(configuration.getMaxOutgoingTCP());
		this.maxMessageSize = configuration.getMaxMessageSize();
		this.executionHandlerSender = executionHandlerSender;
	}

	/**
	 * Returns a channel that is managed by this pool. Once the channel is
	 * closed, the channel will be added to the pool.
	 * @param channelChache 
	 * 
	 * @param connectTimeout
	 * 
	 * @param ioHandler The handler that is
	 * @return A channel with the handler or null if disposed or interrupted
	 */
	public ChannelFuture channelTCP(ChannelHandler timeoutHandler, ChannelHandler replyHandler,
			SocketAddress remoteAddress, int connectTimeoutMillis, TCPChannelChache channelChache) throws ChannelException
	{
		try
		{
			while(!semaphoreTCPMessages.tryAcquire(200, TimeUnit.MILLISECONDS))
				channelChache.expireCache();
		}
		catch (InterruptedException e)
		{
			throw new ChannelException(e);
		}
		
		int failCounter = 0;
		for (;;)
		{
			Channel channel;
			synchronized (channelsTCP)
			{
				if (disposeTCP)
				{
					logger.warn("tpc disposed, not returning a channel");
					semaphoreTCPMessages.release();
					throw new ChannelException("tpc disposed, not returning a channel");
				}
				try
				{
					ChannelFuture channelFuture = createChannelTCP(timeoutHandler, replyHandler,
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
							semaphoreTCPMessages.release();
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
						semaphoreTCPMessages.release();
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
						semaphoreTCPMessages.release();
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
			ChannelHandler replyHandler, SocketAddress remoteAddress, SocketAddress localAddress,
			int connectionTimoutMillis)
	{
		ClientBootstrap bootstrap = new ClientBootstrap(tcpClientChannelFactory);
		bootstrap.setOption("connectTimeoutMillis", connectionTimoutMillis);
		// bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("soLinger", 0);
		// bootstrap.setOption("reuseAddress", true);
		// bootstrap.setOption("keepAlive", true);
		setupBootstrap(bootstrap, timeoutHandler, replyHandler,
				new TomP2PDecoderTCP(maxMessageSize));
		return bootstrap.connect(remoteAddress);
	}

	private Channel createChannelUDP(ChannelHandler timeoutHandler, ChannelHandler replyHandler,
			boolean allowBroadcast)
	{
		ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(udpChannelFactory);
		setupBootstrap(bootstrap, timeoutHandler, replyHandler, new TomP2PDecoderUDP());
		// enable per default, as we support a broadcast ping to find other
		// peers.
		bootstrap.setOption("broadcast", allowBroadcast ? true : false);
		bootstrap.setOption("receiveBufferSizePredictor", new FixedReceiveBufferSizePredictor(
				ConnectionHandler.UDP_LIMIT));
		Channel c = bootstrap.bind(new InetSocketAddress(0));
		return c;
	}

	private void setupBootstrap(Bootstrap bootstrap, ChannelHandler timeoutHandler,
			ChannelHandler replyHandler, ChannelUpstreamHandler decoder)
	{
		ChannelPipeline pipe = bootstrap.getPipeline();
		if (timeoutHandler != null)
			pipe.addLast("timeout", timeoutHandler);
		pipe.addLast("encoder2", encoder2);
		pipe.addLast("encoder1", encoder1);
		pipe.addLast("decoder", decoder);
		if (replyHandler != null)
		{
			pipe.addLast("executor", executionHandlerSender);
			pipe.addLast("reply", replyHandler);
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
	// private class CloseHandler extends SimpleChannelUpstreamHandler
	{
		// TODO: add keep alive connections, find out if up or downstream
	}
}
