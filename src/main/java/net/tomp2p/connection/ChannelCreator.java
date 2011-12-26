/*
 * Copyright 2011 Thomas Bocek
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.TomP2PDecoderTCP;
import net.tomp2p.message.TomP2PDecoderUDP;
import net.tomp2p.message.TomP2PEncoderTCP;
import net.tomp2p.message.TomP2PEncoderUDP;
import net.tomp2p.p2p.Statistics;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.rpc.RequestHandlerUDP;

import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictor;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;

/**
 * Creates the channels. This class is created by {@link ConnectionReservation}
 * and should never be called directly.
 * 
 * @author Thomas Bocek
 * 
 */
public class ChannelCreator
{
	final private Semaphore connectionSemaphore;
	final private ChannelGroup channelsTCP = new DefaultChannelGroup("TomP2P ConnectionPool TCP");
	final private ChannelGroup channelsUDP = new DefaultChannelGroup("TomP2P ConnectionPool UDP");
	// objects needed to create the connection
	final private MessageLogger messageLoggerFilter;
	final private ChannelFactory tcpClientChannelFactory;
	final private ChannelFactory udpChannelFactory;
	// indicates if the TCP connections are kept alive
	final private boolean keepAliveAndReuse;
	final private Map<InetSocketAddress, ChannelFuture> cacheMap;
	final private Statistics statistics;
	private volatile boolean shutdown;
	private volatile AtomicInteger permitsCount;
	//
	final private Set<FutureResponse> active = Collections.synchronizedSet(new HashSet<FutureResponse>());

	/**
	 * Package private constructor, since this is created by
	 * {@link ConnectionReservation} and should never be called directly.
	 * 
	 * @param permits The number of max. parallel connections.
	 * @param statistics The class that counts the created TCP and UDP
	 *        connections.
	 * @param messageLoggerFilter
	 * @param tcpClientChannelFactory
	 * @param udpClientChannelFactory
	 * @param keepAliveAndReuse
	 */
	ChannelCreator(int permits, Statistics statistics,
			MessageLogger messageLoggerFilter, ChannelFactory tcpClientChannelFactory,
			ChannelFactory udpClientChannelFactory, boolean keepAliveAndReuse)
	{
		this.permitsCount = new AtomicInteger(permits);
		this.connectionSemaphore = new Semaphore(permits);
		this.cacheMap = new ConcurrentHashMap<InetSocketAddress, ChannelFuture>(permits);
		this.messageLoggerFilter = messageLoggerFilter;
		this.tcpClientChannelFactory = tcpClientChannelFactory;
		this.udpChannelFactory = udpClientChannelFactory;
		this.keepAliveAndReuse = keepAliveAndReuse;
		this.statistics = statistics;
	}

	/**
	 * Creates a UDP channel.
	 * 
	 * @param timeoutHandler The handler that deals with timeouts
	 * @param requestHandler The handler that deals with incoming replies
	 * @param futureResponse The future object that takes care of future events
	 * @param broadcast Set to true if broadcast is allowed
	 * @return The created channel or null if we are shutting down.
	 */
	public Channel createUDPChannel(ReplyTimeoutHandler timeoutHandler,
			RequestHandlerUDP requestHandler, final FutureResponse futureResponse, boolean broadcast)
	{
		if (shutdown)
		{
			return null;
		}
		// If we are out of semaphores, we cannot create any channels. Since we
		// know how many channels max. in parallel are created, we can reserve
		// it.
		if (!connectionSemaphore.tryAcquire())
		{
			throw new RuntimeException("you ran out of permits. You had " + permitsCount
					+ " available, but now its down to 0");
		}
		statistics.incrementUDPChannelCreation();
		// now, we don't exceeded the limits, so create channels
		Channel channel;
		try
		{
			channel = createChannelUDP(timeoutHandler, requestHandler, broadcast);
		}
		catch (Exception e)
		{
			futureResponse.setFailed("Cannot create channel " + e);
			connectionSemaphore.release();
			statistics.decrementUDPChannelCreation();
			return null;
		}

		synchronized (this)
		{
			if (shutdown)
			{
				channel.close().awaitUninterruptibly();
				futureResponse.setFailed("shutdown in progres (ChannelCreator/UDP)");
				connectionSemaphore.release();
				statistics.decrementUDPChannelCreation();
				return null;
			}
			channelsUDP.add(channel);
			//TODO:DBX debug why it hangs here
			active.add(futureResponse);
		}

		channel.getCloseFuture().addListener(new ChannelFutureListener()
		{
			@Override
			public void operationComplete(ChannelFuture future) throws Exception
			{
				connectionSemaphore.release();
				statistics.decrementUDPChannelCreation();
				//TODO:DBX debug why it hangs here
				active.remove(futureResponse);
			}
		});
		return channel;
	}

	/**
	 * Creates a TCP channel future. Once the future finishes, the channel can
	 * be used to connect to peers.
	 * 
	 * @param timeoutHandler The handler that deals with timeouts
	 * @param requestHandler The handler that deals with incoming replies
	 * @param futureResponse The future object that takes care of future events
	 * @param connectTimeoutMillis The timeout after which a connection attempt
	 *        is considered a failure
	 * @param recipient The recipient to create the connection. If the recipient
	 *        is already open, the connection will be reused.
	 * @return The channel future or null if we are shutting down.
	 */
	public ChannelFuture createTCPChannel(ReplyTimeoutHandler timeoutHandler,
			RequestHandlerTCP requestHandler,
			final FutureResponse futureResponse, int connectTimeoutMillis,
			final InetSocketAddress recipient)
	{
		if (shutdown)
		{
			return null;
		}
		ChannelFuture channelFuture;
		boolean newConnection = true;
		if (keepAliveAndReuse)
		{
			channelFuture = cacheMap.get(recipient);
			if (channelFuture == null)
			{
				// If we are out of semaphores, we cannot create any channels.
				// Since we know how many channels max. in parallel are created,
				// we can reserve it.
				if (!connectionSemaphore.tryAcquire())
				{
					throw new RuntimeException("you ran out of permits. You had " + permitsCount
							+ " available, but now its down to 0");
				}
				statistics.incrementTCPChannelCreation();
				// now, we don't exceeded the limits, so create channels
				channelFuture = createChannelTCP(timeoutHandler, requestHandler,
						recipient, new InetSocketAddress(0), connectTimeoutMillis);
				cacheMap.put(recipient, channelFuture);
			}
			else
			{
				newConnection = false;
				ReplyTimeoutHandler oldTimoutHandler = (ReplyTimeoutHandler) channelFuture
						.getChannel().getPipeline().replace("timeout", "timeout", timeoutHandler);
				// abort the old timeouthandler. If we have not dealt with it
				// (should not happen), then abort and throw exception
				oldTimoutHandler.cancel();
				channelFuture.getChannel().getPipeline()
						.replace("request", "request", requestHandler);
				// we need a new RequestHandlerTCP in order for the new message
			}
		}
		else
		{
			// If we are out of semaphores, we cannot create any channels. Since
			// we know how many channels max. in parallel are created, we can
			// reserve it.
			if (!connectionSemaphore.tryAcquire())
			{
				throw new RuntimeException("you ran out of permits. You had " + permitsCount
						+ " available, but now its down to 0");
			}
			statistics.incrementTCPChannelCreation();
			try
			{
				channelFuture = createChannelTCP(timeoutHandler, requestHandler,
						recipient, new InetSocketAddress(0), connectTimeoutMillis);
			}
			catch (Exception e)
			{
				futureResponse.setFailed("Cannot create channel " + e);
				connectionSemaphore.release();
				statistics.decrementTCPChannelCreation();
				return null;
			}
		}
		final Channel channel = channelFuture.getChannel();

		synchronized (this)
		{
			if (shutdown)
			{
				channel.close().awaitUninterruptibly();
				futureResponse.setFailed("shutdown in progres (ChannelCreator/TCP)");
				connectionSemaphore.release();
				statistics.decrementTCPChannelCreation();
				return null;
			}
			channelsTCP.add(channel);
			//TODO:DBX debug why it hangs here
			active.add(futureResponse);
		}

		if (newConnection)
		{
			channel.getCloseFuture().addListener(new ChannelFutureListener()
			{
				@Override
				public void operationComplete(ChannelFuture future) throws Exception
				{
					connectionSemaphore.release();
					statistics.decrementTCPChannelCreation();
					if (keepAliveAndReuse)
					{
						cacheMap.remove(recipient);
					}
					//TODO:DBX debug why it hangs here
					active.remove(futureResponse);
				}
			});
		}
		return channelFuture;
	}

	/**
	 * Creates a channel the Netty way. We set soLinger to 0 since we may end up
	 * with too many connections in a WAIT state. Setting soLinger to 0 sends
	 * back an RST in case of a close, which may get an exception
	 * "connection reset by peer".
	 * 
	 * @param timeoutHandler The handler that deals with timeouts
	 * @param requestHandler The handler that deals with incoming replies
	 * @param remoteAddress The remote address we connect to
	 * @param localAddress The local address we bind to
	 * @param connectionTimoutMillis The timeout after which a connection
	 *        attempt is considered a failure
	 * @return The channel future
	 */
	private ChannelFuture createChannelTCP(ChannelHandler timeoutHandler,
			ChannelHandler requestHandler, SocketAddress remoteAddress,
			SocketAddress localAddress, int connectionTimoutMillis)
	{
		ClientBootstrap bootstrap = new ClientBootstrap(tcpClientChannelFactory);
		bootstrap.setOption("connectTimeoutMillis", connectionTimoutMillis);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("soLinger", 0);
		bootstrap.setOption("reuseAddress", true);
		bootstrap.setOption("keepAlive", true);
		setupBootstrapTCP(bootstrap, timeoutHandler, requestHandler, new TomP2PDecoderTCP(),
				new TomP2PEncoderTCP(), new ChunkedWriteHandler(), messageLoggerFilter);
		ChannelFuture channelFuture = bootstrap.connect(remoteAddress);
		return channelFuture;
	}

	/**
	 * Creates a channel the Netty way. We need to set the receive buftfer,
	 * since we need to reserve enough space and the default 786 bytes is not
	 * enough.
	 * 
	 * @param timeoutHandler The handler that deals with timeouts
	 * @param requestHandler The handler that deals with incoming replies
	 * @param allowBroadcast Set to true if broadcast is allowed
	 * @return The channel
	 */
	private Channel createChannelUDP(ChannelHandler timeoutHandler, ChannelHandler requestHandler,
			boolean allowBroadcast)
	{
		ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(udpChannelFactory);
		setupBootstrapUDP(bootstrap, timeoutHandler, requestHandler, new TomP2PDecoderUDP(),
				new TomP2PEncoderUDP(), messageLoggerFilter);
		// enable per default, as we support a broadcast ping to find other
		// peers.
		bootstrap.setOption("broadcast", allowBroadcast ? true : false);
		bootstrap.setOption("receiveBufferSizePredictor", new FixedReceiveBufferSizePredictor(
				ConnectionHandler.UDP_LIMIT));
		Channel c = bootstrap.bind(new InetSocketAddress(0));
		return c;
	}

	/**
	 * Fill the TCP pipeline with handlers. This pipeline contains the streamer,
	 * which the UDP version doesnt.
	 * 
	 * @param bootstrap The bootstrap object with settings
	 * @param timeoutHandler The handler that deals with timeouts
	 * @param requestHandler The handler that deals with incoming replies
	 * @param decoder The message decoder that converts from a Netty byte buffer
	 *        to an {@link Message} object
	 * @param encoder The message encoder that converts from a {@link Message}
	 *        object to a Netty byte buffer
	 * @param streamer The chunk streamer that deals with partial data.
	 * @param messageLoggerFilter The handler to log what was sent over the wire
	 */
	private static void setupBootstrapTCP(Bootstrap bootstrap, ChannelHandler timeoutHandler,
			ChannelHandler requestHandler, ChannelUpstreamHandler decoder,
			ChannelDownstreamHandler encoder, ChunkedWriteHandler streamer,
			ChannelHandler messageLoggerFilter)
	{
		ChannelPipeline pipe = bootstrap.getPipeline();
		if (timeoutHandler != null)
		{
			pipe.addLast("timeout", timeoutHandler);
		}
		pipe.addLast("streamer", streamer);
		pipe.addLast("encoder", encoder);
		pipe.addLast("decoder", decoder);
		if (messageLoggerFilter != null)
		{
			pipe.addLast("loggerUpstream", messageLoggerFilter);
		}
		if (requestHandler != null)
		{
			pipe.addLast("request", requestHandler);
		}
	}

	/**
	 * Fill the TCP pipeline with handlers. . This pipeline does not contains
	 * the streamer, which the UDP version does.
	 * 
	 * @param bootstrap The bootstrap object with settings
	 * @param timeoutHandler The handler that deals with timeouts
	 * @param requestHandler The handler that deals with incoming replies
	 * @param decoder The message decoder that converts from a Netty byte buffer
	 *        to an {@link Message} object
	 * @param encoder The message encoder that converts from a {@link Message}
	 *        object to a Netty byte buffer
	 * @param messageLoggerFilter The handler to log what was sent over the wire
	 */
	private static void setupBootstrapUDP(Bootstrap bootstrap, ChannelHandler timeoutHandler,
			ChannelHandler requestHandler, ChannelUpstreamHandler decoder,
			ChannelDownstreamHandler encoder, ChannelHandler messageLoggerFilter)
	{
		ChannelPipeline pipe = bootstrap.getPipeline();
		if (timeoutHandler != null)
			pipe.addLast("timeout", timeoutHandler);
		pipe.addLast("encoder", encoder);
		pipe.addLast("decoder", decoder);
		if (messageLoggerFilter != null)
			pipe.addLast("loggerUpstream", messageLoggerFilter);
		if (requestHandler != null)
		{
			pipe.addLast("request", requestHandler);
		}
	}

	/**
	 * Closes a permanent connection. If no connection existent, then this
	 * method returns
	 * 
	 * @param destination The address of the destination peer of the permanent
	 *        connection.
	 * @return The ChannelFuture of the close operation or null if the
	 *         connection was not in the cached map.
	 */
	public ChannelFuture close(PeerAddress destination)
	{
		ChannelFuture channelFuture = cacheMap.get(destination);
		if (channelFuture != null)
		{
			return channelFuture.getChannel().close();
		}
		return null;
	}

	/**
	 * @return The number of permits, which is the max. number of allowed
	 *         parallel connections
	 */
	public int getPermits()
	{
		return permitsCount.get();
	}

	/**
	 * Releases permits. This can also be a partial release
	 * 
	 * @param permits The number of permits to be released
	 */
	public void release(int permits)
	{
		connectionSemaphore.release(permits);
		int result = permitsCount.addAndGet(-permits);
		if(result < 0)
		{
			throw new RuntimeException("Cannot release more than I acquired");
		}
		
	}

	/**
	 * @return Returns true if the channel creator has no permits anymore
	 */
	public boolean hasNoPermits()
	{
		return permitsCount.compareAndSet(0, 0);
	}

	/**
	 * Shuts down this channelcreator. That means a flag is set and if a
	 * connection should be created, null is returned.
	 */
	public void shutdown()
	{
		synchronized (this)
		{
			shutdown = true;
			channelsTCP.close().awaitUninterruptibly();
			channelsUDP.close().awaitUninterruptibly();
		}
	}
}