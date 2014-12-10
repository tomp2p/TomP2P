/*
 * Copyright 2013 Thomas Bocek
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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.TomP2PCumulationTCP;
import net.tomp2p.message.TomP2POutbound;
import net.tomp2p.message.TomP2PSinglePacketUDP;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The "server" part that accepts connections.
 * 
 * @author Thomas Bocek
 * 
 */
public final class ChannelServer {

	private static final Logger LOG = LoggerFactory.getLogger(ChannelServer.class);
	// private static final int BACKLOG = 128;

	private final EventLoopGroup bossGroup;
	private final EventLoopGroup workerGroup;

	private Channel channelUDP;
	private Channel channelTCP;

	private final FutureDone<Void> futureServerDone = new FutureDone<Void>();

	// setup
	private final Bindings interfaceBindings;
	
	private final ChannelServerConfiguration channelServerConfiguration;
	private final Dispatcher dispatcher;
	private final List<PeerStatusListener> peerStatusListeners;
	
	private final DropConnectionInboundHandler tcpDropConnectionInboundHandler;
	private final DropConnectionInboundHandler udpDropConnectionInboundHandler;
	private final ChannelHandler udpDecoderHandler;

	/**
	 * Sets parameters and starts network device discovery.
	 * 
	 * @param channelServerConfiguration
	 *            The server configuration, that contains e.g. the handlers
	 * @param dispatcher
	 *            The shared dispatcher
	 * @param peerStatusListeners
	 *            The status listener for offline peers
	 * @throws IOException
	 *             If device discovery failed.
	 */
	public ChannelServer(final EventLoopGroup bossGroup, final EventLoopGroup workerGroup, final ChannelServerConfiguration channelServerConfiguration, final Dispatcher dispatcher,
	        final List<PeerStatusListener> peerStatusListeners) throws IOException {
		this.bossGroup = bossGroup;
		this.workerGroup = workerGroup;
		this.interfaceBindings = channelServerConfiguration.bindingsIncoming();
		this.channelServerConfiguration = channelServerConfiguration;
		this.dispatcher = dispatcher;
		this.peerStatusListeners = peerStatusListeners;
		final String status = DiscoverNetworks.discoverInterfaces(interfaceBindings);
		if (LOG.isInfoEnabled()) {
			LOG.info("Status of interface search: " + status);
		}
		
		this.tcpDropConnectionInboundHandler = new DropConnectionInboundHandler(channelServerConfiguration.maxTCPIncomingConnections());
		this.udpDropConnectionInboundHandler = new DropConnectionInboundHandler(channelServerConfiguration.maxUDPIncomingConnections());
		this.udpDecoderHandler = new TomP2PSinglePacketUDP(channelServerConfiguration.signatureFactory());
	}

	/**
	 * @return The channel server configuration.
	 */
	public ChannelServerConfiguration channelServerConfiguration() {
		return channelServerConfiguration;
	}

	/**
	 * Starts to listen to UDP and TCP ports.
	 * 
	 * @throws IOException
	 *             If the startup fails, e.g, ports already in use
	 */
	public boolean startup() throws IOException {
		if (!channelServerConfiguration.isDisableBind()) {
			final boolean listenAll = interfaceBindings.isListenAll();
			if (listenAll) {
				if (LOG.isInfoEnabled()) {
					LOG.info("Listening for broadcasts on port udp: "
					        + channelServerConfiguration.ports().udpPort() + " and tcp:"
					        + channelServerConfiguration.ports().tcpPort());
				}
				if (!startupTCP(new InetSocketAddress(channelServerConfiguration.ports().tcpPort()),
				        channelServerConfiguration)
				        || !startupUDP(new InetSocketAddress(channelServerConfiguration.ports().udpPort()),
				                channelServerConfiguration)) {
					LOG.warn("cannot bind TCP or UDP");
					return false;
				}

			} else {
				for (InetAddress addr : interfaceBindings.foundAddresses()) {
					if (LOG.isInfoEnabled()) {
						LOG.info("Listening on address: " + addr + " on port udp: "
						        + channelServerConfiguration.ports().udpPort() + " and tcp:"
						        + channelServerConfiguration.ports().tcpPort());
					}
					if (!startupTCP(new InetSocketAddress(addr, channelServerConfiguration.ports().tcpPort()),
					        channelServerConfiguration)
					        || !startupUDP(new InetSocketAddress(addr, channelServerConfiguration.ports()
					                .udpPort()), channelServerConfiguration)) {
						LOG.warn("cannot bind TCP or UDP");
						return false;
					}
				}
			}
		}
		return true;
	}

	/**
	 * Start to listen on a UPD port.
	 * 
	 * @param listenAddresses
	 *            The address to listen to
	 * @param config
	 *            Can create handlers to be attached to this port
	 * @return True if startup was successful
	 */
	boolean startupUDP(final InetSocketAddress listenAddresses, final ChannelServerConfiguration config) {
		Bootstrap b = new Bootstrap();
		b.group(workerGroup);
		b.channel(NioDatagramChannel.class);
		b.option(ChannelOption.SO_BROADCAST, true);
		b.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(ConnectionBean.UDP_LIMIT));

		b.handler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(final Channel ch) throws Exception {
				for (Map.Entry<String, Pair<EventExecutorGroup, ChannelHandler>> entry : handlers(false).entrySet()) {
					if (!entry.getValue().isEmpty()) {
						ch.pipeline().addLast(entry.getValue().element0(), entry.getKey(), entry.getValue().element1());
					} else if (entry.getValue().element1() != null) {
						ch.pipeline().addLast(entry.getKey(), entry.getValue().element1());
					}
				}
			}
		});

		ChannelFuture future = b.bind(listenAddresses);
		channelUDP = future.channel();
		return handleFuture(future);
	}

	/**
	 * Start to listen on a TCP port.
	 * 
	 * @param listenAddresses
	 *            The address to listen to
	 * @param config
	 *            Can create handlers to be attached to this port
	 * @return True if startup was successful
	 */
	boolean startupTCP(final InetSocketAddress listenAddresses, final ChannelServerConfiguration config) {
		ServerBootstrap b = new ServerBootstrap();
		b.group(bossGroup, workerGroup);
		b.channel(NioServerSocketChannel.class);
		b.childHandler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(final Channel ch) throws Exception {
				// b.option(ChannelOption.SO_BACKLOG, BACKLOG);
				bestEffortOptions(ch, ChannelOption.SO_LINGER, 0);
				bestEffortOptions(ch, ChannelOption.TCP_NODELAY, true);
				for (Map.Entry<String, Pair<EventExecutorGroup, ChannelHandler>> entry : handlers(true).entrySet()) {
					if (!entry.getValue().isEmpty()) {
						ch.pipeline().addLast(entry.getValue().element0(), entry.getKey(), entry.getValue().element1());
					} else if (entry.getValue().element1() != null) {
						ch.pipeline().addLast(entry.getKey(), entry.getValue().element1());
					}
				}
			}
		});
		ChannelFuture future = b.bind(listenAddresses);
		channelTCP = future.channel();
		return handleFuture(future);
	}

	private static <T> void bestEffortOptions(final Channel ch, ChannelOption<T> option, T value) {
		try {
			ch.config().setOption(option, value);
		} catch (ChannelException e) {
			// Ignore
		}
	}

	/**
	 * Creates the Netty handlers. After it sends it to the user, where the
	 * handlers can be modified. We add a couple or null handlers where the user
	 * can add its own handler.
	 * 
	 * @param tcp
	 *            Set to true if connection is TCP, false if UDP
	 * @return The channel handlers that may have been modified by the user
	 */
	private Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers(final boolean tcp) {
		TimeoutFactory timeoutFactory = new TimeoutFactory(null, channelServerConfiguration.idleTCPSeconds(),
		        peerStatusListeners, "Server");
		final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;
		if (tcp) {
			final int nrTCPHandlers = 8; // 6 / 0.75 = 7;
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrTCPHandlers);
			handlers.put("dropconnection", new Pair<EventExecutorGroup, ChannelHandler>(null, tcpDropConnectionInboundHandler));
			handlers.put("timeout0",
			        new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutFactory.idleStateHandlerTomP2P()));
			handlers.put("timeout1", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutFactory.timeHandler()));
			handlers.put("decoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2PCumulationTCP(
			        channelServerConfiguration.signatureFactory())));
		} else {
			// we don't need here a timeout since we receive a packet or
			// nothing. It is different than with TCP where we
			// may get a stream and in the middle of it, the other peer goes
			// offline. This cannot happen with UDP
			final int nrUDPHandlers = 6; // 4 = 0.75 = 4 
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrUDPHandlers);
			handlers.put("dropconnection", new Pair<EventExecutorGroup, ChannelHandler>(null, udpDropConnectionInboundHandler));
			handlers.put("decoder", new Pair<EventExecutorGroup, ChannelHandler>(null, udpDecoderHandler));
		}
		handlers.put("encoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2POutbound(false,
		        channelServerConfiguration.signatureFactory())));
		handlers.put("dispatcher", new Pair<EventExecutorGroup, ChannelHandler>(null, dispatcher));
		return channelServerConfiguration.pipelineFilter().filter(handlers, tcp, false);
	}

	/**
	 * Handles the waiting and returning the channel.
	 * 
	 * @param future
	 *            The future to wait for
	 * @return The channel or null if we failed to bind.
	 */
	private boolean handleFuture(final ChannelFuture future) {
		try {
			future.await();
		} catch (InterruptedException e) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("could not start UPD server", e);
			}
			return false;
		}
		boolean success = future.isSuccess();
		if (success) {
			return true;
		} else {
			future.cause().printStackTrace();
			return false;
		}

	}

	/**
	 * Shuts down the server.
	 * 
	 * @return The future when the shutdown is complete. This includes the
	 *         worker and boss event loop
	 */
	public FutureDone<Void> shutdown() {
		final int maxListeners = (channelUDP!=null && channelTCP!=null) ? 2 : 1;
		// we have two things to shut down: UDP and TCP
		final AtomicInteger listenerCounter = new AtomicInteger(0);
		LOG.debug("shutdown servers");
		if(channelUDP != null) {
			channelUDP.close().addListener(new GenericFutureListener<ChannelFuture>() {
				@Override
				public void operationComplete(final ChannelFuture future) throws Exception {
					LOG.debug("shutdown TCP server");
					if(listenerCounter.incrementAndGet()==maxListeners) {
						futureServerDone.done();
					}
				}
			});
		}
		if(channelTCP != null) {
			channelTCP.close().addListener(new GenericFutureListener<ChannelFuture>() {
				@Override
				public void operationComplete(final ChannelFuture future) throws Exception {
					LOG.debug("shutdown TCP channels");
					if(listenerCounter.incrementAndGet()==maxListeners) {
						futureServerDone.done();
					}
				}
			});
		}
		return shutdownFuture();
	}

	/**
	 * @return The shutdown future that is used when calling {@link #shutdown()}
	 */
	public FutureDone<Void> shutdownFuture() {
		return futureServerDone;
	}
}
