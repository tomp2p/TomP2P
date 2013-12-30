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
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

	// important to keep them low, since a too high value results in connection
	// degradation
	private final EventLoopGroup bossGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() / 2,
	        new DefaultThreadFactory(ConnectionBean.THREAD_NAME + "boss - "));
	private final EventLoopGroup workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() / 2,
	        new DefaultThreadFactory(ConnectionBean.THREAD_NAME + "worker-server - "));

	private Channel channelUDP;
	private Channel channelTCP;

	private final FutureDone<Void> futureServerDone = new FutureDone<Void>();

	// setup
	private final Bindings interfaceBindings;
	private final Ports ports;

	private final ChannelServerConficuration channelServerConfiguration;
	private final Dispatcher dispatcher;
	private final PeerStatusListener[] peerStatusListeners;

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
	public ChannelServer(final ChannelServerConficuration channelServerConfiguration, final Dispatcher dispatcher,
	        final PeerStatusListener[] peerStatusListeners) throws IOException {
		this.interfaceBindings = channelServerConfiguration.interfaceBindings();
		this.ports = channelServerConfiguration.ports();
		this.channelServerConfiguration = channelServerConfiguration;
		this.dispatcher = dispatcher;
		this.peerStatusListeners = peerStatusListeners;
		final String status = DiscoverNetworks.discoverInterfaces(interfaceBindings);
		if (LOG.isInfoEnabled()) {
			LOG.info("Status of interface search: " + status);
		}
	}

	/**
	 * @return The binding that was used to setup the incoming connections
	 */
	public Ports ports() {
		return ports;
	}

	/**
	 * @return The channel server configuration.
	 */
	public ChannelServerConficuration channelServerConfiguration() {
		return channelServerConfiguration;
	}

	/**
	 * Starts to listen to UDP and TCP ports.
	 * 
	 * @throws IOException
	 *             If the startup fails, e.g, ports already in use
	 */
	public void startup() throws IOException {
		if (!channelServerConfiguration.disableBind()) {
			final boolean listenAll = interfaceBindings.isListenAll();
			if (listenAll) {
				if (LOG.isInfoEnabled()) {
					LOG.info("Listening for broadcasts on port udp: " + ports.externalUDPPort() + " and tcp:"
					        + ports.externalTCPPort());
				}
				if (!startupTCP(new InetSocketAddress(ports.externalTCPPort()), channelServerConfiguration)
				        || !startupUDP(new InetSocketAddress(ports.externalUDPPort()), channelServerConfiguration)) {
					throw new IOException("cannot bind TCP or UDP");
				}

			} else {
				for (InetAddress addr : interfaceBindings.foundAddresses()) {
					if (LOG.isInfoEnabled()) {
						LOG.info("Listening on address: " + addr + " on port udp: " + ports.externalUDPPort()
						        + " and tcp:" + ports.externalTCPPort());
					}
					if (!startupTCP(new InetSocketAddress(addr, ports.externalTCPPort()), channelServerConfiguration)
					        || !startupUDP(new InetSocketAddress(addr, ports.externalUDPPort()),
					                channelServerConfiguration)) {
						throw new IOException("cannot bind TCP or UDP");
					}
				}
			}
		}
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
	boolean startupUDP(final InetSocketAddress listenAddresses, final ChannelServerConficuration config) {
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
	boolean startupTCP(final InetSocketAddress listenAddresses, final ChannelServerConficuration config) {
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
			final int nrTCPHandlers = 5;
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrTCPHandlers);
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
			final int nrUDPHandlers = 3;
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrUDPHandlers);
			handlers.put("decoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2PSinglePacketUDP(
			        channelServerConfiguration.signatureFactory())));
		}
		handlers.put("encoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2POutbound(false,
		        channelServerConfiguration.signatureFactory())));
		handlers.put("dispatcher", new Pair<EventExecutorGroup, ChannelHandler>(null, dispatcher));
		channelServerConfiguration.pipelineFilter().filter(handlers, tcp, false);
		return handlers;
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
		LOG.debug("shutdown UPD server");
		channelUDP.close().addListener(new GenericFutureListener<ChannelFuture>() {
			@Override
			public void operationComplete(final ChannelFuture future) throws Exception {
				LOG.debug("shutdown TCP server");
				channelTCP.close().addListener(new GenericFutureListener<ChannelFuture>() {
					@SuppressWarnings({ "unchecked", "rawtypes" })
					@Override
					public void operationComplete(final ChannelFuture future) throws Exception {
						LOG.debug("shutdown TCP workergroup");
						workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).addListener(new GenericFutureListener() {
							@Override
							public void operationComplete(final Future future) throws Exception {
								LOG.debug("shutdown TCP workergroup done!");
								bossGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).addListener(
								        new GenericFutureListener() {
									        @Override
									        public void operationComplete(final Future future) throws Exception {
										        futureServerDone.setDone();
									        }
								        });
							}
						});
					}
				});
			}
		});
		return shutdownFuture();
	}

	/**
	 * @return The shutdown future that is used when calling {@link #shutdown()}
	 */
	public FutureDone<Void> shutdownFuture() {
		return futureServerDone;
	}
}
