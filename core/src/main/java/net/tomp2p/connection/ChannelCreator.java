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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the channels. This class is created by {@link Reservation}
 * and should never be called directly. With this class one can create TCP or
 * UDP channels up to a certain extent. Thus it must be know beforehand how much
 * connections will be created.
 * 
 * @author Thomas Bocek
 */
public class ChannelCreator {
	private static final Logger LOG = LoggerFactory.getLogger(ChannelCreator.class);

	private final EventLoopGroup workerGroup;
	private final ChannelGroup recipients = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

	private final int maxPermitsUDP;
	private final int maxPermitsTCP;

	private final Semaphore semaphoreUPD;
	private final Semaphore semaphoreTCP;

	// we should be fair, otherwise we see connection timeouts due to unfairness
	// if busy
	private final ReadWriteLock readWriteLockUDP = new ReentrantReadWriteLock(true);
	private final Lock readUDP = readWriteLockUDP.readLock();
	private final Lock writeUDP = readWriteLockUDP.writeLock();

	private final ReadWriteLock readWriteLockTCP = new ReentrantReadWriteLock(true);
	private final Lock readTCP = readWriteLockTCP.readLock();
	private final Lock writeTCP = readWriteLockTCP.writeLock();

	private final FutureDone<Void> futureChannelCreationDone;

	private final ChannelClientConfiguration channelClientConfiguration;
	
	private final InetAddress sendFromAddress;

	private boolean shutdownUDP = false;
	private boolean shutdownTCP = false;

	/**
	 * Package private constructor, since this is created by
	 * {@link Reservation} and should never be called directly.
	 * 
	 * @param workerGroup
	 *            The worker group for netty that is shared between TCP and UDP.
	 *            This workergroup is not shutdown if this class is shutdown
	 * @param futureChannelCreationDone
	 *            We need to set this from the outside as we want to attach
	 *            listeners to it
	 * @param maxPermitsUDP
	 *            The number of max. parallel UDP connections.
	 * @param maxPermitsTCP
	 *            The number of max. parallel TCP connections.
	 * @param channelClientConfiguration
	 *            The configuration that contains the pipeline filter
	 */
	ChannelCreator(final EventLoopGroup workerGroup, final FutureDone<Void> futureChannelCreationDone,
			int maxPermitsUDP, int maxPermitsTCP,
			final ChannelClientConfiguration channelClientConfiguration, InetAddress sendFromAddress) {
		this.workerGroup = workerGroup;
		this.futureChannelCreationDone = futureChannelCreationDone;
		this.maxPermitsUDP = maxPermitsUDP;
		this.maxPermitsTCP = maxPermitsTCP;
		this.semaphoreUPD = new Semaphore(maxPermitsUDP);
		this.semaphoreTCP = new Semaphore(maxPermitsTCP);
		this.channelClientConfiguration = channelClientConfiguration;
		this.sendFromAddress = sendFromAddress;
	}
        
        static class ChannelCloseListener implements GenericFutureListener<ChannelFuture> {
            
            final private Semaphore semaphore;
            
            private FutureResponse futureResponse;
            private Message responseMessage;
            private Throwable cause;
            private boolean doneMessage = false;
            
            private FutureDone<Void> closeFuture;
            private boolean doneClose = false;
            
            private boolean notified = false;
            
            public ChannelCloseListener() {
                this.semaphore = new Semaphore(1);
            }
            
            public ChannelCloseListener(final Semaphore semaphore) {
                this.semaphore = semaphore;
            }
            
            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                synchronized(this) {
                    semaphore.release();
                    if(!doneMessage) {
                        if(cause == null) {
                            after(futureResponse, responseMessage);
                        } else {
                            failAfter(futureResponse, cause);
                        }
                    }
                    if(!doneClose) {
                        after(closeFuture);
                    }
                    notified = true;
                }
                
            }
            
            public void after(FutureResponse futureResponse, Message responseMessage) {
                synchronized(this) {
                    if(!notified) {
                        this.futureResponse = futureResponse;
                        this.responseMessage = responseMessage;
                    } else {
                        futureResponse.response(responseMessage);
                        doneMessage = true;
                    }
                }
            }
            
            public void failAfter(FutureResponse futureResponse, Throwable cause) {
                synchronized(this) {
                    if(!notified) {
                        this.futureResponse = futureResponse;
                        this.cause = cause;
                    } else {
                        futureResponse.failed(cause);
                        doneMessage = true;
                    }
                }
            }

            public void after(FutureDone<Void> closeFuture) {
                synchronized(this) {
                    if(!notified) {
                        this.closeFuture = closeFuture;
                    } else {
                        closeFuture.done();
                        doneClose = true;
                    }
                }
            }
        }

	/**
	 * Creates a "channel" to the given address. This won't send any message
	 * unlike TCP.
	 * 
	 * @param broadcast
	 *            Sets this channel to be able to broadcast
	 * @param channelHandlers
	 *            The handlers to filter and set
	 * @return The channel future object or null if we are shut down
	 */
	public Pair<ChannelCloseListener, ChannelFuture> createUDP(final SocketAddress socketAddress,  
                final Map<String, ChannelHandler> channelHandlers,
			boolean fireandforget) {
		readUDP.lock();
		try {
			if (shutdownUDP) {
				return null;
			}
			if (!semaphoreUPD.tryAcquire()) {
				final String errorMsg = "Tried to acquire more resources (UDP) than announced.";
				LOG.error(errorMsg);
				throw new RuntimeException(errorMsg);
			}
			final Bootstrap b = new Bootstrap();
			b.group(workerGroup);
			b.channel(NioDatagramChannel.class);
			b.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(ConnectionBean.UDP_LIMIT));
			
			//we don't need to increase the buffers as we limit the connections in tomp2p
			b.option(ChannelOption.SO_RCVBUF, 2 * 1024 * 1024);
			b.option(ChannelOption.SO_SNDBUF, 2 * 1024 * 1024);
			//b.option(ChannelOption.SO_BROADCAST, true);
			addHandlers(b, channelHandlers);
			// Here we need to bind, as opposed to the TCP, were we connect if
			// we do a connect, we cannot receive
			// broadcast messages
			final ChannelFuture channelFuture;
			
			LOG.debug("Create UDP, use from address: {}", sendFromAddress);
			if(fireandforget) {
				channelFuture = b.connect(socketAddress);
			} else {
				channelFuture = b.bind(new InetSocketAddress(sendFromAddress, 0));
			}
                        ChannelCloseListener cl = new ChannelCloseListener(semaphoreUPD);
                        channelFuture.channel().closeFuture().addListener(cl);
			recipients.add(channelFuture.channel());
			return new Pair<ChannelCloseListener, ChannelFuture>(cl, channelFuture);
		} finally {
			readUDP.unlock();
		}
	}

	/**
	 * Creates a channel to the given address. This will setup the TCP
	 * connection
	 * 
	 * @param socketAddress
	 *            The address to send future messages
	 * @param connectionTimeoutMillis
	 *            The timeout for establishing a TCP connection
	 * @param channelHandlers
	 *            The handlers to filter and set
	 * @param futureResponse
	 *            the futureResponse
	 * @return The channel future object or null if we are shut down.
	 */
	public Pair<ChannelCloseListener, ChannelFuture> createTCP(final SocketAddress socketAddress, final int connectionTimeoutMillis,
			final Map<String, ChannelHandler> channelHandlers) {
		readTCP.lock();
		try {
			if (shutdownTCP) {
				return null;
			}
			if (!semaphoreTCP.tryAcquire()) {
				final String errorMsg = "Tried to acquire more resources (TCP) than announced.";
				LOG.error(errorMsg);
				throw new RuntimeException(errorMsg);
			}
			Bootstrap b = new Bootstrap();
			b.group(workerGroup);
			b.channel(NioSocketChannel.class);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeoutMillis);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_LINGER, 0);
			b.option(ChannelOption.SO_REUSEADDR, true);
			//b.option(ChannelOption.SO_RCVBUF, 2 * 1024 * 1024);
			//b.option(ChannelOption.SO_SNDBUF, 2 * 1024 * 1024);
			addHandlers(b, channelHandlers);
			
			LOG.debug("Create TCP, use from address: {}", sendFromAddress);

			ChannelFuture channelFuture = b.connect(socketAddress, new InetSocketAddress(sendFromAddress, 0));
                        ChannelCloseListener cl = new ChannelCloseListener(semaphoreTCP);
                        channelFuture.channel().closeFuture().addListener(cl);
			recipients.add(channelFuture.channel());
			return new Pair<ChannelCloseListener, ChannelFuture>(cl, channelFuture);
		} finally {
			readTCP.unlock();
		}
	}

	/**
	 * Since we want to add multiple handlers, we need to do this with the
	 * pipeline.
	 * 
	 * @param bootstrap
	 *            The bootstrap
	 * @param channelHandlers
	 *            The handlers to be added.
	 */
	private void addHandlers(final Bootstrap bootstrap, final Map<String, ChannelHandler> channelHandlers) {
		bootstrap.handler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(final Channel ch) throws Exception {
				ch.config().setAllocator(channelClientConfiguration.byteBufAllocator());
				for (Map.Entry<String, ChannelHandler> entry : channelHandlers.entrySet()) {
                                    ch.pipeline().addLast(entry.getKey(), entry.getValue());
				}
			}
		});
	}

	public boolean isShutdown() {
		return shutdownTCP || shutdownUDP;
	}

	/**
	 * Shuts down this channel creator. This means that no more TCP or UDP connections
	 * can be established.
	 * 
	 * @return The shutdown future.
	 */
	public FutureDone<Void> shutdown() {
		// set shutdown flag for UDP and TCP
        // if we acquire a write lock, all read locks are blocked as well
                writeUDP.lock();
		writeTCP.lock();
		try {
			if (shutdownTCP || shutdownUDP) {
				shutdownFuture().failed("already shutting down");
				return shutdownFuture();
			}
			shutdownUDP = true;
			shutdownTCP = true;
		} finally {
			writeTCP.unlock();
			writeUDP.unlock();
		}

		recipients.close().addListener(new GenericFutureListener<ChannelGroupFuture>() {
			@Override
			public void operationComplete(final ChannelGroupFuture future) throws Exception {
				// we can block here as we block in GlobalEventExecutor.INSTANCE
				semaphoreUPD.acquireUninterruptibly(maxPermitsUDP);
				semaphoreTCP.acquireUninterruptibly(maxPermitsTCP);
				shutdownFuture().done();
			}
		});

		return shutdownFuture();
	}

	/**
	 * @return The shutdown future that is used when calling {@link #shutdown()}
	 */
	public FutureDone<Void> shutdownFuture() {
		return futureChannelCreationDone;
	}

	public int availableUDPPermits() {
		return semaphoreUPD.availablePermits();
	}

	public int availableTCPPermits() {
		return semaphoreTCP.availablePermits();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("sem-udp:");
		sb.append(semaphoreUPD.availablePermits());
		sb.append(",sem-tcp:");
		sb.append(semaphoreTCP.availablePermits());
		sb.append(",addrUDP:");
		sb.append(semaphoreUPD);
		return sb.toString();
	}
}
