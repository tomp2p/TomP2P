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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ChannelServer;
import net.tomp2p.connection.ChannelServerConficuration;
import net.tomp2p.connection.PipelineFilter;
import net.tomp2p.connection.Reservation;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.utils.Pair;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the connection reservation.
 * 
 * @author Thomas Bocek
 * 
 */
public class TestReservation {
	private static final int PORT = 4000;
	private static final SocketAddress SOCKET_ADDRESS = new InetSocketAddress("127.0.0.1", PORT);

	private ChannelServer cs;

	/**
	 * Starts to processes to listen to port 4000.
	 * 
	 * @throws IOException
	 *             If the process could not be started
	 * @throws InterruptedException
	 */
	@Before
	public void createSink() throws IOException {
		Bindings bindings = new Bindings().addAddress(InetAddress.getByName("127.0.0.1"));
		ChannelServerConficuration c = new ChannelServerConficuration();
		c.interfaceBindings(bindings);
		c.ports(new Ports(PORT, PORT));
		c.pipelineFilter(new MyPipeLine());
		cs = new ChannelServer(c, null, null);
		cs.startup();
	}

	/**
	 * Terminates the "server" part.
	 */
	@After
	public void shutdwon() {
		cs.shutdown();
	}

	/**
	 * Test the TCP connection reservation.
	 * 
	 * @throws InterruptedException .
	 */
	@Test
	public void testReservationTCP() throws InterruptedException {
		EventLoopGroup ev = new NioEventLoopGroup();
		long start = System.currentTimeMillis();
		final int round = 10;
		final int inner = 200;
		final int conn = 50;
		final int tcpMax = 1000;
		for (int i = 0; i < round; i++) {
			ChannelClientConfiguration c = new ChannelClientConfiguration();
			c.maxPermitsTCP(tcpMax);
			c.pipelineFilter(new MyPipeLine());
			Reservation r = new Reservation(ev, c);
			List<FutureChannelCreator> fcc = new ArrayList<FutureChannelCreator>();
			for (int j = 0; j < inner; j++) {
				FutureChannelCreator fc = r.create(0, conn);
				fc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						final ChannelCreator cc = future.getChannelCreator();
						final int timeout = 2000;
						final CountDownLatch countDownLatch = new CountDownLatch(conn);
						for (int k = 0; k < conn; k++) {
							ChannelFuture channelFuture = cc.createTCP(SOCKET_ADDRESS, timeout,
							        new HashMap<String, Pair<EventExecutorGroup, ChannelHandler>>());
							channelFuture.addListener(new GenericFutureListener<ChannelFuture>() {
								@Override
								public void operationComplete(final ChannelFuture future) throws Exception {
									future.channel().close();
									countDownLatch.countDown();
								}
							});
						}
						countDownLatch.await();
						cc.shutdown().awaitUninterruptibly();
					}
				});
				fcc.add(fc);
			}
			for (FutureChannelCreator fcc1 : fcc) {
				fcc1.awaitListeners();
			}
			r.shutdown().awaitUninterruptibly();
		}
		ev.shutdownGracefully().awaitUninterruptibly();
		long time = System.currentTimeMillis() - start;
		long mil = TimeUnit.SECONDS.toMillis(1);
		System.err.println("BENCHMARK: opened and closed " + round + " x " + inner + " x " + conn
		        + " TCP connections to localhost in " + time + " ms. STAT: TCP res open/close per sec:"
		        + ((round * inner * conn * mil) / time));
	}

	/**
	 * Test the TCP connection reservation. This will throw many
	 * "TomP2PSinglePacketUDP - did not get the complete packet" exceptions,
	 * since we are not having any proper headers.
	 * 
	 * @throws InterruptedException .
	 */
	@Test
	public void testReservationUDP() throws InterruptedException {
		EventLoopGroup ev = new NioEventLoopGroup();
		long start = System.currentTimeMillis();
		final int round = 10;
		final int inner = 200;
		final int conn = 50;
		final int udpMax = 1000;
		for (int i = 0; i < round; i++) {
			ChannelClientConfiguration c = new ChannelClientConfiguration();
			c.pipelineFilter(new MyPipeLine());
			c.maxPermitsUDP(udpMax);
			Reservation r = new Reservation(ev, c);
			List<FutureChannelCreator> fcc = new ArrayList<FutureChannelCreator>();
			for (int j = 0; j < inner; j++) {
				FutureChannelCreator fc = r.create(conn, 0);
				fc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						final ChannelCreator cc = future.getChannelCreator();
						final CountDownLatch countDownLatch = new CountDownLatch(conn);
						for (int k = 0; k < conn; k++) {
							ChannelFuture channelFuture = cc.createUDP(SOCKET_ADDRESS, false,
							        new HashMap<String, Pair<EventExecutorGroup, ChannelHandler>>() {
							        });
							channelFuture.addListener(new GenericFutureListener<ChannelFuture>() {
								@Override
								public void operationComplete(final ChannelFuture future) throws Exception {
									future.channel().writeAndFlush(Unpooled.wrappedBuffer(new byte[1]));
									future.channel().close();
									countDownLatch.countDown();
								}
							});
						}
						countDownLatch.await();
						cc.shutdown().awaitUninterruptibly();
					}
				});
				fcc.add(fc);
			}
			for (FutureChannelCreator fcc1 : fcc) {
				fcc1.awaitListeners();
			}
			r.shutdown().awaitUninterruptibly();
		}
		ev.shutdownGracefully().awaitUninterruptibly();
		long time = System.currentTimeMillis() - start;
		long mil = TimeUnit.SECONDS.toMillis(1);
		System.err.println("BENCHMARK: opened and closed " + round + " x " + inner + " x " + conn
		        + " UDP connections to localhost in " + time + " ms. STAT: UDP res open/close per sec:"
		        + ((round * inner * conn * mil) / time));
	}

	/**
	 * Test an unclean shutdown, that means the reservation is shutdown, but the
	 * connectioncreation is not.
	 * 
	 * @throws InterruptedException .
	 */
	@Test
	public void testReservationTCPNonCleanShutdown() throws InterruptedException {
		EventLoopGroup ev = new NioEventLoopGroup();
		long start = System.currentTimeMillis();
		final int round = 50;
		final int inner = 100;
		final int conn = 10;
		final int tcpMax = 1000;
		for (int i = 0; i < round; i++) {
			ChannelClientConfiguration c = new ChannelClientConfiguration();
			c.pipelineFilter(new MyPipeLine());
			c.maxPermitsTCP(tcpMax);
			Reservation r = new Reservation(ev, c);
			List<FutureChannelCreator> fcc = new ArrayList<FutureChannelCreator>();
			for (int j = 0; j < inner; j++) {
				FutureChannelCreator fc = r.create(0, conn);
				fc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						if (future.isFailed()) {
							return;
						}
						final ChannelCreator cc = future.getChannelCreator();
						final int timeout = 2000;
						for (int k = 0; k < conn; k++) {
							ChannelFuture channelFuture = cc.createTCP(SOCKET_ADDRESS, timeout,
							        new HashMap<String, Pair<EventExecutorGroup, ChannelHandler>>() {
							        });
							if (channelFuture == null) {
								return;
							}
							channelFuture.addListener(new GenericFutureListener<ChannelFuture>() {
								@Override
								public void operationComplete(final ChannelFuture future) throws Exception {
									future.channel().close();
								}
							});
						}
					}
				});
				fcc.add(fc);
			}
			for (FutureChannelCreator fcc1 : fcc) {
				fcc1.awaitListeners();
			}
			r.shutdown().awaitUninterruptibly();
		}
		ev.shutdownGracefully().awaitUninterruptibly();
		long time = System.currentTimeMillis() - start;
		long mil = TimeUnit.SECONDS.toMillis(1);
		System.err.println("BENCHMARK: opened and closed " + round + " x " + inner + " x " + conn
		        + " TCP connections to localhost in " + time + " ms. STAT: TCP unclean open/close per sec:"
		        + ((round * inner * conn * mil) / time));
	}

	/**
	 * Unclean shutdown of pending connections.
	 * 
	 * @throws InterruptedException .
	 */
	@Test
	public void testReservationTCPNonCleanShutdown2() throws InterruptedException {
		EventLoopGroup ev = new NioEventLoopGroup();
		final int round = 50;
		final int inner = 100;
		final int conn = 10;
		final int tcpMax = 1000;
		for (int i = 0; i < round; i++) {
			ChannelClientConfiguration c = new ChannelClientConfiguration();
			c.pipelineFilter(new MyPipeLine());
			c.maxPermitsTCP(tcpMax);
			Reservation r = new Reservation(ev, c);
			List<FutureChannelCreator> fcc = new ArrayList<FutureChannelCreator>();
			for (int j = 0; j < inner; j++) {
				FutureChannelCreator fc = r.create(0, conn);
				fc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						if (future.isFailed()) {
							return;
						}
						final ChannelCreator cc = future.getChannelCreator();
						final int timeout = 2000;
						for (int k = 0; k < conn; k++) {
							ChannelFuture channelFuture = cc.createTCP(SOCKET_ADDRESS, timeout,
							        new HashMap<String, Pair<EventExecutorGroup, ChannelHandler>>() {
							        });
							if (channelFuture == null) {
								return;
							}
							channelFuture.addListener(new GenericFutureListener<ChannelFuture>() {
								@Override
								public void operationComplete(final ChannelFuture future) throws Exception {
									future.channel().close();
								}
							});
						}
					}
				});
				fcc.add(fc);
			}
			r.shutdown().awaitUninterruptibly();
		}
		ev.shutdownGracefully().awaitUninterruptibly();
	}

	private static class MyPipeLine implements PipelineFilter {

		@Override
		public void filter(Map<String, Pair<EventExecutorGroup, ChannelHandler>> channelHandlers, boolean tcp,
		        boolean client) {
			for (Iterator<Map.Entry<String, Pair<EventExecutorGroup, ChannelHandler>>> iterator = channelHandlers
			        .entrySet().iterator(); iterator.hasNext();) {
				if (iterator.next().getValue() == null) {
					iterator.remove();
				}
			}

		}

	}

}
