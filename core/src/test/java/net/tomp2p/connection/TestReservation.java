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
import io.netty.util.concurrent.DefaultThreadFactory;
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

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.PeerBuilder;
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
	
	private EventLoopGroup workerGroup;
	
	@Before
	public void createSink() throws IOException {
		Bindings bindings = new Bindings().addAddress(InetAddress.getByName("127.0.0.1"));
		ChannelServerConficuration c = new ChannelServerConficuration();
		c.bindingsIncoming(bindings);
		c.ports(new Ports(PORT, PORT));
		c.pipelineFilter(new MyPipeLine());
		final EventLoopGroup bossGroup = new NioEventLoopGroup(0,
    	        new DefaultThreadFactory(ConnectionBean.THREAD_NAME + "boss - "));
    	workerGroup = new NioEventLoopGroup(0,
    	        new DefaultThreadFactory(ConnectionBean.THREAD_NAME + "worker-server - "));
		cs = new ChannelServer(bossGroup, workerGroup, c, null, null);
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
		long start = System.currentTimeMillis();
		final int round = 10;
		final int inner = 200;
		final int conn = 50;
		final int tcpMax = 1000;
		for (int i = 0; i < round; i++) {
			ChannelClientConfiguration c = PeerBuilder.createDefaultChannelClientConfiguration();
			c.maxPermitsTCP(tcpMax);
			c.pipelineFilter(new MyPipeLine());
			Reservation r = new Reservation(workerGroup, c);
			List<FutureChannelCreator> fcc = new ArrayList<FutureChannelCreator>();
			for (int j = 0; j < inner; j++) {
				FutureChannelCreator fc = r.create(0, conn);
				fc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						final ChannelCreator cc = future.channelCreator();
						final int timeout = 2000;
						final CountDownLatch countDownLatch = new CountDownLatch(conn);
						for (int k = 0; k < conn; k++) {
							ChannelFuture channelFuture = cc.createTCP(SOCKET_ADDRESS, timeout,
							        new HashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(), new FutureResponse(null));
							channelFuture.addListener(new GenericFutureListener<ChannelFuture>() {
								@Override
								public void operationComplete(final ChannelFuture future) throws Exception {
									future.channel().close();
									countDownLatch.countDown();
								}
							});
						}
						countDownLatch.await();
						cc.shutdown().awaitListenersUninterruptibly();
					}
				});
				fcc.add(fc);
			}
			for (FutureChannelCreator fcc1 : fcc) {
				fcc1.awaitListeners();
			}
			r.shutdown().awaitUninterruptibly();
		}
		//ev.shutdownGracefully().awaitUninterruptibly();
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
		long start = System.currentTimeMillis();
		final int round = 10;
		final int inner = 200;
		final int conn = 50;
		final int udpMax = 1000;
		for (int i = 0; i < round; i++) {
			ChannelClientConfiguration c = PeerBuilder.createDefaultChannelClientConfiguration();
			c.pipelineFilter(new MyPipeLine());
			c.maxPermitsUDP(udpMax);
			Reservation r = new Reservation(workerGroup, c);
			List<FutureChannelCreator> fcc = new ArrayList<FutureChannelCreator>();
			for (int j = 0; j < inner; j++) {
				FutureChannelCreator fc = r.create(conn, 0);
				fc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						final ChannelCreator cc = future.channelCreator();
						final CountDownLatch countDownLatch = new CountDownLatch(conn);
						for (int k = 0; k < conn; k++) {
							ChannelFuture channelFuture = cc.createUDP(false,
							        new HashMap<String, Pair<EventExecutorGroup, ChannelHandler>>() {
							        }, new FutureResponse(null));
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
		long start = System.currentTimeMillis();
		final int round = 100;
		final int inner = 100;
		final int conn = 5;
		final int tcpMax = 500;
		for (int i = 0; i < round; i++) {
			ChannelClientConfiguration c = PeerBuilder.createDefaultChannelClientConfiguration();
			c.pipelineFilter(new MyPipeLine());
			c.maxPermitsTCP(tcpMax);
			Reservation r = new Reservation(workerGroup, c);
			List<FutureChannelCreator> fcc = new ArrayList<FutureChannelCreator>();
			for (int j = 0; j < inner; j++) {
				FutureChannelCreator fc = r.create(0, conn);
				fc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						if (future.isFailed()) {
							return;
						}
						final ChannelCreator cc = future.channelCreator();
						final int timeout = 2000;
						for (int k = 0; k < conn; k++) {
							ChannelFuture channelFuture = cc.createTCP(SOCKET_ADDRESS, timeout,
							        new HashMap<String, Pair<EventExecutorGroup, ChannelHandler>>() {
							        }, new FutureResponse(null));
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
			r.shutdown().awaitListenersUninterruptibly();
		}
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
		final int round = 100;
		final int inner = 100;
		final int conn = 5;
		final int tcpMax = 500;
		for (int i = 0; i < round; i++) {
			ChannelClientConfiguration c = PeerBuilder.createDefaultChannelClientConfiguration();
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
						final ChannelCreator cc = future.channelCreator();
						final int timeout = 2000;
						for (int k = 0; k < conn; k++) {
							ChannelFuture channelFuture = cc.createTCP(SOCKET_ADDRESS, timeout,
							        new HashMap<String, Pair<EventExecutorGroup, ChannelHandler>>() {
							        }, new FutureResponse(null));
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
			r.shutdown().awaitListenersUninterruptibly();
		}
		ev.shutdownGracefully().awaitUninterruptibly();
	}

	private static class MyPipeLine implements PipelineFilter {

		@Override
		public Map<String, Pair<EventExecutorGroup, ChannelHandler>> filter(Map<String, Pair<EventExecutorGroup, ChannelHandler>> channelHandlers, boolean tcp,
		        boolean client) {
			for (Iterator<Map.Entry<String, Pair<EventExecutorGroup, ChannelHandler>>> iterator = channelHandlers
			        .entrySet().iterator(); iterator.hasNext();) {
				if (iterator.next().getValue() == null) {
					iterator.remove();
				}
			}
			return channelHandlers;
		}

	}

}
