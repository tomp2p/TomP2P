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
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.utils.Pair;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the channel creator with a sink created with "nc". It is possible to observe the traffic with
 * "sudo tcpdump -i lo".
 * 
 * A quick benchmark shows that the speed of TCP, with around ~9'000 TCP open/close per second on localhost is ok.
 * Comparing this to:
 * 
 * <pre>
 * server command: nc -lk -p 4001
 * client command: time for i in {1..10000}; do nc 127.0.0.1 4001 < /dev/null; done
 * </pre>
 * 
 * that results in around 500 open/close per seconds. Of course a direct comparison is not possible since you have the
 * process creation overhead, but its an indication.
 * 
 * If we try this in golang, then its faster: ~10'500 open/close per second if we use concurrent access (20 concurrent).
 * Thus Java is not that much slower. However, increasing the concurrent access results in a worse performance for java
 * (~7'000), while golang remains around 10'000.
 * 
 * @author Thomas Bocek
 * 
 */
public class TestChannelCreator {

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
        Bindings bindings = new Bindings().addAddress(Inet4Address.getByName("127.0.0.1"));
        ChannelServerConficuration c = new ChannelServerConficuration();
        c.interfaceBindings(bindings);
        c.internalPorts(new Ports(PORT, PORT));
        c.pipelineFilter(new MyPipeLine());
        final EventLoopGroup bossGroup = new NioEventLoopGroup(2,
    	        new DefaultThreadFactory(ConnectionBean.THREAD_NAME + "boss - "));
    	final EventLoopGroup workerGroup = new NioEventLoopGroup(2,
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
     * Test the server part of TomP2P with external applications. Not intended to be used in automated tests.
     * 
     * @throws InterruptedException .
     * @throws IOException .
     */
    @Test
    @Ignore
    public void sink() throws InterruptedException, IOException {
        ChannelServerConficuration c = new ChannelServerConficuration();
        final EventLoopGroup bossGroup = new NioEventLoopGroup(2,
    	        new DefaultThreadFactory(ConnectionBean.THREAD_NAME + "boss - "));
    	final EventLoopGroup workerGroup = new NioEventLoopGroup(2,
    	        new DefaultThreadFactory(ConnectionBean.THREAD_NAME + "worker-server - "));
        ChannelServer cs = new ChannelServer(bossGroup, workerGroup, c, null, null);
        final int port = 4000;
        cs.startupTCP(new InetSocketAddress("127.0.0.1", port), new ChannelServerConficuration());
        // wait forever.
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * This test only works if the server calls "nc -lk 4000", that is done in @Before. It will open and close 1000 *
     * 1000 connections to localhost.
     * 
     * @throws InterruptedException .
     */
    @Test
    @Ignore
    public void testCreatorTCP() throws InterruptedException {
        long start = System.currentTimeMillis();
        final int rounds = 10000;
        final int connections = 20;
        final int printOut = 100;
        EventLoopGroup ev = new NioEventLoopGroup();

        final int timeout = 4000;
        for (int j = 0; j < rounds; j++) {

            ChannelClientConfiguration c = PeerBuilder.createDefaultChannelClientConfiguration();
            c.pipelineFilter(new MyPipeLine());
            
            final ChannelCreator channelCreator2 = new ChannelCreator(ev, new FutureDone<Void>(), 0,
                    connections, c);
            if (j % printOut == 0) {
                System.out.print(j + " ");
            }

            final CountDownLatch countDownLatch = new CountDownLatch(connections);
            final Map<String, Pair<EventExecutorGroup, ChannelHandler>> tmp = new HashMap<String, Pair<EventExecutorGroup, ChannelHandler>>();

            final GenericFutureListener<ChannelFuture> handler = new GenericFutureListener<ChannelFuture>() {
                @Override
                public void operationComplete(final ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        future.channel().close();
                        countDownLatch.countDown();

                    } else {
                        future.cause().printStackTrace();
                    }
                }
            };

            for (int i = 0; i < connections; i++) {
                final ChannelFuture channelFuture = channelCreator2.createTCP(SOCKET_ADDRESS, timeout, tmp, new FutureResponse(null));
                channelFuture.addListener(handler);
            }
            countDownLatch.await();
            channelCreator2.shutdown().awaitUninterruptibly();
        }
        ev.shutdownGracefully().awaitUninterruptibly();
        long time = System.currentTimeMillis() - start;
        long mil = TimeUnit.SECONDS.toMillis(1);
        System.err.println("\nBENCHMARK: opened and closed " + connections + " x " + rounds
                + " TCP connections to localhost in " + time + " ms. STAT: TCP open/close per sec:"
                + ((connections * rounds * mil) / time));
    }

    /**
     * This test only works if the server calls "nc -lku 4000", that is done in @Before.
     * 
     * @throws InterruptedException .
     */
    @Test
    @Ignore
    public void testCreatorUDP() throws InterruptedException {
        long start = System.currentTimeMillis();
        final int rounds = 10000;
        final int connections = 20;
        final int printOut = 100;
        EventLoopGroup ev = new NioEventLoopGroup();

        for (int j = 0; j < rounds; j++) {

            ChannelClientConfiguration c = PeerBuilder.createDefaultChannelClientConfiguration();
            c.pipelineFilter(new MyPipeLine());
            final ChannelCreator channelCreator2 = new ChannelCreator(ev, new FutureDone<Void>(),
                    connections, 0, c);

            if (j % printOut == 0) {
                System.out.print(j + " ");
            }

            final CountDownLatch countDownLatch = new CountDownLatch(connections);
            final Map<String, Pair<EventExecutorGroup, ChannelHandler>> tmp = new HashMap<String, Pair<EventExecutorGroup, ChannelHandler>>();

            GenericFutureListener<ChannelFuture> handler = new GenericFutureListener<ChannelFuture>() {
                @Override
                public void operationComplete(final ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        future.channel().writeAndFlush(Unpooled.wrappedBuffer(new byte[1]));
                        future.channel().close();
                        countDownLatch.countDown();
                    } else {
                        future.cause().printStackTrace();
                    }
                }
            };

            for (int i = 0; i < connections; i++) {
                final ChannelFuture channelFuture = channelCreator2.createUDP(SOCKET_ADDRESS, false, tmp, new FutureResponse(null));
                channelFuture.addListener(handler);
            }
            countDownLatch.await();
            channelCreator2.shutdown().awaitUninterruptibly();
        }
        ev.shutdownGracefully().awaitUninterruptibly();
        long time = System.currentTimeMillis() - start;
        long mil = TimeUnit.SECONDS.toMillis(1);
        System.err.println("\nBENCHMARK: opened and closed " + connections + " x " + rounds
                + " UDP connections to localhost in " + time + " ms. STAT: UDP open/close per sec:"
                + ((connections * rounds * mil) / time));
    }
    
    private static class MyPipeLine implements PipelineFilter {

        @Override
        public Map<String, Pair<EventExecutorGroup, ChannelHandler>> filter(Map<String, Pair<EventExecutorGroup, ChannelHandler>> channelHandlers, boolean tcp, boolean client) {
            for(Iterator<Map.Entry<String, Pair<EventExecutorGroup, ChannelHandler>>> iterator = channelHandlers.entrySet().iterator();iterator.hasNext(); ) {
                if(iterator.next().getValue()==null) {
                    iterator.remove();
                }
            }
            return channelHandlers;
        }
        
    }

}
