package net.tomp2p.p2p;

import io.netty.channel.ChannelHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.connection.PipelineFilter;
import net.tomp2p.connection.StandardProtocolFamily;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.message.CountConnectionOutboundHandler;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.utils.Pair;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestConnection {
	
	@Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };

    @Test
    public void test() throws Exception {
        Random rnd = new Random(42);
        Peer peer1 = null;
        Peer peer2 = null;
        try {
        	
        	final CountConnectionOutboundHandler ccohTCP = new CountConnectionOutboundHandler();
        	final CountConnectionOutboundHandler ccohUDP = new CountConnectionOutboundHandler();
        	
        	PipelineFilter pf = new PipelineFilter() {
				@Override
				public Map<String, Pair<EventExecutorGroup, ChannelHandler>> filter(Map<String, Pair<EventExecutorGroup, ChannelHandler>> channelHandlers, boolean tcp,
				        boolean client) {
					
					Map<String, Pair<EventExecutorGroup, ChannelHandler>> retVal = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>();
					retVal.put("counter", new Pair<EventExecutorGroup, ChannelHandler>(null, tcp? ccohTCP:ccohUDP));
					retVal.putAll(channelHandlers);
					return retVal;
				}
			};
			ChannelServerConfiguration csc = PeerBuilder.createDefaultChannelServerConfiguration();
			ChannelClientConfiguration ccc = PeerBuilder.createDefaultChannelClientConfiguration();
			csc.pipelineFilter(pf);
			ccc.pipelineFilter(pf);
        	
            Bindings b1 = new Bindings().addProtocol(StandardProtocolFamily.INET).addAddress(InetAddress.getByName("127.0.0.1"));
            Bindings b2 = new Bindings().addProtocol(StandardProtocolFamily.INET).addAddress(InetAddress.getByName("127.0.0.1"));
            
            peer1 = new PeerBuilder(new Number160(rnd)).ports(4005).bindings(b1).channelClientConfiguration(ccc).channelServerConfiguration(csc).start();
            peer2 = new PeerBuilder(new Number160(rnd)).ports(4006).bindings(b2).channelClientConfiguration(ccc).channelServerConfiguration(csc).start();

            peer2.objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    return "world!";
                }
            });
            // keep the connection for 20s alive. Setting -1 means to keep it
            // open as long as possible
            FutureBootstrap masterAnother = peer1.bootstrap().peerAddress(peer2.peerAddress()).start();
            FutureBootstrap anotherMaster = peer2.bootstrap().peerAddress(peer1.peerAddress()).start();
            masterAnother.awaitUninterruptibly();
            anotherMaster.awaitUninterruptibly();
            FuturePeerConnection fpc = peer1.createPeerConnection(peer2.peerAddress());
            // fpc.awaitUninterruptibly();
            // PeerConnection peerConnection = fpc.peerConnection();
            String sentObject = "Hello";
            FutureDirect fd = peer1.sendDirect(fpc).object(sentObject).start();
            System.out.println("send " + sentObject);
            fd.awaitUninterruptibly();
            Assert.assertEquals(true, fd.isSuccess());
            System.out.println("received " + fd.object() + " connections: "
                    + ccohTCP.total());
            // we reuse the connection
            long start = System.currentTimeMillis();
            System.out.println("send " + sentObject);
            fd = peer1.sendDirect(fpc).object(sentObject).start();
            fd.awaitUninterruptibly();
            System.err.println(fd.failedReason());
            Assert.assertEquals(true, fd.isSuccess());
            System.err.println(fd.failedReason());
            System.out.println("received " + fd.object() + " connections: "
                    + ccohTCP.total());
            // now we don't want to keep the connection open anymore:
            double duration = (System.currentTimeMillis() - start) / 1000d;
            System.out.println("Send and get in s:" + duration);
            fpc.close().await();
            System.out.println("done");
        } finally {
            if (peer1 != null) {
                peer1.shutdown().await();
                System.out.println("done1");
            }
            if (peer2 != null) {
                peer2.shutdown().await();
                System.out.println("done2");
            }
        }
    }
    
    @Test
    public void testPermanentConnection() throws Exception {
        Random rnd = new Random(42);
        Peer peer1 = null;
        Peer peer2 = null;
        try {
            
            Bindings b1 = new Bindings().addProtocol(StandardProtocolFamily.INET).addAddress(InetAddress.getByName("127.0.0.1"));
            Bindings b2 = new Bindings().addProtocol(StandardProtocolFamily.INET).addAddress(InetAddress.getByName("127.0.0.1"));
            
            peer1 = new PeerBuilder(new Number160(rnd)).ports(4005).bindings(b1).start();
            peer2 = new PeerBuilder(new Number160(rnd)).ports(4006).bindings(b2).start();

            peer2.objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    return "world!";
                }
            });
            // keep the connection for 20s alive. Setting -1 means to keep it
            // open as long as possible
            FutureBootstrap masterAnother = peer1.bootstrap().peerAddress(peer2.peerAddress()).start();
            FutureBootstrap anotherMaster = peer2.bootstrap().peerAddress(peer1.peerAddress()).start();
            masterAnother.awaitUninterruptibly();
            anotherMaster.awaitUninterruptibly();
            
            int before = peer1.connectionBean().reservation().availablePermitsPermanentTCP();
            final FuturePeerConnection fpc = peer1.createPeerConnection(peer2.peerAddress());
            
            // fpc.awaitUninterruptibly();
            // PeerConnection peerConnection = fpc.peerConnection();
            String sentObject = "Hello";
            FutureDirect fd = peer1.sendDirect(fpc).object(sentObject).start();
            fd.awaitUninterruptibly();
            
            Assert.assertEquals(before - 1, peer1.connectionBean().reservation().availablePermitsPermanentTCP());
            
            peer2.shutdown().await();
            
            fpc.peerConnection().closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
                @Override
                public void operationComplete(FutureDone<Void> future) throws Exception {
                    fpc.peerConnection().close().await();
                }
            });
            //TODO: make this not wait
            Thread.sleep(1000);
            Assert.assertEquals(before, peer1.connectionBean().reservation().availablePermitsPermanentTCP());
            
            // we reuse the connection
            /*long start = System.currentTimeMillis();
            System.out.println("send " + sentObject);
            fd = peer1.sendDirect(fpc).object(sentObject).start();
            fd.awaitUninterruptibly();
            System.err.println(fd.failedReason());
            Assert.assertEquals(true, fd.isSuccess());
            System.err.println(fd.failedReason());
            System.out.println("received " + fd.object() + " connections: "
                    + ccohTCP.total());
            // now we don't want to keep the connection open anymore:
            double duration = (System.currentTimeMillis() - start) / 1000d;
            System.out.println("Send and get in s:" + duration);*/
            fpc.close().await();
            System.out.println("done");
        } finally {
            if (peer1 != null) {
                peer1.shutdown().await();
                System.out.println("done1");
            }
            if (peer2 != null) {
                peer2.shutdown().await();
                System.out.println("done2");
            }
        }
    }
}
