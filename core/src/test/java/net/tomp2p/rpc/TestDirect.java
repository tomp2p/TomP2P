package net.tomp2p.rpc;

import io.netty.channel.ChannelHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.LinkedHashMap;
import java.util.Map;

import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.connection.PipelineFilter;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.CountConnectionOutboundHandler;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestDirect {

    @Test
    public void testDirectMessage1() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start();
            recv1.objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    return "yes";
                }
            });

            FutureChannelCreator fcc = sender.connectionBean().reservation().create(0, 2);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            SendDirectBuilder sendDirectBuilder = new SendDirectBuilder(sender, (PeerAddress) null);
            sendDirectBuilder.object("test");

            FutureResponse fd1 = sender.directDataRPC()
                    .send(recv1.peerAddress(), sendDirectBuilder, cc);
            FutureResponse fd2 = sender.directDataRPC()
                    .send(recv1.peerAddress(), sendDirectBuilder, cc);

            fd1.awaitUninterruptibly();
            fd2.awaitUninterruptibly();

            System.err.println(fd1.failedReason());
            Assert.assertEquals(true, fd1.isSuccess());
            Assert.assertEquals(true, fd2.isSuccess());

            Object ret = fd1.responseMessage().buffer(0).object();
            Assert.assertEquals("yes", ret);
            fd1.release();
            fd2.release();
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testOrder() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            recv1.objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    Integer i = (Integer) request;
                    System.err.println("got " + i);
                    return i + 1;
                }
            });
            for (int i = 0; i < 500; i++) {

                FutureChannelCreator fcc = sender.connectionBean().reservation().create(0, 1);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();

                SendDirectBuilder sendDirectBuilder = new SendDirectBuilder(sender, (PeerAddress) null);
                sendDirectBuilder.object((Object) Integer.valueOf(i));

                final FutureResponse futureData = sender.directDataRPC().send(recv1.peerAddress(),
                        sendDirectBuilder, cc);
                Utils.addReleaseListener(cc, futureData);
                futureData.addListener(new BaseFutureAdapter<FutureResponse>() {
                    @Override
                    public void operationComplete(FutureResponse future) throws Exception {
                        // the future object might be null if the future failed,
                        // e.g due to shutdown
                        System.err.println(future.responseMessage().buffer(0).object());
                        futureData.release();
                    }
                });         
            }
            System.err.println("done");
            Thread.sleep(2000);

        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testDirectReconnect() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
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
            sender = new PeerBuilder(new Number160("0x50")).p2pId(55).enableMaintenance(false).ports(2424).channelClientConfiguration(ccc).channelServerConfiguration(csc).start();
            recv1 = new PeerBuilder(new Number160("0x20")).p2pId(55).enableMaintenance(false).ports(8088).channelClientConfiguration(ccc).channelServerConfiguration(csc).start();
            recv1.objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    return "yes";
                }
            });
            FuturePeerConnection peerConnection = sender.createPeerConnection(recv1.peerAddress());
            ccohTCP.reset();
            ccohUDP.reset();

            FutureDirect fd1 = sender.sendDirect(peerConnection).object("test")
                    .connectionTimeoutTCPMillis(2000).idleTCPMillis(10 * 1000).start();
            fd1.awaitUninterruptibly();
            fd1.awaitListenersUninterruptibly();
            Assert.assertEquals(true, fd1.isSuccess());
            Assert.assertEquals(1, ccohTCP.total());
            Assert.assertEquals(0, ccohUDP.total());
            Thread.sleep(2000);
            System.err.println("send second with the same connection");
            FutureDirect fd2 = sender.sendDirect(peerConnection).object("test").start();
            fd2.awaitUninterruptibly();
            Assert.assertEquals(1, ccohTCP.total());
            Assert.assertEquals(0, ccohUDP.total());
            Assert.assertEquals(true, fd2.isSuccess());
            peerConnection.close().await();
            System.err.println("done");
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testDirect2() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
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
            sender = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).enableMaintenance(false)
                    .channelClientConfiguration(ccc).channelServerConfiguration(csc).start();
            recv1 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).enableMaintenance(false)
            		.channelClientConfiguration(ccc).channelServerConfiguration(csc).start();
            recv1.objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    return "yes";
                }
            });
            FuturePeerConnection peerConnection = sender.createPeerConnection(recv1.peerAddress(), 8000);
            ccohTCP.reset();
            ccohUDP.reset();

            FutureDirect fd1 = sender.sendDirect(peerConnection).object("test")
                    .connectionTimeoutTCPMillis(2000).idleTCPMillis(5 * 1000).start();
            fd1.awaitUninterruptibly();
            Assert.assertTrue(fd1.isSuccess());

            Assert.assertEquals(1, ccohTCP.total());

            Thread.sleep(7000);
            System.out.println("request 2");
            FutureDirect fd2 = sender.sendDirect(peerConnection).object("test").start();
            fd2.awaitUninterruptibly();
            peerConnection.close().await();
            Assert.assertEquals(2, ccohTCP.total());
            System.out.println("done");
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }
}
