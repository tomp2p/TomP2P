package net.tomp2p.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ChannelServerConficuration;
import net.tomp2p.connection.PipelineFilter;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.ProgressListener;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.CountConnectionOutboundHandler;
import net.tomp2p.message.Message;
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
    public void testDirectMessage() throws Exception {
        testDirectMessage(true);
        testDirectMessage(false);
    }

    private void testDirectMessage(boolean wait) throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        final AtomicInteger replyComplete = new AtomicInteger(0);
        final AtomicInteger replyNotComplete = new AtomicInteger(0);
        final AtomicInteger progressComplete = new AtomicInteger(0);
        final AtomicInteger progressNotComplete = new AtomicInteger(0);
        try {

            PeerBuilder pm1 = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424);
            ChannelServerConficuration css = pm1.createDefaultChannelServerConfiguration();
            css.idleTCPSeconds(Integer.MAX_VALUE);
            pm1.channelServerConfiguration(css);
            sender = pm1.start();

            PeerBuilder pm2 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088);
            pm2.channelServerConfiguration(css);
            recv1 = pm2.start();

            recv1.rawDataReply(new RawDataReply() {

                @Override
                public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete)
                        throws Exception {
                    System.err.println("reply 2 ? " + complete);
                    ByteBuf replyBuffer = Unpooled.buffer(50);
                    replyBuffer.writerIndex(50);
                    if (complete) {
                        replyComplete.incrementAndGet();
                        return new Buffer(replyBuffer, 100);
                    } else {
                        replyNotComplete.incrementAndGet();
                        return new Buffer(replyBuffer, 100);
                    }
                }
            });

            FutureChannelCreator fcc = sender.connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            SendDirectBuilder sendDirectBuilder = new SendDirectBuilder(sender, (PeerAddress) null);
            sendDirectBuilder.streaming();
            sendDirectBuilder.idleTCPSeconds(Integer.MAX_VALUE);

            byte[] me = new byte[50];
            Buffer b = new Buffer(Unpooled.compositeBuffer(), 100);
            b.addComponent(Unpooled.wrappedBuffer(me));
            if (!wait) {
                ByteBuf replyBuffer = Unpooled.buffer(50);
                replyBuffer.writerIndex(50);
                b.addComponent(replyBuffer);
            }
            sendDirectBuilder.buffer(b);
            sendDirectBuilder.progressListener(new ProgressListener() {
                @Override
                public void progress(final Message interMediateMessage) {
                    if (interMediateMessage.isDone()) {
                        progressComplete.incrementAndGet();
                        System.err.println("progress 1 ? done");
                    } else {
                        progressNotComplete.incrementAndGet();
                        System.err.println("progress 1 ? not done");
                    }
                }
            });

            FutureResponse fr = sender.directDataRPC().send(recv1.peerAddress(), sendDirectBuilder, cc);
            if (wait) {
                Thread.sleep(500);
                ByteBuf replyBuffer = Unpooled.buffer(50);
                replyBuffer.writerIndex(50);
                b.addComponent(replyBuffer);
            }
            fr.progress();
            System.err.println("progres");
            // we are not done yet!

            // now we are done
            fr.awaitUninterruptibly();

            if (wait) {
                Assert.assertEquals(1, progressComplete.get());
                Assert.assertEquals(1, progressNotComplete.get());
                Assert.assertEquals(1, replyComplete.get());
                Assert.assertEquals(1, replyNotComplete.get());
            } else {
                Assert.assertEquals(1, progressComplete.get());
                Assert.assertEquals(1, progressNotComplete.get());
                Assert.assertEquals(2, replyComplete.get());
                Assert.assertEquals(0, replyNotComplete.get());
            }

            Assert.assertEquals(true, fr.isSuccess());
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

                FutureResponse futureData = sender.directDataRPC().send(recv1.peerAddress(),
                        sendDirectBuilder, cc);
                Utils.addReleaseListener(cc, futureData);
                futureData.addListener(new BaseFutureAdapter<FutureResponse>() {
                    @Override
                    public void operationComplete(FutureResponse future) throws Exception {
                        // the future object might be null if the future failed,
                        // e.g due to shutdown
                        System.err.println(future.responseMessage().buffer(0).object());
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
			ChannelServerConficuration csc = PeerBuilder.createDefaultChannelServerConfiguration();
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
                    .connectionTimeoutTCPMillis(2000).idleTCPSeconds(10 * 1000).start();
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
			ChannelServerConficuration csc = PeerBuilder.createDefaultChannelServerConfiguration();
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
                    .connectionTimeoutTCPMillis(2000).idleTCPSeconds(5).start();
            fd1.awaitUninterruptibly();

            Assert.assertEquals(1, ccohTCP.total());

            Thread.sleep(7000);

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
