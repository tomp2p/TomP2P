package net.tomp2p.rpc;

import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ChannelServerConficuration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.ProgressListener;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Timings;
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

            PeerMaker pm1 = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424);
            ChannelServerConficuration css = pm1.createDefaultChannelServerConfiguration();
            css.idleTCPSeconds(Integer.MAX_VALUE);
            pm1.channelServerConfiguration(css);
            sender = pm1.makeAndListen();

            PeerMaker pm2 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088);
            pm2.channelServerConfiguration(css);
            recv1 = pm2.makeAndListen();

            recv1.setRawDataReply(new RawDataReply() {

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

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            SendDirectBuilder sendDirectBuilder = new SendDirectBuilder(sender, (PeerAddress) null);
            sendDirectBuilder.setStreaming();
            sendDirectBuilder.idleTCPSeconds(Integer.MAX_VALUE);

            byte[] me = new byte[50];
            Buffer b = new Buffer(Unpooled.compositeBuffer(), 100);
            b.addComponent(Unpooled.wrappedBuffer(me));
            if (!wait) {
                ByteBuf replyBuffer = Unpooled.buffer(50);
                replyBuffer.writerIndex(50);
                b.addComponent(replyBuffer);
            }
            sendDirectBuilder.setBuffer(b);
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

            FutureResponse fr = sender.getDirectDataRPC().send(recv1.getPeerAddress(), sendDirectBuilder, cc);
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
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            recv1.setObjectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    return "yes";
                }
            });

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 2);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            SendDirectBuilder sendDirectBuilder = new SendDirectBuilder(sender, (PeerAddress) null);
            sendDirectBuilder.setObject("test");

            FutureResponse fd1 = sender.getDirectDataRPC()
                    .send(recv1.getPeerAddress(), sendDirectBuilder, cc);
            FutureResponse fd2 = sender.getDirectDataRPC()
                    .send(recv1.getPeerAddress(), sendDirectBuilder, cc);

            fd1.awaitUninterruptibly();
            fd2.awaitUninterruptibly();

            System.err.println(fd1.getFailedReason());
            Assert.assertEquals(true, fd1.isSuccess());
            Assert.assertEquals(true, fd2.isSuccess());

            Object ret = fd1.getResponse().getBuffer(0).object();
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            recv1.setObjectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    Integer i = (Integer) request;
                    System.err.println("got " + i);
                    return i + 1;
                }
            });
            for (int i = 0; i < 500; i++) {

                FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
                fcc.awaitUninterruptibly();
                cc = fcc.getChannelCreator();

                SendDirectBuilder sendDirectBuilder = new SendDirectBuilder(sender, (PeerAddress) null);
                sendDirectBuilder.setObject((Object) Integer.valueOf(i));

                FutureResponse futureData = sender.getDirectDataRPC().send(recv1.getPeerAddress(),
                        sendDirectBuilder, cc);
                Utils.addReleaseListener(cc, futureData);
                futureData.addListener(new BaseFutureAdapter<FutureResponse>() {
                    @Override
                    public void operationComplete(FutureResponse future) throws Exception {
                        // the future object might be null if the future failed,
                        // e.g due to shutdown
                        System.err.println(future.getResponse().getBuffer(0).object());
                    }
                });
            }
            System.err.println("done");
            Timings.sleep(2000);

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

            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            recv1.setObjectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    return "yes";
                }
            });
            FuturePeerConnection peerConnection = sender.createPeerConnection(recv1.getPeerAddress());
            ChannelCreator.resetConnectionCounts();

            FutureResponse fd1 = sender.sendDirect(peerConnection).setObject("test")
                    .connectionTimeoutTCPMillis(2000).idleTCPSeconds(10 * 1000).start();
            Assert.assertEquals(1, ChannelCreator.tcpConnectionCount());
            Assert.assertEquals(0, ChannelCreator.udpConnectionCount());
            fd1.awaitListenersUninterruptibly();
            Assert.assertEquals(true, fd1.isSuccess());
            Assert.assertEquals(1, ChannelCreator.tcpConnectionCount());
            Assert.assertEquals(0, ChannelCreator.udpConnectionCount());
            Timings.sleep(2000);
            System.err.println("send second with the same connection");
            FutureResponse fd2 = sender.sendDirect(peerConnection).setObject("test").start();
            fd2.awaitUninterruptibly();
            Assert.assertEquals(1, ChannelCreator.tcpConnectionCount());
            Assert.assertEquals(0, ChannelCreator.udpConnectionCount());
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

            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).setEnableMaintenance(false)
                    .makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).setEnableMaintenance(false)
                    .makeAndListen();
            recv1.setObjectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    return "yes";
                }
            });
            FuturePeerConnection peerConnection = sender.createPeerConnection(recv1.getPeerAddress());
            ChannelCreator.resetConnectionCounts();

            FutureResponse fd1 = sender.sendDirect(peerConnection).setObject("test")
                    .connectionTimeoutTCPMillis(2000).idleTCPSeconds(5).start();
            fd1.awaitUninterruptibly();

            Assert.assertEquals(1, ChannelCreator.tcpConnectionCount());

            Timings.sleep(7000);

            FutureResponse fd2 = sender.sendDirect(peerConnection).setObject("test").start();
            fd2.awaitUninterruptibly();
            peerConnection.close().await();
            Assert.assertEquals(2, ChannelCreator.tcpConnectionCount());
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
