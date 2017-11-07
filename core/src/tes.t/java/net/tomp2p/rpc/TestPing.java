package net.tomp2p.rpc;

import java.util.ArrayList;
import java.util.List;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestPing {
    static Bindings bindings = new Bindings();
    static {
        //bindings.addInterface("lo");
    }
    
    @Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };

    @Test
    public void testPingTCP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();

            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            FutureResponse fr = sender.pingRPC().pingTCP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
        } finally {
            if (cc != null) {
                cc.shutdown().await();
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
    public void testPingTCP2() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();

            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            FutureResponse fr = sender.pingRPC().pingTCP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            FutureResponse fr2 = recv1.pingRPC().pingTCP(sender.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr2.awaitUninterruptibly();
            Assert.assertEquals(true, fr2.isSuccess());
            Assert.assertEquals(true, fr.isSuccess());
        } finally {
            if (cc != null) {
                cc.shutdown().await();
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
    public void testPingTCPDeadLock2() throws Exception {
        Peer sender1 = null;
        Peer recv11 = null;
        ChannelCreator cc = null;
        try {
            final Peer sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            sender1 = sender;
            final Peer recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            recv11 = recv1;

            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            final ChannelCreator cc1 = cc;

            FutureResponse fr = sender.pingRPC().pingTCP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();

            fr.addListener(new BaseFutureAdapter<FutureResponse>() {
                @Override
                public void operationComplete(final FutureResponse future) throws Exception {
                    FutureResponse fr2 = sender.pingRPC().pingTCP(recv1.peerAddress(), cc1,
                            new DefaultConnectionConfiguration());
                    try {
                        fr2.await();
                    } catch (IllegalStateException ise) {
                        Assert.fail();
                    }
                }
            });
            Thread.sleep(1000);
            Assert.assertEquals(true, fr.isSuccess());
        } finally {
            if (cc != null) {
                cc.shutdown().await();
            }
            if (sender1 != null) {
                sender1.shutdown().await();
            }
            if (recv11 != null) {
                recv11.shutdown().await();
            }
        }
    }

    @Test
    public void testPingUDP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean());
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean());
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            FutureResponse fr = handshake.pingUDP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
        } finally {
            if (cc != null) {
                cc.shutdown().await();
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
    public void testPingHandlerError() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean(), false, true,
                    false);
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean(), false, true, false);
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            FutureResponse fr = handshake.pingTCP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(false, fr.isSuccess());
        } finally {
            if (cc != null) {
                cc.shutdown().await();
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
    public void testPingTimeoutTCP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean(), false, true,
                    true);
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean(), false, true, true);
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            FutureResponse fr = handshake.pingTCP(recv1.peerBean().serverPeerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(false, fr.isSuccess());
            System.err.println("done:" + fr.failedReason());
            Assert.assertEquals(true, fr.failedReason().contains("TIMEOUT"));
            
        } finally {
            if (cc != null) {
                cc.shutdown().await();
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
    public void testPingHandlerFailure() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean(), false, true,
                    false);
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean(), false, true, false);
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            FutureResponse fr = handshake.pingUDP(recv1.peerBean().serverPeerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(false, fr.isSuccess());
        } finally {
            if (cc != null) {
                cc.shutdown().await();
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
    public void testPingTimeoutUDP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean(), false, true,
                    true);
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean(), false, true, true);
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            FutureResponse fr = handshake.pingUDP(recv1.peerBean().serverPeerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(false, fr.isSuccess());
            System.err.println("done:" + fr.failedReason());
            Assert.assertEquals(true, fr.failedReason().contains("TIMEOUT"));
        } finally {
            if (cc != null) {
                cc.shutdown().await();
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
    public void testPingTCPPool() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            List<FutureResponse> list = new ArrayList<FutureResponse>(50);
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 50);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            for (int i = 0; i < 50; i++) {
                FutureResponse fr = sender.pingRPC().pingTCP(recv1.peerAddress(), cc,
                        new DefaultConnectionConfiguration());
                list.add(fr);
            }
            for (FutureResponse fr2 : list) {
                fr2.awaitUninterruptibly();
                Assert.assertTrue(fr2.isSuccess());
            }
        } finally {
            if (cc != null) {
                cc.shutdown().await();
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
    public void testPingTCPPool2() throws Exception {
        Peer p[] = new Peer[50];
        try {
            for (int i = 0; i < p.length; i++) {
                p[i] = new PeerBuilder(Number160.createHash(i)).p2pId(55).ports(2424 + i).start();
            }
            List<FutureResponse> list = new ArrayList<FutureResponse>();
            for (int i = 0; i < p.length; i++) {
                FutureChannelCreator fcc = p[0].connectionBean().reservation().create(0, 1);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.channelCreator();
                FutureResponse fr = p[0].pingRPC().pingTCP(p[i].peerAddress(), cc,
                        new DefaultConnectionConfiguration());
                Utils.addReleaseListener(cc, fr);
                list.add(fr);
            }
            for (FutureResponse fr2 : list) {
                fr2.awaitUninterruptibly();
                boolean success = fr2.isSuccess();
                if (!success) {
                    System.err.println("FAIL.");
                    Assert.fail();
                }
            }
            System.err.println("DONE.");
        } finally {
            for (int i = 0; i < p.length; i++) {
                p[i].shutdown().await();
            }
        }
    }

    @Test
    public void testPingTime() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            long start = System.currentTimeMillis();
            List<FutureResponse> list = new ArrayList<FutureResponse>(100);
            for (int i = 0; i < 20; i++) {
                FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 50);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.channelCreator();
                for (int j = 0; j < 50; j++) {
                    FutureResponse fr = sender.pingRPC().pingTCP(recv1.peerAddress(), cc,
                            new DefaultConnectionConfiguration());
                    list.add(fr);
                }
                for (FutureResponse fr2 : list) {
                    fr2.awaitUninterruptibly();
                    if (!fr2.isSuccess())
                        System.err.println("fail " + fr2.failedReason());
                    Assert.assertEquals(true, fr2.isSuccess());
                }
                list.clear();
                cc.shutdown().await();
            }
            System.out.println("TCP time: " + (System.currentTimeMillis() - start));
            for (FutureResponse fr2 : list) {
                fr2.awaitUninterruptibly();
                Assert.assertEquals(true, fr2.isSuccess());
            }
            //
            start = System.currentTimeMillis();
            list = new ArrayList<FutureResponse>(50);
            for (int i = 0; i < 20; i++) {
                FutureChannelCreator fcc = recv1.connectionBean().reservation().create(50, 0);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.channelCreator();
                for (int j = 0; j < 50; j++) {
                    FutureResponse fr = sender.pingRPC().pingUDP(recv1.peerAddress(), cc,
                            new DefaultConnectionConfiguration());
                    list.add(fr);
                }
                for (FutureResponse fr2 : list) {
                    fr2.awaitUninterruptibly();
                    Assert.assertEquals(true, fr2.isSuccess());
                }
                list.clear();
                cc.shutdown().await();
            }

            System.out.println("UDP time: " + (System.currentTimeMillis() - start));
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
    public void testPingReserveLoop() throws Exception {
        for (int i = 0; i < 100; i++) {
            testPingReserve();
        }
    }

    @Test
    public void testPingReserve() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.channelCreator();
            FutureResponse fr = sender.pingRPC().pingTCP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            Utils.addReleaseListener(cc, fr);
            fr.awaitUninterruptibly();
            fr.awaitListeners();
            Assert.assertEquals(true, fr.isSuccess());
            
            FutureResponse fr2 = sender.pingRPC().pingTCP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr2.awaitUninterruptibly();
            fr2.awaitListeners();
            // we have released the reservation here
            // System.err.println(fr2.getFailedReason());
            Assert.assertEquals(false, fr2.isSuccess());

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
