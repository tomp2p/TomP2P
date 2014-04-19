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
import net.tomp2p.p2p.PeerReachable;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.PingRPC;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestPing {
    static Bindings bindings = new Bindings();
    static {
        bindings.addInterface("lo");
    }

    @Test
    public void testPingTCP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            FutureResponse fr = sender.pingRPC().pingTCP(recv1.getPeerAddress(), cc,
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            FutureResponse fr = sender.pingRPC().pingTCP(recv1.getPeerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            FutureResponse fr2 = recv1.pingRPC().pingTCP(sender.getPeerAddress(), cc,
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
            final Peer sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            sender1 = sender;
            final Peer recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            recv11 = recv1;

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();
            final ChannelCreator cc1 = cc;

            FutureResponse fr = sender.pingRPC().pingTCP(recv1.getPeerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();

            fr.addListener(new BaseFutureAdapter<FutureResponse>() {
                @Override
                public void operationComplete(final FutureResponse future) throws Exception {
                    FutureResponse fr2 = sender.pingRPC().pingTCP(recv1.getPeerAddress(), cc1,
                            new DefaultConnectionConfiguration());
                    try {
                        fr2.await();
                    } catch (IllegalStateException ise) {
                        Assert.fail();
                    }
                }
            });
            Timings.sleep(1000);
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            PingRPC handshake = new PingRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            new PingRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingUDP(recv1.getPeerAddress(), cc,
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            PingRPC handshake = new PingRPC(sender.getPeerBean(), sender.getConnectionBean(), false, true,
                    false);
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            new PingRPC(recv1.getPeerBean(), recv1.getConnectionBean(), false, true, false);
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCP(recv1.getPeerAddress(), cc,
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            PingRPC handshake = new PingRPC(sender.getPeerBean(), sender.getConnectionBean(), false, true,
                    true);
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            new PingRPC(recv1.getPeerBean(), recv1.getConnectionBean(), false, true, true);
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCP(recv1.getPeerBean().serverPeerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(false, fr.isSuccess());
            Assert.assertEquals(false, fr.getFailedReason().contains("exception on the other side"));
            Assert.assertEquals(true, fr.getFailedReason().contains("channel is idle"));
            System.err.println("done:" + fr.getFailedReason());
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            PingRPC handshake = new PingRPC(sender.getPeerBean(), sender.getConnectionBean(), false, true,
                    false);
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            new PingRPC(recv1.getPeerBean(), recv1.getConnectionBean(), false, true, false);
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingUDP(recv1.getPeerBean().serverPeerAddress(), cc,
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            PingRPC handshake = new PingRPC(sender.getPeerBean(), sender.getConnectionBean(), false, true,
                    true);
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            new PingRPC(recv1.getPeerBean(), recv1.getConnectionBean(), false, true, true);
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingUDP(recv1.getPeerBean().serverPeerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(false, fr.isSuccess());
            Assert.assertEquals(false, fr.getFailedReason().contains("exception on the other side"));
            Assert.assertEquals(true, fr.getFailedReason().contains("channel is idle"));
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            List<FutureResponse> list = new ArrayList<FutureResponse>(50);
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 50);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();
            for (int i = 0; i < 50; i++) {
                FutureResponse fr = sender.pingRPC().pingTCP(recv1.getPeerAddress(), cc,
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
                p[i] = new PeerMaker(Number160.createHash(i)).p2pId(55).ports(2424 + i).makeAndListen();
            }
            List<FutureResponse> list = new ArrayList<FutureResponse>();
            for (int i = 0; i < p.length; i++) {
                FutureChannelCreator fcc = p[0].getConnectionBean().reservation().create(0, 1);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.getChannelCreator();
                FutureResponse fr = p[0].pingRPC().pingTCP(p[i].getPeerAddress(), cc,
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            long start = System.currentTimeMillis();
            List<FutureResponse> list = new ArrayList<FutureResponse>(100);
            for (int i = 0; i < 20; i++) {
                FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 50);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.getChannelCreator();
                for (int j = 0; j < 50; j++) {
                    FutureResponse fr = sender.pingRPC().pingTCP(recv1.getPeerAddress(), cc,
                            new DefaultConnectionConfiguration());
                    list.add(fr);
                }
                for (FutureResponse fr2 : list) {
                    fr2.awaitUninterruptibly();
                    if (!fr2.isSuccess())
                        System.err.println("fail " + fr2.getFailedReason());
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
                FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(50, 0);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.getChannelCreator();
                for (int j = 0; j < 50; j++) {
                    FutureResponse fr = sender.pingRPC().pingUDP(recv1.getPeerAddress(), cc,
                            new DefaultConnectionConfiguration());
                    list.add(fr);
                }
                int ii = 0;
                for (FutureResponse fr2 : list) {
                    System.err.println("waiting for " + (ii++) + fr2.getRequest());
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = sender.pingRPC().pingTCP(recv1.getPeerAddress(), cc,
                    new DefaultConnectionConfiguration());
            Utils.addReleaseListener(cc, fr);
            fr.awaitUninterruptibly();
            fr.awaitListeners();
            Assert.assertEquals(true, fr.isSuccess());
            FutureResponse fr2 = sender.pingRPC().pingTCP(recv1.getPeerAddress(), cc,
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
