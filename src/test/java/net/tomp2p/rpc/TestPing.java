package net.tomp2p.rpc;

import java.util.ArrayList;
import java.util.List;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerListener;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.HandshakeRPC;
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
        try {
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            FutureChannelCreator fcc = recv1.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            recv1.getConnectionBean().getConnectionReservation().release(cc);
        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
        }
    }

    @Test
    public void testPingTCP2() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            FutureChannelCreator fcc1 = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc1.awaitUninterruptibly();
            ChannelCreator cc1 = fcc1.getChannelCreator();
            FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc1);
            fr.awaitUninterruptibly();
            FutureChannelCreator fcc2 = recv1.getConnectionBean().getConnectionReservation().reserve(1);
            fcc2.awaitUninterruptibly();
            ChannelCreator cc2 = fcc2.getChannelCreator();
            FutureResponse fr2 = recv1.getHandshakeRPC().pingTCP(sender.getPeerAddress(), cc2);
            fr2.awaitUninterruptibly();
            Assert.assertEquals(true, fr2.isSuccess());
            sender.getConnectionBean().getConnectionReservation().release(cc1);
            recv1.getConnectionBean().getConnectionReservation().release(cc2);
        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
        }
    }

    @Test
    public void testPingTCPDeadLock2() throws Exception {
        Peer sender1 = null;
        Peer recv11 = null;
        try {
            final Peer sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            sender1 = sender;
            final Peer recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            recv11 = recv1;

            FutureChannelCreator fcc1 = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc1.awaitUninterruptibly();
            ChannelCreator cc1 = fcc1.getChannelCreator();
            FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc1);
            fr.awaitUninterruptibly();
            FutureChannelCreator fcc2 = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc2.awaitUninterruptibly();
            final ChannelCreator cc2 = fcc2.getChannelCreator();
            fr.addListener(new BaseFutureAdapter<FutureResponse>() {
                @Override
                public void operationComplete(FutureResponse future) throws Exception {
                    FutureResponse fr2 = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc2);
                    try {
                        fr2.await();
                    } catch (IllegalStateException ise) {
                        Assert.fail();
                    }
                }
            });
            Timings.sleep(1000);
            Assert.assertEquals(true, fr.isSuccess());
            sender.getConnectionBean().getConnectionReservation().release(cc1);
            sender.getConnectionBean().getConnectionReservation().release(cc2);
        } finally {
            if (sender1 != null)
                sender1.halt();
            if (recv11 != null)
                recv11.halt();
        }
    }

    @Test
    public void testPingUDP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            new HandshakeRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingUDP(recv1.getPeerAddress(), cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            sender.getConnectionBean().getConnectionReservation().release(cc);
        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
        }
    }

    @Test
    public void testPingTimeoutTCP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender.getConnectionBean(),
                    new ArrayList<PeerListener>(), false, true, false);
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            new HandshakeRPC(recv1.getPeerBean(), recv1.getConnectionBean(), new ArrayList<PeerListener>(), false,
                    true, false);
            final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCP(recv1.getPeerBean().getServerPeerAddress(), cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(false, fr.isSuccess());
            sender.getConnectionBean().getConnectionReservation().release(cc);
        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
        }
    }

    @Test
    public void testPingTimeoutTCP2() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender.getConnectionBean(),
                    new ArrayList<PeerListener>(), false, true, true);
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            new HandshakeRPC(recv1.getPeerBean(), recv1.getConnectionBean(), new ArrayList<PeerListener>(), false,
                    true, true);
            final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCP(recv1.getPeerBean().getServerPeerAddress(), cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(false, fr.isSuccess());
            sender.getConnectionBean().getConnectionReservation().release(cc);
            System.err.println("done");
        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
        }
    }

    @Test
    public void testPingTimeoutTCP3() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender.getConnectionBean(),
                    new ArrayList<PeerListener>(), false, true, true);
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            new HandshakeRPC(recv1.getPeerBean(), recv1.getConnectionBean(), new ArrayList<PeerListener>(), false,
                    true, true);
            final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCP(recv1.getPeerBean().getServerPeerAddress(), cc);
            String error = "##Test Failure**";
            sender.setFutureTimeout(fr, 500, error);
            fr.awaitUninterruptibly();
            Assert.assertEquals(false, fr.isSuccess());
            Assert.assertEquals(true, fr.getFailedReason().indexOf(error) > 0);
            sender.getConnectionBean().getConnectionReservation().release(cc);
        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
        }
    }

    @Test
    public void testPingTimeoutUDP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender.getConnectionBean(),
                    new ArrayList<PeerListener>(), false, true, false);
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            new HandshakeRPC(recv1.getPeerBean(), recv1.getConnectionBean(), new ArrayList<PeerListener>(), false,
                    true, false);
            final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingUDP(recv1.getPeerBean().getServerPeerAddress(), cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(false, fr.isSuccess());
            sender.getConnectionBean().getConnectionReservation().release(cc);
        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
        }
    }

    @Test
    public void testPingTCPPool() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            List<FutureResponse> list = new ArrayList<FutureResponse>(50);
            final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(50);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            for (int i = 0; i < 50; i++) {
                FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc);
                list.add(fr);
            }
            for (FutureResponse fr2 : list) {
                fr2.awaitUninterruptibly();
                Assert.assertTrue(fr2.isSuccess());
            }
            sender.getConnectionBean().getConnectionReservation().release(cc);
        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
        }
    }

    @Test
    public void testPingTCPPool2() throws Exception {
        Peer p[] = new Peer[50];
        try {
            for (int i = 0; i < p.length; i++) {
                p[i] = new PeerMaker(Number160.createHash(i)).setP2PId(55).setPorts(2424 + i).makeAndListen();
            }
            List<FutureResponse> list = new ArrayList<FutureResponse>();
            for (int i = 0; i < p.length; i++) {
                final FutureChannelCreator fcc = p[0].getConnectionBean().getConnectionReservation().reserve(1);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.getChannelCreator();
                FutureResponse fr = p[0].getHandshakeRPC().pingTCP(p[i].getPeerAddress(), cc);
                Utils.addReleaseListenerAll(fr, p[0].getConnectionBean().getConnectionReservation(), cc);
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
                p[i].halt();
            }
        }
    }

    @Test
    public void testPingTime() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            long start = System.currentTimeMillis();
            List<FutureResponse> list = new ArrayList<FutureResponse>(100);
            for (int i = 0; i < 20; i++) {
                final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(50);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.getChannelCreator();
                for (int j = 0; j < 50; j++) {
                    FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc);
                    list.add(fr);
                }
                for (FutureResponse fr2 : list) {
                    fr2.awaitUninterruptibly();
                    if (!fr2.isSuccess())
                        System.err.println("fail " + fr2.getFailedReason());
                    Assert.assertEquals(true, fr2.isSuccess());
                }
                list.clear();
                sender.getConnectionBean().getConnectionReservation().release(cc);
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
                final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(50);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.getChannelCreator();
                for (int j = 0; j < 50; j++) {
                    FutureResponse fr = sender.getHandshakeRPC().pingUDP(recv1.getPeerAddress(), cc);
                    list.add(fr);
                }
                for (FutureResponse fr2 : list) {
                    fr2.awaitUninterruptibly();
                    Assert.assertEquals(true, fr2.isSuccess());
                }
                list.clear();
                sender.getConnectionBean().getConnectionReservation().release(cc);
            }

            System.out.println("UDP time: " + (System.currentTimeMillis() - start));
        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
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
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            FutureChannelCreator fcc = recv1.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc);
            Utils.addReleaseListenerAll(fr, recv1.getConnectionBean().getConnectionReservation(), cc);
            fr.awaitUninterruptibly();
            fr.awaitListeners();
            Assert.assertEquals(true, fr.isSuccess());
            FutureResponse fr2 = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc);
            fr2.awaitUninterruptibly();
            fr2.awaitListeners();
            // we have released the reservation here
            // System.err.println(fr2.getFailedReason());
            Assert.assertEquals(false, fr2.isSuccess());

        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
        }
    }
}
