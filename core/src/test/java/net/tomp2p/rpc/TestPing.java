package net.tomp2p.rpc;


import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelTransceiver;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.network.KCP;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number256;
import net.tomp2p.utils.Pair;

import java.util.ArrayList;
import java.util.List;

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
    public void testPingUDP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number256("0x9876")).p2pId(55).enableMaintenance(false).port(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean());
            recv1 = new PeerBuilder(new Number256("0x1234")).p2pId(55).enableMaintenance(false).port(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean());
            Pair<FutureDone<Message>, KCP> fr = handshake.pingUDP(recv1.peerAddress());
            fr.element0().awaitUninterruptibly();
            System.err.println(fr.element0().failedReason());
            Assert.assertEquals(true, fr.element0().isSuccess());
            Assert.assertEquals(3, ChannelTransceiver.packetCounterSend());
            //we shutdown between the 1 and 2 received packet, so it might be 1 or 2
            Assert.assertEquals(true, ChannelTransceiver.packetCounterReceive() == 3 || ChannelTransceiver.packetCounterReceive() == 2);
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
            ChannelTransceiver.resetCounters();
        }
    }
    
    @Test
    public void testPingUDPKnowPeer() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number256("0x9876")).p2pId(55).enableMaintenance(false).port(9876).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean());
            recv1 = new PeerBuilder(new Number256("0x1234")).p2pId(55).enableMaintenance(false).port(1234).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean());
            Pair<FutureDone<Message>, KCP>  fr = handshake.pingUDP(recv1.peerAddress());
            fr.element0().awaitUninterruptibly();
            
            
            fr = handshake.pingUDP(recv1.peerAddress());
            
            fr.element0().awaitUninterruptibly();
            
            Assert.assertEquals(true, fr.element0().isSuccess());
            Assert.assertEquals(5, ChannelTransceiver.packetCounterSend());
            Assert.assertEquals(5, ChannelTransceiver.packetCounterReceive());
            
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
            ChannelTransceiver.resetCounters();
        }
    }
    

    @Test
    public void testPingTCPDeadLock2() throws Exception {
        Peer sender1 = null;
        Peer recv11 = null;
        try {
            final Peer sender = new PeerBuilder(new Number256("0x9876")).p2pId(55).port(2424).start();
            sender1 = sender;
            final Peer recv1 = new PeerBuilder(new Number256("0x1234")).p2pId(55).port(8088).start();
            recv11 = recv1;



            Pair<FutureDone<Message>, KCP> fr = sender.pingRPC().pingUDP(recv1.peerAddress());
            fr.element0().awaitUninterruptibly();

            fr.element0().addListener(new BaseFutureAdapter<FutureDone<Message>>() {
                @Override
                public void operationComplete(final FutureDone<Message> future) throws Exception {
                	Pair<FutureDone<Message>, KCP> fr2 = sender.pingRPC().pingUDP(recv1.peerAddress());
                    try {
                        fr2.element0().await();
                    } catch (IllegalStateException ise) {
                        Assert.fail();
                    }
                }
            });
            Thread.sleep(1000);
            Assert.assertEquals(true, fr.element0().isSuccess());
        } finally {
            if (sender1 != null) {
                sender1.shutdown().await();
            }
            if (recv11 != null) {
                recv11.shutdown().await();
            }
            ChannelTransceiver.resetCounters();
        }
    }

    @Test
    public void testPingHandlerError() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number256("0x9876")).p2pId(55).enableMaintenance(false).port(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean(), false, true,
                    false);
            recv1 = new PeerBuilder(new Number256("0x1234")).p2pId(55).enableMaintenance(false).port(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean(), false, true, false);
            Pair<FutureDone<Message>, KCP> fr = handshake.pingUDP(recv1.peerAddress());
            fr.element0().awaitUninterruptibly();
            Assert.assertEquals(false, fr.element0().isSuccess());
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
            ChannelTransceiver.resetCounters();
        }
    }

    @Test
    public void testPingTimeout() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number256("0x9876")).p2pId(55).port(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean(), false, true,
                    true);
            recv1 = new PeerBuilder(new Number256("0x1234")).p2pId(55).port(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean(), false, true, true);
            Pair<FutureDone<Message>, KCP> fr = handshake.pingUDP(recv1.peerBean().serverPeerAddress());
            fr.element0().awaitUninterruptibly();
            Assert.assertEquals(false, fr.element0().isSuccess());
            System.err.println("done:" + fr.element0().failedReason());
            Assert.assertEquals(true, fr.element0().failedReason().contains("timeout"));
            
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
            ChannelTransceiver.resetCounters();
        }
    }

    @Test
    public void testPingPool() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number256("0x9876")).p2pId(55).port(2424).start();
            recv1 = new PeerBuilder(new Number256("0x1234")).p2pId(55).port(8088).start();
            List<Pair<FutureDone<Message>, KCP>> list = new ArrayList<>(50);
            for (int i = 0; i < 50; i++) {
            	Pair<FutureDone<Message>, KCP> fr = sender.pingRPC().pingUDP(recv1.peerAddress());
                list.add(fr);
            }
            for (Pair<FutureDone<Message>, KCP> fr2 : list) {
                fr2.element0().awaitUninterruptibly();
                System.err.println(fr2.element0().failedReason());
                Assert.assertTrue(fr2.element0().isSuccess());
            }
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
            ChannelTransceiver.resetCounters();
        }
    }

    @Test
    public void testPingTCPPool2() throws Exception {
        Peer p[] = new Peer[50];
        try {
            for (int i = 0; i < p.length; i++) {
                p[i] = new PeerBuilder(Number256.createHash(""+i)).p2pId(55).port(2424 + i).start();
            }
            List<Pair<FutureDone<Message>, KCP>> list = new ArrayList<>();
            for (int i = 0; i < p.length; i++) {
            	Pair<FutureDone<Message>, KCP> fr = p[0].pingRPC().pingUDP(p[i].peerAddress());
                
                
                list.add(fr);
            }
            for (Pair<FutureDone<Message>, KCP> fr2 : list) {
                fr2.element0().awaitUninterruptibly();
                boolean success = fr2.element0().isSuccess();
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
            ChannelTransceiver.resetCounters();
        }
    }

    @Test
    public void testPingTime() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number256("0x9876")).p2pId(55).port(2424).start();
            recv1 = new PeerBuilder(new Number256("0x1234")).p2pId(55).port(8088).start();
            long start = System.currentTimeMillis();
            List<Pair<FutureDone<Message>, KCP>> list = new ArrayList<>(100);
            for (int i = 0; i < 20; i++) {
                for (int j = 0; j < 50; j++) {
                	Pair<FutureDone<Message>, KCP> fr = sender.pingRPC().pingUDP(recv1.peerAddress());
                    list.add(fr);
                }
                list.clear();
            }
            System.out.println("UDP time: " + (System.currentTimeMillis() - start));
            for (Pair<FutureDone<Message>, KCP> fr2 : list) {
                fr2.element0().awaitUninterruptibly();
                Assert.assertEquals(true, fr2.element0().isSuccess());
            }
            
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
            ChannelTransceiver.resetCounters();
        }
    }
}
