package net.tomp2p.rpc;


import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ChannelServer;
import net.tomp2p.connection.ClientChannel;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

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
        ChannelServer.resetCounters();
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).enableMaintenance(false).ports(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean());
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).enableMaintenance(false).ports(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean());
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr = handshake.pingUDP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.element0().awaitUninterruptibly();
            Assert.assertEquals(true, fr.element0().isSuccess());
            Assert.assertEquals(1, ChannelServer.packetCounterSend());
            //we shutdown between the 1 and 2 received packet, so it might be 1 or 2
            Assert.assertEquals(true, ChannelServer.packetCounterReceive() == 1 || ChannelServer.packetCounterReceive() == 2);
        } finally {
            if (cc != null) {
                cc.shutdown();
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
    public void testPingUDPKnowPeer() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        ChannelServer.resetCounters();
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).enableMaintenance(false).ports(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean());
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).enableMaintenance(false).ports(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean());
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr = handshake.pingUDP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.element0().awaitUninterruptibly();
            
            fr = handshake.pingUDP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.element0().awaitUninterruptibly();
            
            Assert.assertEquals(true, fr.element0().isSuccess());
            Assert.assertEquals(2, ChannelServer.packetCounterSend());
            Assert.assertEquals(3, ChannelServer.packetCounterReceive());
        } finally {
            if (cc != null) {
                cc.shutdown();
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

            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            final ChannelCreator cc1 = cc;

            Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr = sender.pingRPC().pingUDP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.element0().awaitUninterruptibly();

            fr.element0().addListener(new BaseFutureAdapter<FutureDone<Message>>() {
                @Override
                public void operationComplete(final FutureDone<Message> future) throws Exception {
                	Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr2 = sender.pingRPC().pingUDP(recv1.peerAddress(), cc1,
                            new DefaultConnectionConfiguration());
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
            if (cc != null) {
                cc.shutdown();
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
    public void testPingHandlerError() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).enableMaintenance(false).ports(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean(), false, true,
                    false);
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).enableMaintenance(false).ports(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean(), false, true, false);
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr = handshake.pingUDP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.element0().awaitUninterruptibly();
            Assert.assertEquals(false, fr.element0().isSuccess());
        } finally {
            if (cc != null) {
                cc.shutdown();
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
    public void testPingTimeout() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean(), false, true,
                    true);
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            new PingRPC(recv1.peerBean(), recv1.connectionBean(), false, true, true);
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr = handshake.pingUDP(recv1.peerBean().serverPeerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.element0().awaitUninterruptibly();
            Assert.assertEquals(false, fr.element0().isSuccess());
            System.err.println("done:" + fr.element0().failedReason());
            Assert.assertEquals(true, fr.element0().failedReason().contains("TIMEOUT"));
            
        } finally {
            if (cc != null) {
                cc.shutdown();
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
    public void testPingPool() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            List<Pair<FutureDone<Message>, FutureDone<ClientChannel>>> list = new ArrayList<>(50);
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(50);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            for (int i = 0; i < 50; i++) {
            	Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr = sender.pingRPC().pingUDP(recv1.peerAddress(), cc,
                        new DefaultConnectionConfiguration());
                list.add(fr);
            }
            for (Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr2 : list) {
                fr2.element0().awaitUninterruptibly();
                Assert.assertTrue(fr2.element0().isSuccess());
            }
        } finally {
            if (cc != null) {
                cc.shutdown();
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
            List<Pair<FutureDone<Message>, FutureDone<ClientChannel>>> list = new ArrayList<>();
            for (int i = 0; i < p.length; i++) {
                FutureChannelCreator fcc = p[0].connectionBean().reservation().create(1);
                fcc.awaitUninterruptibly();
                final ChannelCreator cc = fcc.channelCreator();
                Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr = p[0].pingRPC().pingUDP(p[i].peerAddress(), cc,
                        new DefaultConnectionConfiguration());
                fr.element1().addListener(new BaseFutureAdapter<FutureDone<ClientChannel>>() {
					@Override
					public void operationComplete(FutureDone<ClientChannel> future) throws Exception {
						cc.shutdown();
					}
				});
                
                list.add(fr);
            }
            for (Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr2 : list) {
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
            List<Pair<FutureDone<Message>, FutureDone<ClientChannel>>> list = new ArrayList<>(100);
            for (int i = 0; i < 20; i++) {
                FutureChannelCreator fcc = recv1.connectionBean().reservation().create(50);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.channelCreator();
                for (int j = 0; j < 50; j++) {
                	Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr = sender.pingRPC().pingUDP(recv1.peerAddress(), cc,
                            new DefaultConnectionConfiguration());
                    list.add(fr);
                }
                list.clear();
                cc.shutdown();
            }
            System.out.println("UDP time: " + (System.currentTimeMillis() - start));
            for (Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr2 : list) {
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
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.channelCreator();
            Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr = sender.pingRPC().pingUDP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            
            fr.element0().awaitUninterruptibly();
            fr.element1().awaitUninterruptibly();
            cc.shutdown();
            
            Assert.assertEquals(true, fr.element0().isSuccess());
            
            Pair<FutureDone<Message>, FutureDone<ClientChannel>> fr2 = sender.pingRPC().pingUDP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr2.element0().awaitUninterruptibly();
            fr2.element1().awaitUninterruptibly();
            // we have released the reservation here
            // System.err.println(fr2.getFailedReason());
            Assert.assertEquals(false, fr2.element0().isSuccess());

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
