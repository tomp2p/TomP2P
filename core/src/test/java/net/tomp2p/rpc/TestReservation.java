package net.tomp2p.rpc;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestReservation {

	@Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };

    @Test
    public void testReservationTCP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();

            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 3);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            for (int i = 0; i < 1000; i++) {
                FutureResponse fr1 = sender.pingRPC().pingTCP(recv1.peerAddress(), cc, new DefaultConnectionConfiguration());
                FutureResponse fr2 = sender.pingRPC().pingTCP(recv1.peerAddress(), cc, new DefaultConnectionConfiguration());
                FutureResponse fr3 = sender.pingRPC().pingTCP(recv1.peerAddress(), cc, new DefaultConnectionConfiguration());
                fr1.awaitUninterruptibly();
                fr2.awaitUninterruptibly();
                fr3.awaitUninterruptibly();
                System.err.println(fr1.failedReason() + " / " + fr2.failedReason() + " / "
                        + fr3.failedReason());
                Assert.assertEquals(true, fr1.isSuccess());
                Assert.assertEquals(true, fr2.isSuccess());
                Assert.assertEquals(true, fr3.isSuccess());
            }
        } catch (Throwable t) {
            t.printStackTrace();
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

    /*
     * @Test public void testReservationUDPL() throws Exception { for(int i=0;i<100;i++) testReservationUDP(); }
     */

    @Test
    public void testReservationUDP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();

            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(3, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            for (int i = 0; i < 1000; i++) {
                FutureResponse fr1 = sender.pingRPC().pingUDP(recv1.peerAddress(), cc, new DefaultConnectionConfiguration());
                FutureResponse fr2 = sender.pingRPC().pingUDP(recv1.peerAddress(), cc, new DefaultConnectionConfiguration());
                FutureResponse fr3 = sender.pingRPC().pingUDP(recv1.peerAddress(), cc, new DefaultConnectionConfiguration());
                fr1.awaitUninterruptibly();
                fr2.awaitUninterruptibly();
                fr3.awaitUninterruptibly();
                System.err.println(fr1.failedReason() + " / " + fr2.failedReason() + " / "
                        + fr3.failedReason());
                Assert.assertEquals(true, fr1.isSuccess());
                Assert.assertEquals(true, fr2.isSuccess());
                Assert.assertEquals(true, fr3.isSuccess());
            }
        } catch (Throwable t) {
            t.printStackTrace();
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
}
