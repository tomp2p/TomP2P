package net.tomp2p.dht;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Test;

public class TestQuit {
    @Test
    public void testGracefulhalt() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            sender.bootstrap().peerAddress(recv1.peerAddress()).start().awaitUninterruptibly();
            Assert.assertEquals(1, sender.peerBean().peerMap().all().size());
            Assert.assertEquals(1, recv1.peerBean().peerMap().allOverflow().size());
            // graceful shutdown
            
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            
            PeerDHT senderDHT = new PeerDHT(sender);
            ShutdownBuilder builder = new ShutdownBuilder(senderDHT);
            
            senderDHT.quitRPC().quit(recv1.peerAddress(), builder, cc);
            sender.shutdown().await();
            // don't care about the sender
            Assert.assertEquals(0, recv1.peerBean().peerMap().all().size());

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
