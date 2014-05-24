package net.tomp2p.rpc;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.p2p.builder.ShutdownBuilder;
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
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            sender.bootstrap().setPeerAddress(recv1.getPeerAddress()).start().awaitUninterruptibly();
            Assert.assertEquals(1, sender.getPeerBean().peerMap().getAll().size());
            Assert.assertEquals(1, recv1.getPeerBean().peerMap().getAllOverflow().size());
            // graceful shutdown
            
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            
            ShutdownBuilder builder = new ShutdownBuilder(sender);
            
            FutureResponse fr = sender.getQuitRPC().quit(recv1.getPeerAddress(), builder, cc);
            sender.shutdown().await();
            // don't care about the sender
            Assert.assertEquals(0, recv1.getPeerBean().peerMap().getAll().size());

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
