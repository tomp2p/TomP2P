package net.tomp2p.rpc;

import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Timings;

import org.junit.Assert;
import org.junit.Test;

public class TestPeerExchange {
    @Test
    public void testPex() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            Number160 locationKey = new Number160("0x5555");
            Number160 domainKey = new Number160("0x7777");

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            sender.getPeerBean().trackerStorage()
                    .addActive(locationKey, domainKey, sender.getPeerAddress(), null);
            
            FutureResponse fr = sender.getPeerExchangeRPC().peerExchange(recv1.getPeerAddress(), locationKey,
                    domainKey, false, cc, new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            if (fr.isFailed()) {
                System.err.println(fr.getFailedReason());
            }
            Assert.assertEquals(true, fr.isSuccess());
            Timings.sleep(500);
            Assert.assertEquals(1, recv1.getPeerBean().trackerStorage().sizeSecondary(locationKey, domainKey));
        } catch (Exception e) {
            e.printStackTrace();
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
}
