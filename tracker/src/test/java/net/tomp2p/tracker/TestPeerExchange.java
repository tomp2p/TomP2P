package net.tomp2p.tracker;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
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
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            Number160 locationKey = new Number160("0x5555");
            Number160 domainKey = new Number160("0x7777");

            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            sender.peerBean().trackerStorage()
                    .addActive(locationKey, domainKey, sender.peerAddress(), null);
            
            FutureResponse fr = sender.getPeerExchangeRPC().peerExchange(recv1.peerAddress(), locationKey,
                    domainKey, false, cc, new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            if (fr.isFailed()) {
                System.err.println(fr.failedReason());
            }
            Assert.assertEquals(true, fr.isSuccess());
            Timings.sleep(500);
            Assert.assertEquals(1, recv1.peerBean().trackerStorage().sizeSecondary(locationKey, domainKey));
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
