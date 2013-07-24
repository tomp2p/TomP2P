package net.tomp2p.rpc;

import net.tomp2p.connection.ChannelCreator;
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
        try {
            sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).makeAndListen();
            Number160 locationKey = new Number160("0x5555");
            Number160 domainKey = new Number160("0x7777");
            FutureChannelCreator fcc = recv1.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            sender.getPeerBean().getTrackerStorage()
                    .addActive(locationKey, domainKey, sender.getPeerAddress(), null, 0, 0);
            FutureResponse fr = sender.getPeerExchangeRPC().peerExchange(recv1.getPeerAddress(), locationKey,
                    domainKey, false, cc, false);
            fr.awaitUninterruptibly();
            if (fr.isFailed())
                System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());
            Timings.sleep(200);
            Assert.assertEquals(1, recv1.getPeerBean().getTrackerStorage().sizeSecondary(locationKey, domainKey));
            recv1.getConnectionBean().getConnectionReservation().release(cc);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (sender != null)
                sender.halt();
            if (recv1 != null)
                recv1.halt();
        }
    }
}
