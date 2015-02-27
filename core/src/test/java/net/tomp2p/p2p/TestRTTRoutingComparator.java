package net.tomp2p.p2p;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.peers.RTT;

import org.junit.Assert;
import org.junit.Test;

public class TestRTTRoutingComparator {

    /**
     * Two PeerStatistic with the same PeerIDs should compare to 0 (equality)
     */
    @Test
    public void testEquality() {
        PeerAddress peer1 = new PeerAddress(new Number160("0xa3fb3982c38193f12c40a3fb3982c38193f12c60"));
        PeerStatistic peer1Statistic = new PeerStatistic(peer1).addRTT(new RTT(13, false).setEstimated());
        PeerAddress peer2 = new PeerAddress(new Number160("0xa3fb3982c38193f12c40a3fb3982c38193f12c60"));
        PeerStatistic peer2Statistic = new PeerStatistic(peer2).addRTT(new RTT(44, true));

        Comparator<PeerStatistic> comp = new RTTPeerStatisticComparator().getComparator(new Number160("0xfff"));

        int compare = comp.compare(peer1Statistic,peer2Statistic);

        Assert.assertEquals(compare, 0);
    }

    /**
     * Two PeerStatistic with the same metrics but different PeerIDs should not compare to 0.
     */
    @Test
    public void testUnequality() {
        PeerAddress peer1 = new PeerAddress(new Number160("0xa3fb3982c38193f12c40a3fb3982c38193f12c61"));
        PeerStatistic peer1Statistic = new PeerStatistic(peer1).addRTT(new RTT(44, true));
        PeerAddress peer2 = new PeerAddress(new Number160("0xa3fb3982c38193f12c40a3fb3982c38193f12c62"));
        PeerStatistic peer2Statistic = new PeerStatistic(peer2).addRTT(new RTT(44, true));

        Comparator<PeerStatistic> comp = new RTTPeerStatisticComparator().getComparator(new Number160("0xfff"));

        int compare = comp.compare(peer1Statistic,peer2Statistic);

        Assert.assertNotEquals(compare, 0);
    }

    /**
     * A queue with the comparator should result in the correct order when polling
     */
    @Test
    public void testQueueToAskOrder() {
        // Target location
        Number160 location =                new Number160("0xa3fb3982c38193f12c40a3fb3982c38193f12c40");

        // 4 Peers with different (bucket)distances to location
        PeerAddress peer1 = new PeerAddress(new Number160("0xa3fb3982c38193f12c40a3fb3982c38193f12c60"));
        PeerAddress peer2 = new PeerAddress(new Number160("0xa3fb3982c38193f12c40a3fb3982000000000000"));
        PeerAddress peer3 = new PeerAddress(new Number160("0xa3fb3982c38193f12c40a3fb3982700000000000"));
        PeerAddress peer4 = new PeerAddress(new Number160("0xa3fb3982c38193f12c40a3fb3982c38193f12c70"));

        // Test all bucket distances to location (this will be the first sorting criteria)
        Assert.assertEquals(6, PeerMap.classMember(peer1.peerId(), location) + 1);
        Assert.assertEquals(48, PeerMap.classMember(peer2.peerId(), location) + 1);
        Assert.assertEquals(48, PeerMap.classMember(peer3.peerId(), location) + 1);
        Assert.assertEquals(6, PeerMap.classMember(peer4.peerId(), location) + 1);

        // Define Peer Statistics with some RTTs
        PeerStatistic stat1 = new PeerStatistic(peer1).addRTT(new RTT(80, true));
        PeerStatistic stat2 = new PeerStatistic(peer2).addRTT(new RTT(30, true));
        PeerStatistic stat3 = new PeerStatistic(peer3).addRTT(new RTT(5, true)) .addRTT(new RTT(45, true));
        PeerStatistic stat4 = new PeerStatistic(peer4); // no rtt available

        // Create routing queue with comparator and add all statistics
        UpdatableTreeSet<PeerStatistic> queueToAsk = new UpdatableTreeSet<PeerStatistic>(new RTTPeerStatisticComparator().getComparator(location));
        queueToAsk.addAll(Arrays.asList(stat1,stat2,stat3,stat4));

        // Sorting should be in the following priority order
        // 1. KAD bucket distance (how many high-order bits are equal to destination)
        //        (Note that this is different from the complete XOR distance)
        // 2. RTT information available: Those with RTT come before those without RTT
        // 3. RTT Faster peers (low RTT) before slow peers (high RTT)
        // 4. Complete XOR Distance
        Iterator<PeerStatistic> it = queueToAsk.iterator();
        Assert.assertEquals(queueToAsk.pollFirst().peerAddress(), peer1); // Dist 6, 80 RTT
        Assert.assertEquals(queueToAsk.pollFirst().peerAddress(), peer4); // Dist 6, no RTT info
        Assert.assertEquals(queueToAsk.pollFirst().peerAddress(), peer3); // Dist 48 25 RTT
        Assert.assertEquals(queueToAsk.pollFirst().peerAddress(), peer2); // Dist 48 30ms RTT
    }

    @Test
    public void testRouting() throws IOException, InterruptedException {
        Peer startPeer = null;
        ChannelCreator cc = null;
        try {
            // Create Peer from which we start a routing request with the RTT comparator
            Number160 peerID = new Number160("0x1");
            PeerMapConfiguration peerMapConfiguration = new PeerMapConfiguration(peerID);
            peerMapConfiguration.setPeerStatisticComparator(new RTTPeerStatisticComparator());
            PeerMap peerMap = new PeerMap(peerMapConfiguration);
            startPeer = new PeerBuilder(peerID).peerMap(peerMap).ports(4001).start();

            // Create routing location and two peers
            Number160 location =         new Number160("0xff0000");
            Peer peer1 = new PeerBuilder(new Number160("0xffffff")).masterPeer(startPeer).start();
            Peer peer2 = new PeerBuilder(new Number160("0xfff000")).masterPeer(startPeer).start();

            // Make sure that both peers have the same bucket distance
            Assert.assertEquals(0, PeerMap.classCloser(location, peer2.peerAddress(), peer1.peerAddress()));

            // ... but peer2 is closer in the full XOR distance
            Assert.assertEquals(-1, PeerMap.isKadCloser(location, peer2.peerAddress(), peer1.peerAddress()));

            // Add the two peers with a slow and fast response time to the peer's PeerMap
            startPeer.peerBean().peerMap().peerFound(peer2.peerAddress(), null, null, new RTT(100, true));
            startPeer.peerBean().peerMap().peerFound(peer1.peerAddress(), null, null, new RTT(70, true));

            // The peer1 has better RTT and should be the first in the queue of close peers to the location
            NavigableSet<PeerStatistic> closePeers = startPeer.peerBean().peerMap().closePeers(location, 2);
            Assert.assertEquals(closePeers.pollFirst().peerAddress(), peer1.peerAddress());
            Assert.assertEquals(closePeers.pollFirst().peerAddress(), peer2.peerAddress());

            // The peer2 gets an updated RTT (new average: 60ms)
            startPeer.peerBean().peerMap().peerFound(peer2.peerAddress(), null, null, new RTT(10, true));

            // ... and should now be the first in the queue since 60 ms < 70 ms
            closePeers = startPeer.peerBean().peerMap().closePeers(location, 2);
            Assert.assertEquals(closePeers.pollFirst().peerAddress(), peer2.peerAddress());
            Assert.assertEquals(closePeers.pollFirst().peerAddress(), peer1.peerAddress());

            // Start an actual routing
            FutureChannelCreator fcc = startPeer.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            RoutingBuilder routingBuilder = new RoutingBuilder();
            routingBuilder.locationKey(location);
            routingBuilder.maxDirectHits(0);
            routingBuilder.setMaxNoNewInfo(0);
            routingBuilder.maxFailures(0);
            routingBuilder.maxSuccess(1); // Only allow 1 success
            routingBuilder.parallel(1);   // ... and 1 request at a time
            FutureRouting fr = startPeer.distributedRouting().route(routingBuilder, Message.Type.REQUEST_1, cc);
            fr.awaitUninterruptibly();

            // Best hit is the faster peer (peer2)
            Assert.assertEquals(peer2.peerAddress(), fr.potentialHits().pollFirst() );

            // The startPeer should only have contacted peer2 and not peer1 (because of maxSuccess=1, parallel=1)
            Assert.assertTrue(peer2.peerBean().peerMap().containsOverflow(startPeer.peerAddress()));
            Assert.assertFalse(peer1.peerBean().peerMap().containsOverflow(startPeer.peerAddress()));


        } finally {
            cc.shutdown().await();
            startPeer.shutdown().awaitUninterruptibly();
        }


    }

}
