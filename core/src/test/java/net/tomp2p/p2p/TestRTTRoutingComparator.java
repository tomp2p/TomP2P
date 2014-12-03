package net.tomp2p.p2p;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
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


}
