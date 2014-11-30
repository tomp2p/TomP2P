package net.tomp2p.tracker;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import net.tomp2p.futures.FutureTracker;
import net.tomp2p.message.TrackerData;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.VotingSchemeTracker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestTracker {

    @Test
    public void testTracker() throws Exception {
        final Random rnd = new Random(42L);
        PeerTracker master = null;
        try {
            master = new PeerBuilderTracker(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4001).start()).start();
            PeerTracker[] nodes = createNodes(master, 500, rnd);
            // perfect routing
            for (int i = 0; i < nodes.length; i++) {
                for (int j = 0; j < nodes.length; j++)
                    nodes[i].peer().peerBean().peerMap().peerFound(nodes[j].peerAddress(), null, null);
            }
            RoutingConfiguration rc = new RoutingConfiguration(0, 1, 1);
            TrackerConfiguration tc = new TrackerConfiguration(1, 1, 1, 0);
            Number160 trackerID = new Number160(rnd);
            System.err.println("about to store " + trackerID);
            // FutureTracker ft = nodes[300].addToTracker(trackerID, "test",
            // null, rc, tc);
            PeerTracker closest = findClosest(nodes, trackerID);
            FutureTracker ft = nodes[300].addTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc).start();
            ft.awaitUninterruptibly();
            Assert.assertEquals(true, ft.isSuccess());
            Thread.sleep(2000);
            tc = new TrackerConfiguration(1, 1, 0, 1);
            ft = nodes[301].getTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc)
                    .evaluatingScheme(new VotingSchemeTracker()).start();
            ft.awaitUninterruptibly();
            System.err.println(ft.failedReason());
            Assert.assertEquals(true, ft.isSuccess());
            Assert.assertEquals(1, ft.rawPeersOnTracker().size());
        } finally {
            if (master != null) {
                master.peer().shutdown().await();
            }
        }
    }

    private static PeerTracker findClosest(PeerTracker[] nodes, Number160 trackerID) {
    	TreeSet<PeerAddress> set = new TreeSet<PeerAddress>(PeerMap.createComparator(trackerID));
    	for(PeerTracker p:nodes) {
    		set.add(p.peerAddress());
    	}
    	PeerAddress first = set.first();
    	
    	for(PeerTracker p:nodes) {
    		if(p.peerAddress().equals(first)) {
    			return p;
    		}
    	}
	    return null;
    }

	@Test
    public void testTracker2() throws Exception {
        final Random rnd = new Random(42L);
        PeerTracker master = null;
        try {
            master = new PeerBuilderTracker(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4001).start()).verifyPeersOnTracker(false).start();
            PeerTracker[] nodes = createNodes(master, 500, rnd);
            // perfect routing
            for (int i = 0; i < nodes.length; i++) {
                for (int j = 0; j < nodes.length; j++)
                    nodes[i].peer().peerBean().peerMap().peerFound(nodes[j].peerAddress(), null, null);
            }
            RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
            TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0, 1000, 2);
            Number160 trackerID = new Number160(rnd);
            System.err.println("about to store " + trackerID);
            FutureTracker ft = nodes[300].addTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc).start();
            ft.awaitUninterruptibly();
            Assert.assertEquals(true, ft.isSuccess());
            //myself and an other
            Assert.assertEquals(2, ft.directTrackers().size());
            tc = new TrackerConfiguration(1, 1, 1, 3, 1000, 2);
            ft = nodes[301].getTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc)
                    .evaluatingScheme(new VotingSchemeTracker()).start();
            ft.awaitUninterruptibly();
            System.err.println(ft.failedReason());
            Assert.assertEquals(true, ft.isSuccess());
            Assert.assertEquals(1, ft.rawPeersOnTracker().size());
            Assert.assertEquals(1, ft.directTrackers().size());
            Assert.assertEquals(2, ft.potentialTrackers().size());

        } finally {
            if (master != null) {
                master.peer().shutdown().await();
            }
        }
    }

	@Test
    public void testTracker2_5() throws Exception {
        final Random rnd = new Random(42L);
        PeerTracker master = null;
        try {
            master = new PeerBuilderTracker(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4001).start()).verifyPeersOnTracker(false).start();
            PeerTracker[] nodes = createNodes(master, 500, rnd);
            // perfect routing
            for (int i = 0; i < nodes.length; i++) {
                for (int j = 0; j < nodes.length; j++)
                    nodes[i].peer().peerBean().peerMap().peerFound(nodes[j].peerAddress(), null, null);
            }
            RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
            TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0, 1000, 2);
            Number160 trackerID = new Number160(rnd);
            System.err.println("about to store " + trackerID);
            FutureTracker ft = nodes[300].addTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc).start();
            ft.awaitUninterruptibly();
            Assert.assertEquals(true, ft.isSuccess());
            Assert.assertEquals(2, ft.directTrackers().size());
            tc = new TrackerConfiguration(1, 1, 2, 3, 1000, 2);
            ft = nodes[301].getTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc)
                    .evaluatingScheme(new VotingSchemeTracker()).start();
            ft.awaitUninterruptibly();
            Assert.assertEquals(true, ft.isSuccess());
            Assert.assertEquals(2, ft.rawPeersOnTracker().size());
            Assert.assertEquals(1, ft.potentialTrackers().size());
            Assert.assertEquals(2, ft.directTrackers().size());

        } finally {
            if (master != null) {
                master.peer().shutdown().await();
            }
        }
    }

	@Test
    public void testTracker3() throws Exception {
        final Random rnd = new Random(42L);
        PeerTracker master = null;
        try {
            master = new PeerBuilderTracker(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4001).start()).verifyPeersOnTracker(false).start();
            PeerTracker[] nodes = createNodes(master, 500, rnd);
            // perfect routing
            for (int i = 0; i < nodes.length; i++) {
                for (int j = 0; j < nodes.length; j++)
                    nodes[i].peer().peerBean().peerMap().peerFound(nodes[j].peerAddress(), null, null);
            }
            RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
            TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0);
            Number160 trackerID = new Number160(rnd);
            System.err.println("about to store " + trackerID);
            FutureTracker ft = nodes[300].addTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc).start();
            System.err.println("add the peer to the tracker: " + nodes[300].peerAddress());
            ft.awaitUninterruptibly();
            ft = nodes[301].addTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc).start();
            System.err.println("add the peer to the tracker: " + nodes[301].peerAddress());
            ft.awaitUninterruptibly();
            ft = nodes[302].addTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc).start();
            System.err.println("add the peer to the tracker: " + nodes[302].peerAddress());
            ft.awaitUninterruptibly();
            Assert.assertEquals(true, ft.isSuccess());
            Assert.assertEquals(2, ft.directTrackers().size());
            tc = new TrackerConfiguration(1, 1, 2, 2);
            ft = nodes[299].getTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc)
                    .evaluatingScheme(new VotingSchemeTracker()).start();
            ft.awaitUninterruptibly();
            Assert.assertEquals(true, ft.isSuccess());
            // we return there 2 because we use bloomfilters to not return
            // already known peers
            Assert.assertEquals(2, ft.rawPeersOnTracker().size());
            // but here we expect 3 peers, since 3 peers are on the tracker
            Assert.assertEquals(3, ft.rawPeersOnTracker().values().iterator().next().size());
        } finally {
            if (master != null) {
                master.peer().shutdown().await();
            }
        }
    }

	@Test
    public void testTracker4() throws Exception {
        final Random rnd = new Random(42L);
        PeerTracker master = null;
        try {
            master = new PeerBuilderTracker(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4001).start()).verifyPeersOnTracker(false).start();
            PeerTracker[] nodes = createNodes(master, 1000, rnd);
            // perfect routing
            for (int i = 0; i < nodes.length; i++) {
                // nodes[i].getPeerBean().getTrackerStorage()
                // .setTrackerStoreSize(nodes[i].getPeerBean().getTrackerStorage().getTrackerSize());
                for (int j = 0; j < nodes.length; j++)
                    nodes[i].peer().peerBean().peerMap().peerFound(nodes[j].peerAddress(), null, null);
            }
            RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
            // 3 is good!
            TrackerConfiguration tc = new TrackerConfiguration(1, 1, 20, 0, 20, 2);
            Number160 trackerID = new Number160(rnd);
            Set<Number160> tmp = new HashSet<Number160>();
            for (int i = 0; i <= 300; i++) {
                FutureTracker ft = nodes[300 + i].addTracker(trackerID)
                        .domainKey(Number160.createHash("test")).routingConfiguration(rc)
                        .trackerConfiguration(tc).start();
                ft.awaitUninterruptibly();
                System.err.println("added " + nodes[300 + i].peerAddress().peerId() + " on "
                        + ft.directTrackers());
                tmp.add(nodes[300 + i].peerAddress().peerId());
                Assert.assertEquals(true, ft.isSuccess());
                // Assert.assertEquals(true, ft.getDirectTrackers().size() == 2
                // || ft.getDirectTrackers().size() == 3);
            }
            for (int i = 0; i <= 300; i++) {
                FutureTracker ft = nodes[600 - i].addTracker(trackerID)
                        .domainKey(Number160.createHash("test")).routingConfiguration(rc)
                        .trackerConfiguration(tc).start();
                ft.awaitUninterruptibly();
                System.err.println("added " + nodes[300 + i].peerAddress().peerId() + " on "
                        + ft.directTrackers());
                tmp.add(nodes[600 - i].peerAddress().peerId());
                Assert.assertEquals(true, ft.isSuccess());
                // Assert.assertEquals(true, ft.getDirectTrackers().size() == 2
                // || ft.getDirectTrackers().size() == 3);
            }
            tc = new TrackerConfiguration(1, 1, 30, 301, 0, 20);
            FutureTracker ft1 = nodes[299].getTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc)
                    .evaluatingScheme(new VotingSchemeTracker()).start();
            ft1.awaitUninterruptibly();
            Assert.assertEquals(true, ft1.isSuccess());
            for (TrackerData pa : ft1.trackers()) {
                for (PeerAddress pas : pa.peerAddresses().keySet()) {
                    System.err.println("found on DHT1: " + pas.peerId());
                    tmp.remove(pas.peerId());
                }
            }
            // ctg.setUseSecondaryTrackers(true);
            FutureTracker ft2 = nodes[299].getTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc)
                    .evaluatingScheme(new VotingSchemeTracker()).knownPeers(ft1.knownPeers())
                    .start();
            ft2.awaitUninterruptibly();
            System.err.println("Reason: " + ft2.failedReason());
            Assert.assertEquals(true, ft2.isSuccess());
            for (TrackerData pa : ft2.trackers()) {
                for (PeerAddress pas : pa.peerAddresses().keySet()) {
                    if (tmp.remove(pas.peerId()))
                        System.err.println("found on DHT2: " + pas.peerId());
                }
            }
            //
            // for (Number480 n480 : nodes[299].getPeerBean().getTrackerStorage().getKeys(new Number320(trackerID,
            // ctg.getDomain()))) { System.err.println("found locally: " + n480); tmp.remove(n480.getContentKey()); }
            //
            for (Number160 number160 : tmp) {
                System.err.println("not found: " + number160 + " out of 301");
            }
            System.err.println("not found: " + tmp.size());
            Assert.assertEquals(true, tmp.size() < 160);

        } finally {
            if (master != null) {
                master.peer().shutdown().await();
            }
        }
    }

	@Test
    public void testTracker5() throws Exception {
        final Random rnd = new Random(42L);
        PeerTracker master = null;
        try {
            master = new PeerBuilderTracker(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4001).start()).verifyPeersOnTracker(false).start();
            PeerTracker[] nodes = createNodes(master, 500, rnd);
            // perfect routing
            for (int i = 0; i < nodes.length; i++) {
                for (int j = 0; j < nodes.length; j++)
                    nodes[i].peer().peerBean().peerMap().peerFound(nodes[j].peerAddress(), null, null);
            }
            RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
            TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0);
            Number160 trackerID = new Number160(rnd);
            FutureTracker ft = nodes[300].addTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc)
                    .attachement(new Data(",.peoueuaoeue")).start();
            ft.awaitUninterruptibly();
            ft = nodes[301].addTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc)
                    .attachement(new Data(",.peoueuaoeue")).start();
            ft.awaitUninterruptibly();
            ft = nodes[302].addTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc)
                    .attachement(new Data(",.peoueuaoeue")).start();
            ft.awaitUninterruptibly();
            ft = nodes[303].addTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc)
                    .attachement(new Data(",.peoueuaoeue")).start();
            ft.awaitUninterruptibly();
            Assert.assertEquals(true, ft.isSuccess());
            Assert.assertEquals(2, ft.directTrackers().size());
            tc = new TrackerConfiguration(1, 1, 0, 1);
            ft = nodes[199].getTracker(trackerID).domainKey(Number160.createHash("test"))
                    .routingConfiguration(rc).trackerConfiguration(tc).expectAttachement()
                    .evaluatingScheme(new VotingSchemeTracker()).start();
            ft.awaitUninterruptibly();
            Assert.assertEquals(true, ft.isSuccess());
            Assert.assertEquals(1, ft.rawPeersOnTracker().size());
            Assert.assertEquals(",.peoueuaoeue", ft.trackers().iterator().next().peerAddresses()
                    .values().iterator().next().object());
        } finally {
            if (master != null) {
                master.peer().shutdown().await();
            }
        }
    }

    @Test
    public void testTracker6() throws Exception {
        final Random rnd = new Random(42L);
        PeerTracker master = null;
        try {
            master = new PeerBuilderTracker(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4001).start()).verifyPeersOnTracker(false).start();
            Number160 key = new Number160(44);
            FutureTracker future = master.addTracker(key).domainKey(Number160.createHash("test")).start();
            future.awaitUninterruptibly();
            Assert.assertTrue(future.isSuccess());
            //
            FutureTracker future2 = master.getTracker(key).domainKey(Number160.createHash("test")).start();
            future2.awaitUninterruptibly();
            System.err.println(future2.failedReason());
            Assert.assertTrue(future2.isSuccess());
            Assert.assertEquals(future2.peersOnTracker().size(), 1);
        } finally {
            if (master != null) {
                master.peer().shutdown().await();
            }
        }
    }

    @Test
    public void testTrackerResponsibility() throws Exception {
        final Random rnd = new Random(42L);
        PeerTracker master = null;
        try {
            Number160 trackerID = new Number160(rnd);
            Number160 self = new Number160(rnd);
            PeerMapConfiguration pmc = new PeerMapConfiguration(self);
            pmc.peerNoVerification();
            PeerMap pm = new PeerMap(pmc);
            master = new PeerBuilderTracker(new PeerBuilder(self).p2pId(1).ports(4001).peerMap(pm).start()).verifyPeersOnTracker(false).start();

            PeerTracker[] nodes = createNodes(master, 500, rnd);
            FutureTracker futureTracker = nodes[0].addTracker(trackerID).start();
            futureTracker.awaitUninterruptibly();

            // perfect routing
            for (int i = 0; i < nodes.length; i++) {
                for (int j = 0; j < nodes.length; j++) {
                    if (i != j)
                        nodes[i].peer().peerBean().peerMap().peerFound(nodes[j].peerAddress(), null, null);
                }
            }
            FutureTracker ft = nodes[30].getTracker(trackerID).start();
            ft.awaitUninterruptibly();
            Assert.assertEquals(3, ft.trackers().size());
        } finally {
            if (master != null) {
                master.peer().shutdown().await();
            }
        }
    }

    private PeerTracker[] createNodes(PeerTracker master, int nr, Random rnd) throws Exception {

    	PeerTracker[] nodes = new PeerTracker[nr+1];
        for (int i = 0; i < nr; i++) {
            nodes[i+1] = new PeerBuilderTracker(new PeerBuilder(new Number160(rnd)).p2pId(1).masterPeer(master.peer()).start()).verifyPeersOnTracker(false).start();
        }
        nodes[0] = master;
        return nodes;
    }
}