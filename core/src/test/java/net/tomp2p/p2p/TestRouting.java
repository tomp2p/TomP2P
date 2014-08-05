package net.tomp2p.p2p;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import net.tomp2p.Utils2;
import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureWrapper;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRouting {
    private static final Logger LOG = LoggerFactory.getLogger(TestRouting.class);
    final private static Random rnd = new Random(43L);

    @Test
    public void testMerge() throws UnknownHostException {
        // setup
        SortedSet<PeerAddress> queue = new TreeSet<PeerAddress>(PeerMap.createComparator(new Number160(88)));
        SortedSet<PeerAddress> neighbors = new TreeSet<PeerAddress>(
                PeerMap.createComparator(new Number160(88)));
        SortedSet<PeerAddress> already = new TreeSet<PeerAddress>(PeerMap.createComparator(new Number160(88)));
        queue.add(Utils2.createAddress(12));
        queue.add(Utils2.createAddress(14));
        queue.add(Utils2.createAddress(16));
        neighbors.add(Utils2.createAddress(88));
        neighbors.add(Utils2.createAddress(12));
        neighbors.add(Utils2.createAddress(16));
        // do testing and verification
        already.add(Utils2.createAddress(16));
        boolean testb = RoutingMechanism.merge(queue, neighbors, already);
        Assert.assertEquals(true, testb);
        // next one
        neighbors.add(Utils2.createAddress(89));
        testb = RoutingMechanism.merge(queue, neighbors, already);
        Assert.assertEquals(false, testb);
        // next one
        neighbors.add(Utils2.createAddress(88));
        testb = RoutingMechanism.merge(queue, neighbors, already);
        Assert.assertEquals(false, testb);
    }

    @Test
    public void testEvaluate() throws UnknownHostException {
        // setup
        SortedSet<PeerAddress> queue = new TreeSet<PeerAddress>(PeerMap.createComparator(new Number160(88)));
        SortedSet<PeerAddress> neighbors = new TreeSet<PeerAddress>(
                PeerMap.createComparator(new Number160(88)));
        SortedSet<PeerAddress> already = new TreeSet<PeerAddress>(PeerMap.createComparator(new Number160(88)));
        queue.add(Utils2.createAddress(12));
        queue.add(Utils2.createAddress(14));
        queue.add(Utils2.createAddress(16));
        neighbors.add(Utils2.createAddress(89));
        neighbors.add(Utils2.createAddress(12));
        neighbors.add(Utils2.createAddress(16));
        already.add(Utils2.createAddress(16));
        RoutingMechanism routingMechanism = new RoutingMechanism(null, null, null);
        // do testing and verification
        // AtomicInteger nrNoNewInformation = new AtomicInteger();
        boolean testb = routingMechanism.evaluateInformation(neighbors, queue, already, 0);
        Assert.assertEquals(0, routingMechanism.nrNoNewInfo());
        Assert.assertEquals(false, testb);
        testb = routingMechanism.evaluateInformation(neighbors, queue, already, 2);
        Assert.assertEquals(1, routingMechanism.nrNoNewInfo());
        Assert.assertEquals(false, testb);
        neighbors.add(Utils2.createAddress(11));
        testb = routingMechanism.evaluateInformation(neighbors, queue, already, 2);
        Assert.assertEquals(2, routingMechanism.nrNoNewInfo());
        Assert.assertEquals(true, testb);
        neighbors.add(Utils2.createAddress(88));
        testb = routingMechanism.evaluateInformation(neighbors, queue, already, 2);
        Assert.assertEquals(0, routingMechanism.nrNoNewInfo());
        Assert.assertEquals(false, testb);
        //
        testb = routingMechanism.evaluateInformation(neighbors, queue, already, 2);
        Assert.assertEquals(1, routingMechanism.nrNoNewInfo());
        neighbors.add(Utils2.createAddress(89));
        testb = routingMechanism.evaluateInformation(neighbors, queue, already, 2);
        Assert.assertEquals(2, routingMechanism.nrNoNewInfo());
        neighbors.add(Utils2.createAddress(88));
        testb = routingMechanism.evaluateInformation(neighbors, queue, already, 2);
        Assert.assertEquals(3, routingMechanism.nrNoNewInfo());
        Assert.assertEquals(true, testb);
    }

    @Test
    public void testRouting1() throws Exception {
        for (int i = 0; i < 20; i++) {
            testRouting1(true, Type.REQUEST_1);
            testRouting1(false, Type.REQUEST_1);
            testRouting1(true, Type.REQUEST_2);
            testRouting1(false, Type.REQUEST_2);
        }
    }

    private void testRouting1(boolean tcp, Type request) throws Exception {
        Peer[] peers = null;
        ChannelCreator cc = null;
        try {
            // setup
            peers = createSpecialPeers(7);
            addToPeerMap(peers[0], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[1], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress());
            addToPeerMap(peers[2], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress(), peers[3].peerAddress());
            addToPeerMap(peers[3], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress(), peers[3].peerAddress(), peers[4].peerAddress());
            addToPeerMap(peers[4], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress(), peers[3].peerAddress(), peers[4].peerAddress(),
                    peers[5].peerAddress());
            // do testing
            if (tcp) {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(0, 2);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            } else {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(2, 0);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            }

            RoutingBuilder routingBuilder = new RoutingBuilder();
            if (tcp) {
                routingBuilder.forceTCP(true);
            }
            routingBuilder.locationKey(peers[6].peerID());
            routingBuilder.maxDirectHits(0);
            routingBuilder.setMaxNoNewInfo(0);
            routingBuilder.maxFailures(0);
            routingBuilder.maxSuccess(100);
            routingBuilder.parallel(2);

            FutureRouting fr = peers[0].distributedRouting().route(routingBuilder, request, cc);

            // new RoutingBuilder(peers[6].getPeerID(), null, null, 0, 0, 0, 100, 2, false), Type.REQUEST_2, cc);
            fr.awaitUninterruptibly();
            // do verification
            Assert.assertEquals(true, fr.isSuccess());
            SortedSet<PeerAddress> ns = fr.potentialHits();
            Assert.assertEquals(peers[5].peerAddress(), ns.first());
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            for (Peer n : peers) {
                n.shutdown().await();
            }
        }
    }

    @Test
    public void testRouting2() throws Exception {
        for (int i = 0; i < 20; i++) {
            LOG.error("round "+i);
            //testRouting2(true, Type.REQUEST_1);
            //testRouting2(false, Type.REQUEST_1);
            //testRouting2(true, Type.REQUEST_2);
            testRouting2(false, Type.REQUEST_2);
        }
    }

    /**
     * In this test we have a peer that cannot be reached: Utils2.createAddress("0xffffff")
     * @param tcp
     * @param request
     * @throws Exception
     */
    private void testRouting2(boolean tcp, Type request) throws Exception {
        Peer[] peers = null;
        ChannelCreator cc = null;
        try {
            // setup
            peers = createSpecialPeers(7);
            addToPeerMap(peers[0], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[1], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress());
            addToPeerMap(peers[2], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress(), peers[3].peerAddress());
            addToPeerMap(peers[3], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress(), peers[3].peerAddress(), peers[4].peerAddress());
            addToPeerMap(peers[4], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress(), peers[3].peerAddress(), peers[4].peerAddress(),
                    Utils2.createAddress("0xffffff"));
            // do testing

            if (tcp) {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(0, 2);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            } else {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(2, 0);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            }

            RoutingBuilder routingBuilder = new RoutingBuilder();
            if (tcp) {
                routingBuilder.forceTCP(true);
            }
            routingBuilder.locationKey(peers[6].peerID());
            routingBuilder.maxDirectHits(0);
            routingBuilder.setMaxNoNewInfo(0);
            routingBuilder.maxFailures(0);
            routingBuilder.maxSuccess(100);
            routingBuilder.parallel(2);

            FutureRouting fr = peers[0].distributedRouting().route(routingBuilder, request, cc);

            fr.awaitUninterruptibly();
            // do verification
            Assert.assertEquals(true, fr.isSuccess());
            SortedSet<PeerAddress> ns = fr.potentialHits();
            // node5 cannot be reached, so it should not be part of the result
            Assert.assertEquals(false, peers[5].peerAddress().equals(ns.first()));
            Assert.assertEquals(true, peers[4].peerAddress().equals(ns.first()));
            LOG.error("done!");
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            for (Peer n : peers) {
                n.shutdown().await();
            }
        }
    }

    @Test
    public void testRouting3() throws Exception {
        for (int i = 0; i < 20; i++) {
            testRouting3(true, Type.REQUEST_1);
            testRouting3(false, Type.REQUEST_1);
            testRouting3(true, Type.REQUEST_2);
            testRouting3(false, Type.REQUEST_2);
        }
    }

    private void testRouting3(boolean tcp, Type request) throws Exception {
        Peer[] peers = null;
        ChannelCreator cc = null;
        try {
            // setup
            peers = createSpecialPeers(7);
            addToPeerMap(peers[0], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[1], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress());
            addToPeerMap(peers[2], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress(), peers[3].peerAddress(), peers[4].peerAddress(),
                    peers[5].peerAddress());
            addToPeerMap(peers[3], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[4], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[5], peers[0].peerAddress(), peers[1].peerAddress());
            // do testing

            if (tcp) {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(0, 1);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            } else {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(1, 0);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            }

            RoutingBuilder routingBuilder = new RoutingBuilder();
            if (tcp) {
                routingBuilder.forceTCP(true);
            }
            routingBuilder.locationKey(peers[6].peerID());
            routingBuilder.maxDirectHits(0);
            routingBuilder.setMaxNoNewInfo(0);
            routingBuilder.maxFailures(0);
            routingBuilder.maxSuccess(100);
            routingBuilder.parallel(1);

            FutureRouting fr = peers[0].distributedRouting().route(routingBuilder, request, cc);

            fr.awaitUninterruptibly();
            // do verification
            Assert.assertEquals(true, fr.isSuccess());
            SortedSet<PeerAddress> ns = fr.potentialHits();
            Assert.assertEquals(peers[5].peerAddress(), ns.first());
            Assert.assertEquals(false, ns.contains(peers[3].peerAddress()));
            Assert.assertEquals(false, ns.contains(peers[4].peerAddress()));
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            for (Peer n : peers) {
                n.shutdown().await();
            }
        }
    }

    @Test
    public void testRouting4() throws Exception {
        for (int i = 0; i < 20; i++) {
            testRouting4(true, Type.REQUEST_1);
            testRouting4(false, Type.REQUEST_1);
            testRouting4(true, Type.REQUEST_2);
            testRouting4(false, Type.REQUEST_2);
        }
    }

    private void testRouting4(boolean tcp, Type request) throws Exception {
        Peer[] peers = null;
        ChannelCreator cc = null;
        try {
            // setup
            peers = createSpecialPeers(7);
            addToPeerMap(peers[0], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[1], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress());
            addToPeerMap(peers[2], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress(), peers[3].peerAddress(), peers[4].peerAddress(),
                    peers[5].peerAddress());
            addToPeerMap(peers[3], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[4], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[5], peers[0].peerAddress(), peers[1].peerAddress());
            // do testing

            if (tcp) {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(0, 2);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            } else {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(2, 0);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            }

            RoutingBuilder routingBuilder = new RoutingBuilder();
            if (tcp) {
                routingBuilder.forceTCP(true);
            }
            routingBuilder.locationKey(peers[6].peerID());
            routingBuilder.maxDirectHits(0);
            routingBuilder.setMaxNoNewInfo(0);
            routingBuilder.maxFailures(0);
            routingBuilder.maxSuccess(100);
            routingBuilder.parallel(2);

            FutureRouting fr = peers[0].distributedRouting().route(routingBuilder, request, cc);

            fr.awaitUninterruptibly();
            // do verification
            Assert.assertEquals(true, fr.isSuccess());
            SortedSet<PeerAddress> ns = fr.potentialHits();
            Assert.assertEquals(peers[5].peerAddress(), ns.first());
            Assert.assertEquals(true, ns.contains(peers[0].peerAddress()));
            Assert.assertEquals(true, ns.contains(peers[1].peerAddress()));
            Assert.assertEquals(true, ns.contains(peers[2].peerAddress()));
            Assert.assertEquals(false, ns.contains(peers[3].peerAddress()));
            Assert.assertEquals(true, ns.contains(peers[4].peerAddress()));
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            for (Peer n : peers) {
                n.shutdown().await();
            }
        }
    }

    @Test
    public void testRouting5() throws Exception {
        for (int i = 0; i < 20; i++) {
            testRouting5(true, Type.REQUEST_1);
            testRouting5(false, Type.REQUEST_1);
            testRouting5(true, Type.REQUEST_2);
            testRouting5(false, Type.REQUEST_2);
        }
    }

    private void testRouting5(boolean tcp, Type request) throws Exception {
        Peer[] peers = null;
        ChannelCreator cc = null;
        try {
            // setup
            peers = createSpecialPeers(7);
            addToPeerMap(peers[0], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[1], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress());
            addToPeerMap(peers[2], peers[0].peerAddress(), peers[1].peerAddress(),
                    peers[2].peerAddress(), peers[3].peerAddress(), peers[4].peerAddress(),
                    peers[5].peerAddress());
            addToPeerMap(peers[3], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[4], peers[0].peerAddress(), peers[1].peerAddress());
            addToPeerMap(peers[5], peers[0].peerAddress(), peers[1].peerAddress());
            // do testing

            if (tcp) {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(0, 3);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            } else {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(3, 0);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            }

            RoutingBuilder routingBuilder = new RoutingBuilder();
            if (tcp) {
                routingBuilder.forceTCP(true);
            }
            routingBuilder.locationKey(peers[6].peerID());
            routingBuilder.maxDirectHits(0);
            routingBuilder.setMaxNoNewInfo(0);
            routingBuilder.maxFailures(0);
            routingBuilder.maxSuccess(100);
            routingBuilder.parallel(3);

            FutureRouting fr = peers[0].distributedRouting().route(routingBuilder, request, cc);

            fr.awaitUninterruptibly();
            // do verification
            Assert.assertEquals(true, fr.isSuccess());
            SortedSet<PeerAddress> ns = fr.potentialHits();
            Assert.assertEquals(peers[5].peerAddress(), ns.first());
            Assert.assertEquals(true, ns.contains(peers[3].peerAddress()));
            Assert.assertEquals(true, ns.contains(peers[4].peerAddress()));
            Assert.assertEquals(6, ns.size());
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            for (Peer n : peers) {
                n.shutdown().await();
            }
        }
    }

    /**
     * Adds peers to a peer's map.
     * 
     * @param peer
     *            The peer to which the peers will be added
     * @param peers
     *            The peers that will be added
     */
    private void addToPeerMap(Peer peer, PeerAddress... peers) {
        for (int i = 0; i < peers.length; i++) {
            peer.peerBean().peerMap().peerFound(peers[i], null, null);
        }
    }

    private Peer[] createSpecialPeers(int nr) throws Exception {
        StringBuilder sb = new StringBuilder("0x");
        Peer[] peers = new Peer[nr];
        for (int i = 0; i < nr; i++) {
            sb.append("f");
            peers[i] = new PeerBuilder(new Number160(sb.toString())).ports(4001 + i).start();
        }
        return peers;
    }

    @Test
    public void testPerfectRouting() throws Exception {
        final Random rnd = new Random(42L);
        Peer master = null;
        try {
            // setup
            Peer[] peers = Utils2.createNodes(1000, rnd, 4001);
            System.err.println("nodes created.");
            master = peers[0];
            Utils2.perfectRouting(peers);
            // do testing
            Collection<PeerAddress> pas = peers[30].peerBean().peerMap()
                    .closePeers(peers[30].peerID(), 20);
            Iterator<PeerAddress> i = pas.iterator();
            PeerAddress p1 = i.next();
            Assert.assertEquals(peers[262].peerAddress(), p1);
        } finally {
            master.shutdown().await();
        }
    }

    @Test
    public void testRoutingBulk() throws Exception {
        testRoutingBulk(true, Type.REQUEST_1);
        testRoutingBulk(false, Type.REQUEST_1);
        testRoutingBulk(true, Type.REQUEST_1);
        testRoutingBulk(false, Type.REQUEST_2);
    }

    private void testRoutingBulk(boolean tcp, Type request) throws Exception {
        Peer master = null;
        ChannelCreator cc = null;
        try {
            // setup
            Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
            master = peers[0];
            Utils2.perfectRouting(peers);
            // do testing

            if (tcp) {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(0, 1);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            } else {
                FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(1, 0);
                fcc.awaitUninterruptibly();
                cc = fcc.channelCreator();
            }

            RoutingBuilder routingBuilder = new RoutingBuilder();
            if (tcp) {
                routingBuilder.forceTCP(true);
            }
            routingBuilder.locationKey(peers[20].peerID());
            routingBuilder.maxDirectHits(0);
            routingBuilder.setMaxNoNewInfo(0);
            routingBuilder.maxFailures(0);
            routingBuilder.maxSuccess(100);
            routingBuilder.parallel(1);

            FutureRouting fr = peers[500].distributedRouting().route(routingBuilder, request, cc);

            fr.awaitUninterruptibly();
            // do verification
            Assert.assertEquals(true, fr.isSuccess());
            SortedSet<PeerAddress> ns = fr.potentialHits();
            Assert.assertEquals(peers[20].peerAddress(), ns.first());
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (master != null) {
                master.shutdown().await();
            }
        }
    }

    @Test
    public void testRoutingConcurrently() throws Exception {
        for (int i = 0; i < 3; i++) {
            testRoutingConcurrently(true, Type.REQUEST_1, 1);
            testRoutingConcurrently(false, Type.REQUEST_1, 1);
            testRoutingConcurrently(true, Type.REQUEST_1, 1);
            testRoutingConcurrently(false, Type.REQUEST_2, 1);
            testRoutingConcurrently(true, Type.REQUEST_1, 2);
            testRoutingConcurrently(false, Type.REQUEST_1, 2);
            testRoutingConcurrently(true, Type.REQUEST_1, 2);
            testRoutingConcurrently(false, Type.REQUEST_2, 2);
        }
    }

    private void testRoutingConcurrently(boolean tcp, Type request, int res) throws Exception {
        final Random rnd1 = new Random(42L);
        final Random rnd2 = new Random(43L);
        final Random rnd3 = new Random(42L);
        Peer master = null;
        ChannelCreator cc = null;
        try {
            // setup
            Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
            master = peers[0];
            Utils2.perfectRouting(peers);
            // do testing
            System.err.println("do routing.");
            List<FutureRouting> frs = new ArrayList<FutureRouting>();
            for (int i = 0; i < peers.length; i++) {

                if (tcp) {
                    FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(0, res);
                    fcc.awaitUninterruptibly();
                    cc = fcc.channelCreator();
                } else {
                    FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(res, 0);
                    fcc.awaitUninterruptibly();
                    cc = fcc.channelCreator();
                }

                RoutingBuilder routingBuilder = new RoutingBuilder();
                if (tcp) {
                    routingBuilder.forceTCP(true);
                }
                routingBuilder.locationKey(peers[rnd1.nextInt(peers.length)].peerID());
                routingBuilder.maxDirectHits(0);
                routingBuilder.setMaxNoNewInfo(0);
                routingBuilder.maxFailures(0);
                routingBuilder.maxSuccess(100);
                routingBuilder.parallel(res);

                FutureRouting frr = peers[rnd2.nextInt(peers.length)].distributedRouting().route(
                        routingBuilder, request, cc);

                frs.add(frr);
                Utils.addReleaseListener(cc, frr);
            }
            System.err.println("now checking if the tests were successful.");
            for (int i = 0; i < peers.length; i++) {

                frs.get(i).awaitListenersUninterruptibly();
                Assert.assertEquals(true, frs.get(i).isSuccess());
                SortedSet<PeerAddress> ns = frs.get(i).potentialHits();
                Assert.assertEquals(peers[rnd3.nextInt(peers.length)].peerAddress(), ns.first());
            }
            System.err.println("done!");
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (master != null) {
                master.shutdown().await();
            }
        }
    }

    @Test
    public void testRoutingBootstrap1() throws Exception {
        testRoutingBootstrap1(true);
        testRoutingBootstrap1(false);
    }

    private void testRoutingBootstrap1(boolean tcp) throws Exception {
        Peer master = null;
        ChannelCreator cc = null;
        try {
            // setup
            Peer[] peers = Utils2.createNodes(200, rnd, 4001);
            master = peers[0];
            Utils2.perfectRouting(peers);
            // do testing
            Collection<PeerAddress> peerAddresses = new ArrayList<PeerAddress>(1);
            peerAddresses.add(master.peerAddress());
            for (int i = 1; i < peers.length; i++) {

                if (tcp) {
                    FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(0, 1);
                    fcc.awaitUninterruptibly();
                    cc = fcc.channelCreator();
                } else {
                    FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(1, 0);
                    fcc.awaitUninterruptibly();
                    cc = fcc.channelCreator();
                }

                RoutingBuilder routingBuilder = new RoutingBuilder();
                if (tcp) {
                    routingBuilder.forceTCP(true);
                }
                routingBuilder.maxDirectHits(0);
                routingBuilder.setMaxNoNewInfo(5);
                routingBuilder.maxFailures(100);
                routingBuilder.maxSuccess(100);
                routingBuilder.parallel(1);

                FutureDone<Pair<FutureRouting,FutureRouting>> fm = peers[i].distributedRouting().bootstrap(
                        peerAddresses, routingBuilder, cc);
                fm.awaitUninterruptibly();
                // do verification
                Assert.assertEquals(true, fm.isSuccess());
                Assert.assertEquals(true, fm.object().element0().isSuccess());
                Assert.assertEquals(true, fm.object().element1().isSuccess());
            }
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (master != null) {
                master.shutdown().await();
            }
        }
    }

    @Test
    public void testBootstrap() throws Exception {
        Peer master = null;
        Peer client = null;
        try {
            // Bindings b = new Bindings("wlan0");
            master = new PeerBuilder(new Number160(rnd)).ports(4000).start();
            client = new PeerBuilder(new Number160(rnd)).ports(4001).start();

            BaseFuture tmp = client.ping().broadcast().port(4000).start();
            tmp.awaitUninterruptibly();
            System.err.println(tmp.failedReason());
            Assert.assertEquals(true, tmp.isSuccess());

            Assert.assertEquals(1, client.peerBean().peerMap().size());
        } finally {
            if (client != null) {
                client.shutdown().await();
            }
            if (master != null) {
                master.shutdown().await();
            }
        }
    }

    @Test
    public void testBootstrap2() throws Exception {
        Peer master = null;
        Peer client = null;
        try {
        	Bindings b = new Bindings().addInterface("lo");
        	master = new PeerBuilder(new Number160(rnd)).externalBindings(b).ports(4002).start();
            client = new PeerBuilder(new Number160(rnd)).externalBindings(b).ports(4001).start();
            BaseFuture tmp = client.ping().broadcast().port(4001).start();
            tmp.awaitUninterruptibly();
            Assert.assertEquals(false, tmp.isSuccess());
            Assert.assertEquals(0, client.peerBean().peerMap().size());
        } finally {
            if (client != null) {
                client.shutdown().await();
            }
            if (master != null) {
                master.shutdown().await();
            }
        }
    }

    @Test
    public void testRoutingLoop() throws Exception {
        testRoutingLoop(true);
        testRoutingLoop(false);
    }

    private void testRoutingLoop(boolean tcp) throws Exception {
        final Random rnd = new Random(43L);
        for (int k = 0; k < 100; k++) {
            Number160 find = Number160.createHash("findme");
            Peer master = null;
            ChannelCreator cc = null;
            try {
                System.err.println("round " + k);
                // setup
                Peer[] peers = Utils2.createNodes(200, rnd, 4001);
                master = peers[0];
                Utils2.perfectRouting(peers);
                Comparator<PeerAddress> cmp = PeerMap.createComparator(find);
                SortedSet<PeerAddress> ss = new TreeSet<PeerAddress>(cmp);
                for (int i = 0; i < peers.length; i++) {
                    ss.add(peers[i].peerAddress());
                }
                // do testing
                if (tcp) {
                    FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(0, 2);
                    fcc.awaitUninterruptibly();
                    cc = fcc.channelCreator();
                } else {
                    FutureChannelCreator fcc = peers[0].connectionBean().reservation().create(2, 0);
                    fcc.awaitUninterruptibly();
                    cc = fcc.channelCreator();
                }

                RoutingBuilder routingBuilder = new RoutingBuilder();
                if (tcp) {
                    routingBuilder.forceTCP(true);
                }
                routingBuilder.locationKey(find);
                routingBuilder.maxDirectHits(Integer.MAX_VALUE);
                routingBuilder.setMaxNoNewInfo(5);
                routingBuilder.maxFailures(10);
                routingBuilder.maxSuccess(20);
                routingBuilder.parallel(2);

                FutureRouting frr = peers[50].distributedRouting().route(routingBuilder, Type.REQUEST_1,
                        cc);

                frr.awaitUninterruptibly();
                SortedSet<PeerAddress> ss2 = frr.potentialHits();
                // test the first 5 peers, because we set noNewInformation to 5,
                // which means we find at least 5 entries.
                for (int i = 0; i < 5; i++) {
                    PeerAddress pa = ss.first();
                    PeerAddress pa2 = ss2.first();
                    System.err.println("test " + pa + " - " + pa2);
                    Assert.assertEquals(pa.peerId(), pa2.peerId());
                    ss.remove(pa);
                    ss2.remove(pa2);
                }
            } finally {
                if (cc != null) {
                    cc.shutdown().awaitListenersUninterruptibly();
                }
                if (master != null) {
                    master.shutdown().awaitListenersUninterruptibly();
                }
            }
        }
    }
}
