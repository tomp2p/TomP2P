package net.tomp2p.replication;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.Utils2;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.AutomaticFuture;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.p2p.builder.SynchronizationStatistics;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class AutomaticReplicationTest {
    private double reliability = 0.90;
    private int peerId = 1111;
    private int port1 = 4001;
    private int port2 = 4002;
    private int port3 = 4003;
    private int port4 = 4004;
    private int port5 = 4005;
    private int port6 = 4006;
    private int port7 = 4007;
    private int port8 = 4008;
    private int port9 = 4009;
    private int port10 = 4010;
    private int port11 = 4011;
    private int port12 = 4012;
    private int port13 = 4013;
    private int port14 = 4014;

    @Test
    public void testGetXMean() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port1).makeAndListen();
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peer.getPeerBean()
                .peerMap());

        ArrayList<Integer> x = new ArrayList<Integer>();
        x.add(5);
        x.add(10);
        x.add(15);

        assertEquals(10.0, automaticReplication.getXMean(x, 0), 0.0);
    }

    @Test
    public void testGetYMean() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port2).makeAndListen();
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peer.getPeerBean()
                .peerMap());

        ArrayList<Double> y = new ArrayList<Double>();
        y.add(1.0);
        y.add(2.0);
        y.add(3.0);

        assertEquals(2.0, automaticReplication.getYMean(y, 0), 0.0);
    }

    @Test
    public void testGetSumOfXVariationMultipliedYVariation() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port3).makeAndListen();
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peer.getPeerBean()
                .peerMap());

        ArrayList<Integer> x = new ArrayList<Integer>();
        ArrayList<Double> y = new ArrayList<Double>();

        x.add(1);
        x.add(2);
        x.add(3);
        y.add(1.0);
        y.add(2.0);
        y.add(3.0);

        double xMean = automaticReplication.getXMean(x, 0);
        double yMean = automaticReplication.getYMean(y, 0);

        double expectedValue = 0;
        for (int i = 0; i < x.size(); i++)
            expectedValue += (x.get(i) - xMean) * (y.get(i) - yMean);

        assertEquals(expectedValue,
                automaticReplication.getSumOfXVariationMultipliedYVariation(x, y, 0, xMean, yMean), 0.0);
    }

    @Test
    public void testGetSumOfXVariationSquared() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port4).makeAndListen();
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peer.getPeerBean()
                .peerMap());

        ArrayList<Integer> x = new ArrayList<Integer>();

        x.add(1);
        x.add(2);
        x.add(3);

        double xMean = automaticReplication.getXMean(x, 0);

        double expectedValue = 0;
        for (int i = 0; i < x.size(); i++)
            expectedValue += (x.get(i) - xMean) * (x.get(i) - xMean);

        assertEquals(expectedValue, automaticReplication.getSumOfXVariationSquared(x, 0, xMean), 0.0);
    }

    @Test
    public void testGetSumOfRegressionVariationSquared() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port5).makeAndListen();
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peer.getPeerBean()
                .peerMap());

        ArrayList<Integer> x = new ArrayList<Integer>();
        ArrayList<Double> y = new ArrayList<Double>();

        x.add(1);
        x.add(2);
        x.add(3);
        y.add(1.0);
        y.add(2.0);
        y.add(3.0);

        double xMean = automaticReplication.getXMean(x, 0);
        double yMean = automaticReplication.getYMean(y, 0);
        double b1 = automaticReplication.getSumOfXVariationMultipliedYVariation(x, y, 0, xMean, yMean)
                / automaticReplication.getSumOfXVariationSquared(x, 0, xMean);
        double b0 = yMean - b1 * xMean;

        double expectedValue = 0;
        for (int i = 0; i < x.size(); i++)
            expectedValue += (b0 + b1 * x.get(i) - yMean) * (b0 + b1 * x.get(i) - yMean);

        assertEquals(expectedValue,
                automaticReplication.getSumOfRegressionVariationSquared(x, 0, b0, b1, yMean), 0.0);
    }

    @Test
    public void testGetSumOfYVariationSquared() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port6).makeAndListen();
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peer.getPeerBean()
                .peerMap());

        ArrayList<Double> y = new ArrayList<Double>();

        y.add(1.0);
        y.add(2.0);
        y.add(3.0);

        double yMean = automaticReplication.getYMean(y, 0);

        double expectedValue = 0;
        for (int i = 0; i < y.size(); i++)
            expectedValue += (y.get(i) - yMean) * (y.get(i) - yMean);

        assertEquals(expectedValue, automaticReplication.getSumOfYVariationSquared(y, 0, yMean), 0.0);
    }

    @Test
    public void testGetBestSmoothingFactor() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port7).makeAndListen();
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peer.getPeerBean()
                .peerMap());

        ArrayList<Integer> x = new ArrayList<Integer>();
        ArrayList<Double> y = new ArrayList<Double>();

        x.add(8);
        x.add(3);
        x.add(11);
        x.add(14);
        x.add(16);
        y.add(9.0);
        y.add(4.0);
        y.add(10.0);
        y.add(17.0);
        y.add(19.0);

        double rSquared = 0;
        double max = 0;
        int interval = x.size();
        for (int i = 0; i < x.size() - 2; i++) {
            double xMean = automaticReplication.getXMean(x, i);
            double yMean = automaticReplication.getYMean(y, i);
            double b1 = automaticReplication.getSumOfXVariationMultipliedYVariation(x, y, i, xMean, yMean)
                    / automaticReplication.getSumOfXVariationSquared(x, i, xMean);
            double b0 = yMean - b1 * xMean;
            rSquared = automaticReplication.getSumOfRegressionVariationSquared(x, i, b0, b1, yMean)
                    / automaticReplication.getSumOfYVariationSquared(y, i, yMean);
            if (max <= rSquared) {
                max = rSquared;
                interval = x.size() - i;
            }
        }

        double expectedValue = 2.0 / (interval + 1);

        assertEquals(expectedValue, automaticReplication.getBestSmoothingFactor(x, y, x.size()), 0.0);
    }

    @Test
    public void testGetAverage() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port8).makeAndListen();
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peer.getPeerBean()
                .peerMap());

        ArrayList<Integer> observations = new ArrayList<Integer>();
        ArrayList<Double> averages = new ArrayList<Double>();

        observations.add(8);
        observations.add(6);
        observations.add(11);
        observations.add(13);

        averages.add(0.0);
        averages.add(4.0);
        averages.add(5.0);
        averages.add(7.0);

        double smoothingFactor = automaticReplication.getBestSmoothingFactor(observations, averages,
                observations.size());
        double lastObservation = observations.get(observations.size() - 1);
        double lastAverage = averages.get(averages.size() - 1);
        double expectedValue = (lastObservation - lastAverage) * smoothingFactor + lastAverage;

        assertEquals(expectedValue, automaticReplication.getAverage(observations, averages), 0.0);
    }

    @Test
    public void testGetStandardDeviation() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port9).makeAndListen();
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peer.getPeerBean()
                .peerMap());

        ArrayList<Integer> observations = new ArrayList<Integer>();
        ArrayList<Double> averages = new ArrayList<Double>();

        observations.add(8);
        observations.add(6);
        observations.add(11);
        observations.add(13);

        averages.add(0.0);
        averages.add(4.0);
        averages.add(5.0);
        averages.add(7.0);

        double average = automaticReplication.getAverage(observations, averages);

        double expectedValue = 0;
        for (int i = 0; i < observations.size(); i++)
            expectedValue += (observations.get(i) - average) * (observations.get(i) - average);
        expectedValue = Math.sqrt(expectedValue / (observations.size() - 1));

        assertEquals(expectedValue, automaticReplication.getStandardDeviation(observations, average), 0.0);
    }

    @Test
    public void testGetPredictedValue() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port10).makeAndListen();
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peer.getPeerBean()
                .peerMap());

        ArrayList<Integer> observations = new ArrayList<Integer>();
        ArrayList<Double> averages = new ArrayList<Double>();

        observations.add(8);
        observations.add(6);
        observations.add(11);
        observations.add(13);

        averages.add(0.0);
        averages.add(4.0);
        averages.add(5.0);
        averages.add(7.0);

        double average = automaticReplication.getAverage(observations, averages);
        double deviation = automaticReplication.getStandardDeviation(observations, average);

        double expectedValue = Math.ceil(average + deviation);

        assertEquals(expectedValue, automaticReplication.getPredictedValue(observations, average), 0.0);
    }

    @Test
    public void testCalculateReplicationFactor() throws Exception {
        Random RND = new Random();
        Peer[] peers = Utils2.createNodes(10, RND, port11, null, true);
        Utils2.perfectRouting(peers);
        Thread.sleep(1000);
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peers[2]
                .getPeerBean().peerMap());

        ArrayList<Integer> observations = new ArrayList<Integer>();
        ArrayList<Double> averages = new ArrayList<Double>();

        observations.add(8);
        observations.add(6);
        observations.add(7);
        observations.add(8);

        averages.add(0.0);
        averages.add(4.0);
        averages.add(3.0);
        averages.add(4.0);

        double average = automaticReplication.getAverage(observations, averages);
        double predictedValue = automaticReplication.getPredictedValue(observations, average);

        int replicationFactor = 1;
        int neighbourSize = 9;
        while (true) {
            replicationFactor++;
            double probability = 1;
            for (int i = 0; i < replicationFactor; i++)
                probability *= (double) (predictedValue - i) / (double) (neighbourSize - i);

            if ((1 - probability) >= reliability)
                break;
        }

        int expectedValue = replicationFactor;

        assertEquals(expectedValue,
                automaticReplication.calculateReplicationFactor((int) Math.round(predictedValue)));
    }

    @Test
    public void testGetNeighbourPeersSize() throws Exception {
        Random RND = new Random();
        Peer[] peers = Utils2.createNodes(10, RND, port12, null, true);
        Utils2.perfectRouting(peers);
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peers[2]
                .getPeerBean().peerMap());

        int expectedValue = 9;

        assertEquals(expectedValue, automaticReplication.getNeighbourPeersSize());
    }

    @Test
    public void testGetRemovedPeersSize() throws Exception {
        Random RND = new Random();
        Peer[] peers = Utils2.createNodes(10, RND, port13, null, true);
        Utils2.perfectRouting(peers);
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peers[2]
                .getPeerBean().peerMap());

        peers[3].announceShutdown().start().awaitUninterruptibly();
        peers[3].shutdown().awaitUninterruptibly();
        peers[6].announceShutdown().start().awaitUninterruptibly();
        peers[6].shutdown().awaitUninterruptibly();
        peers[8].announceShutdown().start().awaitUninterruptibly();
        peers[8].shutdown().awaitUninterruptibly();
        Thread.sleep(1000);

        int expectedValue = 3;

        assertEquals(expectedValue, automaticReplication.getRemovedPeersSize());
    }

    @Test
    public void testClearRemovedPeers() throws Exception {
        Random RND = new Random();
        Peer[] peers = Utils2.createNodes(10, RND, port14, null, true);
        Utils2.perfectRouting(peers);
        AutomaticReplication automaticReplication = new AutomaticReplication(reliability, peers[2]
                .getPeerBean().peerMap());

        peers[3].announceShutdown().start().awaitUninterruptibly();
        peers[3].shutdown().awaitUninterruptibly();
        peers[6].announceShutdown().start().awaitUninterruptibly();
        peers[6].shutdown().awaitUninterruptibly();
        peers[8].announceShutdown().start().awaitUninterruptibly();
        peers[8].shutdown().awaitUninterruptibly();
        Thread.sleep(1000);

        automaticReplication.clearRemovedPeers();

        int expectedValue = 0;

        assertEquals(expectedValue, automaticReplication.getRemovedPeersSize());
    }

    private static final int N = 100;
    private static final Random rnd = new Random(74);
    private static final int port = 4020;

    private ArrayList<Integer> findTheClosestPeer(Peer[] peers, Number160 locationKey) {
        ArrayList<Integer> closestPeersIndexes = new ArrayList<Integer>();
        int index = -1;
        for (int i = 0; i < 10; i++) {
            Number160 min = null;
            for (int j = 0; j < peers.length; j++) {
                if (min == null && !closestPeersIndexes.contains(j)) {
                    min = peers[j].getPeerID().xor(locationKey);
                    index = j;
                } else if (min.compareTo(peers[j].getPeerID().xor(locationKey)) > 0
                        && !closestPeersIndexes.contains(j)) {
                    min = peers[j].getPeerID().xor(locationKey);
                    index = j;
                }
            }
            closestPeersIndexes.add(index);
        }

        return closestPeersIndexes;
    }

    @Test
    public void testIndirectReplication1() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean testCopied = new AtomicBoolean();
        Peer master = null;
        try {
            AutomaticFuture af = new AutomaticFuture() {
                
                @Override
                public void futureCreated(BaseFuture future) {
                    if(future instanceof FutureDone) {
                        final FutureDone<SynchronizationStatistics> f = (FutureDone<SynchronizationStatistics>) future;
                        f.addListener(new BaseFutureAdapter<FutureDone<SynchronizationStatistics>>() {

                            @Override
                            public void operationComplete(FutureDone<SynchronizationStatistics> future)
                                    throws Exception {
                                testCopied.set(f.getObject().dataCopy() == 71 || f.getObject().dataCopy() == 36 || f.getObject().dataCopy() == 76);
                                latch.countDown();
                            }
                        });
                        
                    }
                    
                }
            };
            Peer[] peers = Utils2.createNodes(N, rnd, port, af, true);

            master = peers[0];
            final Number160 locationKey = new Number160(12345);
            final Number160 domainKey = Number160.ZERO;
            final Number160 contentKey = Number160.ZERO;

            ArrayList<Integer> closestPeersIndexes = findTheClosestPeer(peers, locationKey);

            Peer A = peers[closestPeersIndexes.get(0)];
            Peer B = peers[closestPeersIndexes.get(1)];
            Peer C = peers[closestPeersIndexes.get(2)];

            Data data1 = new Data("CommunicationSystemsDatabaseSoftwareEngineeringRequirementsAnalysis..");
            Data data2 = new Data("CommunicationSystemsDatabaseSoftwarasdfkjasdklfjasdfklajsdfaslkjfsdfd");
            Data data3 = new Data(
                    "oesauhtnoaetnsuuhooaeuoaeuoauoaeuoaeuoaeuaetnsauosanuhatns");

            A.put(locationKey).setData(data1).start().awaitUninterruptibly();
            B.put(locationKey).setData(data2).start().awaitUninterruptibly();
            C.put(locationKey).setData(data3).start().awaitUninterruptibly();

            Utils2.perfectRouting(peers);

            Data data = A.getPeerBean().storage().get(new Number640(locationKey, domainKey, contentKey, Number160.ZERO));
            byte[] valueOfA = data.toBytes();
            data = B.getPeerBean().storage().get(new Number640(locationKey, domainKey, contentKey, Number160.ZERO));
            byte[] valueOfB = data.toBytes();
            data = C.getPeerBean().storage().get(new Number640(locationKey, domainKey, contentKey, Number160.ZERO));
            byte[] valueOfC = data.toBytes();

            Assert.assertArrayEquals(valueOfA, valueOfB);
            Assert.assertArrayEquals(valueOfA, valueOfC);
            
            Assert.assertEquals(true, testCopied.get());
            latch.await();
            
        } finally {
            if (master != null) {
                master.shutdown();
            }
        }
    }

    @Test
    public void testIndirectReplication2() throws Exception {
        Peer master = null;
        try {
            // test for replication, shutdown peers
            Peer[] peers = Utils2.createNodes(15, rnd, port + 1, null, true);
            master = peers[0];
            Utils2.perfectRouting(peers);

            peers[5].announceShutdown().start().awaitUninterruptibly();
            peers[5].shutdown().awaitUninterruptibly();
            peers[7].announceShutdown().start().awaitUninterruptibly();
            peers[7].shutdown().awaitUninterruptibly();
            peers[8].announceShutdown().start().awaitUninterruptibly();
            peers[8].shutdown().awaitUninterruptibly();

            Thread.sleep(60 * 1000);
            assertEquals(4, peers[0].getPeerBean().replicationStorage().getReplicationFactor());
        } finally {
            if (master != null) {
                master.shutdown();
            }
        }
    }
}
