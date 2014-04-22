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
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;
import net.tomp2p.synchronization.SyncStat;

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
    public void testGetBestSmoothingFactor() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port7).makeAndListen();
        AutoReplication automaticReplication = new AutoReplication();
        automaticReplication.reliability(reliability).init(peer);

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

        Assert.assertEquals(0.5, AutoReplication.bestSmoothingFactor(x, y), 0.0);
    }

    @Test
    public void testGetAverage() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port8).makeAndListen();
        AutoReplication automaticReplication = new AutoReplication();
        automaticReplication.reliability(reliability).init(peer);

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

        Assert.assertEquals(9.4, AutoReplication.ema(observations, averages), 0.0);
    }

    @Test
    public void testGetStandardDeviation() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port9).makeAndListen();
        AutoReplication automaticReplication = new AutoReplication();
        automaticReplication.reliability(reliability).init(peer);

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

        double average = AutoReplication.ema(observations, averages);
        Assert.assertEquals(3.111269837220809, AutoReplication.standardDeviation(observations, average), 0.0);
    }

    @Test
    public void testGetPredictedValue() throws IOException {
        Peer peer = new PeerMaker(new Number160(peerId)).ports(port10).makeAndListen();
        AutoReplication automaticReplication = new AutoReplication();
        automaticReplication.reliability(reliability).init(peer);

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

        double average = AutoReplication.ema(observations, averages);

        assertEquals(12.0, AutoReplication.predictedValue(observations, average), 0.0);
    }
    
    @Test
	public void testChoose() {
		double d = AutoReplication.choose(11, 5);
		Assert.assertEquals(462, d, 0.1);
		d = AutoReplication.choose(5, 11);
		Assert.assertEquals(0, d, 0.1);
	}

	@Test
	public void replicationFactor1() {
		int f = AutoReplication.replicationFactor(50, 100, 0.80, 2, 100);
		Assert.assertEquals(3, f);
		f = AutoReplication.replicationFactor(50, 100, 0.99, 2, 100);
		Assert.assertEquals(7, f);
		f = AutoReplication.replicationFactor(10, 100, 0.99, 2, 100);
		Assert.assertEquals(2, f);
		f = AutoReplication.replicationFactor(100, 100, 0.1, 2, 100);
		Assert.assertEquals(100, f);
		f = AutoReplication.replicationFactor(4, 5, 0.4, 2, 100);
		Assert.assertEquals(2, f);
	}
	
	@Test
	public void replicationFactor2() {
		int f = AutoReplication.replicationFactor2(50, 100, 0.80, 2, 100);
		Assert.assertEquals(3, f);
		f = AutoReplication.replicationFactor2(50, 100, 0.99, 2, 100);
		Assert.assertEquals(7, f);
		f = AutoReplication.replicationFactor2(10, 100, 0.99, 2, 100);
		Assert.assertEquals(2, f);
		f = AutoReplication.replicationFactor2(100, 100, 0.1, 2, 100);
		Assert.assertEquals(100, f);
		f = AutoReplication.replicationFactor2(4, 5, 0.4, 2, 100);
		//rounding issue
		Assert.assertEquals(3, f);
	}

   

    @Test
    public void testGetNeighbourPeersSize() throws Exception {
        Random RND = new Random();
        Peer[] peers = Utils2.createNodes(10, RND, port12, null, true);
        Utils2.perfectRouting(peers);
        
        AutoReplication automaticReplication = new AutoReplication();
        automaticReplication.reliability(reliability).init(peers[2]);

        int expectedValue = 9;

        assertEquals(expectedValue, automaticReplication.peerMapSize());
    }

    @Test
    public void testGetRemovedPeersSize() throws Exception {
        Random RND = new Random();
        Peer[] peers = Utils2.createNodes(10, RND, port13, null, true);
        Utils2.perfectRouting(peers);
        
        AutoReplication automaticReplication = new AutoReplication();
        automaticReplication.reliability(reliability).init(peers[2]);

        peers[3].announceShutdown().start().awaitUninterruptibly();
        peers[3].shutdown().awaitUninterruptibly();
        peers[6].announceShutdown().start().awaitUninterruptibly();
        peers[6].shutdown().awaitUninterruptibly();
        peers[8].announceShutdown().start().awaitUninterruptibly();
        peers[8].shutdown().awaitUninterruptibly();
        Thread.sleep(1000);

        int expectedValue = 9 - 3;

        assertEquals(expectedValue, automaticReplication.peerMapSize());
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
                        final FutureDone<SyncStat> f = (FutureDone<SyncStat>) future;
                        f.addListener(new BaseFutureAdapter<BaseFuture>() {
                            @Override
                            public void operationComplete(BaseFuture future)
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
            latch.await();
            Assert.assertEquals(true, testCopied.get());
            
            
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
