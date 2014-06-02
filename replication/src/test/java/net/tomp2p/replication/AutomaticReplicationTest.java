package net.tomp2p.replication;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.Utils2;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.AutomaticFuture;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.storage.Data;
import net.tomp2p.synchronization.SyncStat;

import org.junit.Assert;
import org.junit.Test;

public class AutomaticReplicationTest {
    private double reliability = 0.90;
    private int peerId = 1111;
    private int port7 = 4007;
    private int port8 = 4008;
    private int port9 = 4009;
    private int port10 = 4010;
    private int port12 = 4012;
    private int port13 = 4013;

    @Test
    public void testGetBestSmoothingFactor() throws IOException {
        Peer peer = new PeerBuilder(new Number160(peerId)).ports(port7).start();
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
        Peer peer = new PeerBuilder(new Number160(peerId)).ports(port8).start();
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
        Peer peer = new PeerBuilder(new Number160(peerId)).ports(port9).start();
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
        Peer peer = new PeerBuilder(new Number160(peerId)).ports(port10).start();
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

    private NavigableSet<PeerAddress> findTheClosestPeer(Peer[] peers, Number160 locationKey) {
    	
    	Comparator<PeerAddress> c = PeerMap.createComparator(locationKey);
    	TreeSet<PeerAddress> ts = new TreeSet<>(c);
    	for(Peer peer:peers) {
    		ts.add(peer.peerAddress());
    	}
        return ts;
    }
    
    private Peer find(Peer[] peers, PeerAddress peerAddress) {
    	for(Peer peer:peers) {
    		if(peer.peerAddress().equals(peerAddress)) {
    			return peer;
    		}
    	}
    	return null;
    }
    
    

    @Test
    public void testIndirectReplication1() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean testCopied = new AtomicBoolean();
        Peer master = null;
        try {
            AutomaticFuture af = new AutomaticFuture() {
                
                @SuppressWarnings("unchecked")
                @Override
                public void futureCreated(BaseFuture future) {
                    if(future instanceof FutureDone) {
                        final FutureDone<SyncStat> f = (FutureDone<SyncStat>) future;
                        f.addListener(new BaseFutureAdapter<BaseFuture>() {
                            @Override
                            public void operationComplete(BaseFuture future)
                                    throws Exception {
                            	if(future.isFailed()) {
                            		System.err.println(future.failedReason());
                            	}
                            	//System.err.println(future.isSuccess() +"/"+ f.getObject());
                            	if(!testCopied.get()) {
                            		testCopied.set(f.object().dataCopy() == 56);
                            	}
                                latch.countDown();
                            }
                        });
                        
                    }
                    
                }
            };
            Peer[] peers = Utils2.createNodes(N, rnd, port, af, true);

            master = peers[0];
            final Number160 locationKey = new Number160(12345);

            NavigableSet<PeerAddress> closestPeersIndexes = findTheClosestPeer(peers, locationKey);
            Iterator<PeerAddress> iterator = closestPeersIndexes.iterator();
            
            PeerAddress Pa = iterator.next();
            PeerAddress Pb = iterator.next();
            PeerAddress Pc = iterator.next();

            Peer A = find(peers, Pa);
            Peer B = find(peers, Pb);
            Peer C = find(peers, Pc);
            
            System.err.println("peer A "+A.peerAddress());
            System.err.println("peer B "+B.peerAddress());
            System.err.println("peer C "+C.peerAddress());

            Data data1 = new Data("CommunicationSystemsDatabaseSoftwareEngineeringRequirementsAnalysis..");
            Data data2 = new Data("CommunicationSystemsDatabaseSoftwarasdfkjasdklfjasdfklajsdfaslkjfsdfd");
            Data data3 = new Data(
                    "oesauhtnoaetnsuuhooaeuoaeuoauoaeuoaeuoaeuaetnsauosanuhatns");
                    //Data data3 = new Data("CommunicationSystemsDatabaseSoftwarasdfkjasdklfjasdfklajsdfaslkjfsdfa");
            
            
            //A.getPeerBean().storage().put(new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO), data1, null, false, false);
            //B.getPeerBean().storage().put(new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO), data2, null, false, false);
            //C.getPeerBean().storage().put(new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO), data3, null, false, false);
            A.put(locationKey).setData(data1).start().awaitUninterruptibly();
            B.put(locationKey).setData(data2).start().awaitUninterruptibly();
            C.put(locationKey).setData(data3).start().awaitUninterruptibly();

            Utils2.perfectRouting(peers);

            Data data = A.peerBean().storageLayer().get(new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO));
            byte[] valueOfA = data.toBytes();
            data = B.peerBean().storageLayer().get(new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO));
            byte[] valueOfB = data.toBytes();
            data = C.peerBean().storageLayer().get(new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO));
            byte[] valueOfC = data.toBytes();

            Assert.assertArrayEquals(valueOfA, valueOfB);
            Assert.assertArrayEquals(valueOfA, valueOfC);
            latch.await();
            Assert.assertEquals(true, testCopied.get());
            System.err.println("DONE!");
            
        } finally {
            if (master != null) {
                master.shutdown();
            }
        }
    }
}
