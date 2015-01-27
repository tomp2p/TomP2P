package net.tomp2p.replication;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.Utils2;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerException;
import net.tomp2p.connection.PeerException.AbortCause;
import net.tomp2p.connection.Ports;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.PutBuilder;
import net.tomp2p.dht.StorageMemory;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.AutomaticFuture;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.ResponsibilityListener;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestStoreReplication {
	
	final private static Number160 domainKey = new Number160(20);
	/**
     * Test the responsibility and the notifications.
     * 
     * @throws Exception .
     */
    @Test
    public void testResponsibility0Root1() throws Exception {
        // Random rnd=new Random(42L);
    	PeerDHT master = null;
    	PeerDHT slave = null;
        ChannelCreator cc = null;
        try {
        	StorageMemory s1 = new StorageMemory();
        	master = new PeerBuilderDHT(new PeerBuilder(new Number160("0xee")).start()).storage(s1).start();
        	IndirectReplication im = new IndirectReplication(master);
        	
            
            final AtomicInteger test1 = new AtomicInteger(0);
            final AtomicInteger test2 = new AtomicInteger(0);

            ResponsibilityListener responsibilityListener = new ResponsibilityListener() {
                @Override
                public FutureDone<?> meResponsible(final Number160 locationKey) {
                    System.err.println("I'm responsible for " + locationKey + " / ");
                    test2.incrementAndGet();
                    return new FutureDone<Void>().done();
                }

				@Override
                public FutureDone<?> meResponsible(Number160 locationKey, PeerAddress newPeer) {
	                System.err.println("I sync for " + locationKey + " / ");
	                return new FutureDone<Void>().done();
                }

				@Override
                public FutureDone<?> otherResponsible(Number160 locationKey, PeerAddress other) {
					System.err.println("Other peer (" + other + ")is responsible for " + locationKey);
                    test1.incrementAndGet();
                    return new FutureDone<Void>().done();
                }
            };
            
            im.addResponsibilityListener(responsibilityListener);
            im.start();

            Number160 location = new Number160("0xff");
            Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
            dataMap.put(Number160.ZERO, new Data("string"));

            FutureChannelCreator fcc = master.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(master, location);
            putBuilder.domainKey(location);
            putBuilder.dataMapContent(dataMap);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = master.storeRPC().put(master.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            // s1.put(location, Number160.ZERO, null, dataMap, false, false);
            final int slavePort = 7701;
            slave = new PeerBuilderDHT(new PeerBuilder(new Number160("0xfe")).ports(slavePort).start()).start();
            master.peerBean().peerMap().peerFound(slave.peerAddress(), null, null, null);
            master.peerBean().peerMap().peerFailed(slave.peerAddress(), new PeerException(AbortCause.SHUTDOWN, "shutdown"));
            Assert.assertEquals(1, test1.get());
            Assert.assertEquals(2, test2.get());
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (master != null) {
                master.shutdown().await();
            }
            if (slave != null) {
                slave.shutdown().await();
            }
        }
    }

    /**
     * Test the responsibility and the notifications.
     * 
     * @throws Exception .
     */
    @Test
    public void testResponsibility0Root2() throws Exception {
        final Random rnd = new Random(42L);
        final int port = 8000;
        PeerDHT master = null;
        PeerDHT slave1 = null;
        PeerDHT slave2 = null;
        ChannelCreator cc = null;
        try {
            Number160 loc = new Number160(rnd);
            Map<Number160, Data> contentMap = new HashMap<Number160, Data>();
            contentMap.put(Number160.ZERO, new Data("string"));
            final AtomicInteger test1 = new AtomicInteger(0);
            final AtomicInteger test2 = new AtomicInteger(0);
            StorageMemory s1 = new StorageMemory();
            master = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(port).start()).storage(s1).start();
            IndirectReplication im = new IndirectReplication(master);
            System.err.println("master is " + master.peerAddress());

            ResponsibilityListener responsibilityListener = new ResponsibilityListener() {
                @Override
                public FutureDone<?> otherResponsible(final Number160 locationKey, final PeerAddress other) {
                    System.err.println("Other peer (" + other + ")is responsible for " + locationKey);
                    test1.incrementAndGet();
                    return new FutureDone<Void>().done();
                }

                @Override
                public FutureDone<?> meResponsible(final Number160 locationKey) {
                    System.err.println("I'm responsible for " + locationKey);
                    test2.incrementAndGet();
                    return new FutureDone<Void>().done();
                }
                @Override
                public FutureDone<?> meResponsible(Number160 locationKey, PeerAddress newPeer) {
	                System.err.println("I sync for " + locationKey + " / ");
	                return new FutureDone<Void>().done();
                }
            };

            im.addResponsibilityListener(responsibilityListener);
            im.start();

            FutureChannelCreator fcc = master.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(master, loc);
            putBuilder.domainKey(domainKey);
            putBuilder.dataMapContent(contentMap);
            putBuilder.versionKey(Number160.ZERO);

            master.storeRPC().put(master.peerAddress(), putBuilder, cc).awaitUninterruptibly();
            slave1 = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(port + 1).start()).start();
            slave2 = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(port + 2).start()).start();
            System.err.println("slave1 is " + slave1.peerAddress());
            System.err.println("slave2 is " + slave2.peerAddress());
            // master peer learns about the slave peers
            master.peerBean().peerMap().peerFound(slave1.peerAddress(), null, null, null);
            master.peerBean().peerMap().peerFound(slave2.peerAddress(), null, null, null);

            System.err.println("both peers online");
            PeerAddress slaveAddress1 = slave1.peerAddress();
            slave1.shutdown().await();
            master.peerBean().peerMap().peerFailed(slaveAddress1, new PeerException(AbortCause.SHUTDOWN, "shutdown"));

            Assert.assertEquals(1, test1.get());
            Assert.assertEquals(2, test2.get());

            PeerAddress slaveAddress2 = slave2.peerAddress();
            slave2.shutdown().await();
            master.peerBean().peerMap().peerFailed(slaveAddress2, new PeerException(AbortCause.SHUTDOWN, "shutdown"));

            Assert.assertEquals(1, test1.get());
            Assert.assertEquals(3, test2.get());

        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (master != null) {
                master.shutdown().await();
            }
            if (slave1 != null) {
            	slave1.shutdown().await();
            }
            if (slave2 != null) {
            	slave2.shutdown().await();
            }
        }

    }

	/**
	 * Test the responsibility and the notifications for the n-root replication
	 * approach with replication factor 1.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testReplication0Root1() throws Exception {
		final int replicationFactor = 1;

		Number160 keyA = new Number160("0xa");
		int[][] joinA = 
		// node a is responsible for key a
		{{ 1, 0, 0 },
		// node a remains responsible for key a
		{ 0, 0, 0 },
		// node a remains responsible for key a
		{ 0, 0, 0 },
		// node a remains responsible for key a
		{ 0, 0, 0 }};
		int[][] leaveA =
		// node b leaves, it doesn't affects key a's replica set
		{{ 0, 0, 0 },
		// node c leaves, it doesn't affects key a's replica set
		{ 0, 0, 0 },
		// node d leaves, it doesn't affects key a's replica set
		{ 0, 0, 0 }};
		testReplication(keyA, replicationFactor, false, joinA, leaveA);

		Number160 keyB = new Number160("0xb");
		int[][] expectedB = 
		// as first node, node a is responsible for key b
		{{ 1, 0, 0 },
		// node b joins and becomes responsible for key b (closer than
		// node a), node a has to notify newly
		// joined node b
		{ 0, 0, 1 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 }};
		int[][] leaveB =
		// node a has no replication responsibilities to check
		{{ 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 }};
		testReplication(keyB, replicationFactor, false, expectedB, leaveB);

		Number160 keyC = new Number160("0xc");
		int[][] expectedC =
		// as first node, node a is responsible for key c
		{ { 1, 0, 0 },
		// node b joins, node a is closer to key c than node b, node a
		// doesn't have to notify newly joined node b
		{ 0, 0, 0 },
		// node c joins, node c is closer to key c than node a, node c
		// becomes responsible for key c, node a has to notify newly
		// joined node c
		{ 0, 0, 1 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 } };
		int[][] leaveC =
		// node a has no replication responsibilities to check
		{ { 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 } };
		testReplication(keyC, replicationFactor, false, expectedC, leaveC);

		Number160 keyD = new Number160("0xd");
		int[][] expectedD = 
		// as first node, node a is responsible for key d
		{ { 1, 0, 0 },
		// node b joins, node b is closer to key d than node a and
		// becomes responsible for key d, node a has to notify newly joined node b
		{ 0, 0, 1 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 }};
		int[][] leaveD =
		// node a has no replication responsibilities to check
		{ { 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 }};
		testReplication(keyD, replicationFactor, false, expectedD, leaveD);
	}

	/**
	 * Test the responsibility and the notifications for the 0-root replication
	 * approach with replication factor 2.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testReplication0Root2() throws Exception {
		int replicationFactor = 2;

		Number160 keyA = new Number160("0xa");
		int[][] expectedA = 
		// as first node, node a has to replicate key a
		// replica set: a
		// responsible: a
		{{ 1, 0, 0 },
		// node b joins, node a is closest to key a, node a syncs to node b
		// replica set: a, b
		// responsible: a
		{ 0, 1, 0 },
		// node c joins, node c doesn't affect replica set
		// replica set: a, b
		// responsible: a
		{ 0, 0, 0 },
		// node d joins, node c doesn't affect replica set
		// replica set: a, b
		// responsible: a
		{ 0, 0, 0 }};
		int[][] leaveA =
		// node b leaves, node a has to notify the replica set
		// replica set: a, c
		// responsible: a
		{ { 1, 0, 0 },
		// node c leaves, node a has to notify the replica set
		// replica set: a, d
		// responsible: a
		{ 1, 0, 0 },
		// node d leaves, node a has to notify the replica set
		// responsible: a
		{ 1, 0, 0 }};
		testReplication(keyA, replicationFactor, false, expectedA, leaveA);

		Number160 keyB = new Number160("0xb");
		int[][] expectedB = 
		// as first node, node a has to replicate key b
		// replica set: a
		// responsible: a
		{ { 1, 0, 0 },
		// node b joins, node b is closer to key b than node a, node a
		// is still in replica set, node a notifies node b about
		// responsibility
		// replica set: a, b
		// responsible: b
		{ 0, 0, 1 },
		// node c joins, node a and b remains in replica set for key c
		// (node a and b are closer to key b than node c), node a
		// notifies nobody
		// replica set: a, b
		// responsible: b
		{ 0, 0, 0 },
		// node d joins, node a and b remains in replica set for key d
		// (node a and b are closer to key b than node d), node a
		// notifies nobody
		// replica set: a, b
		// responsible: b
		{ 0, 0, 0 }};
		int[][] leaveB =
		// node b leaves, node b was in replica set of key b, closest node a
		// becomes responsible, node a has to notify replica set
		// replica set: a, d
		// responsible: a
		{ { 1, 0, 0 },
		// node c leaves, node c doesn't affects replica set of key b
		// replica set: a, d
		// responsible: a
		{ 0, 0, 0 },
		// node d leaves, node d was in replica set of key b, node a has
		// to notify replica set
		// replica set: a
		// responsible: a
		{ 1, 0, 0 }};
		testReplication(keyB, replicationFactor, false, expectedB, leaveB);

		Number160 keyC = new Number160("0xc");
		int[][] expectedC = 
		// as first node, node a has to replicate key c
		// replica set: a
		// responsible: a
		{ { 1, 0, 0 },
		// node b joins and is in replica set of key c, closer node a
		// syncs to node b
		// replica set: a, b
		// responsible: a
		{ 0, 1, 0 },
		// node c joins, node c is closest to key c and becomes
		// responsible, node a has to notify node c
		// replica set: a, c
		// responsible: c
		{ 0, 0, 1},
		// node d joins, node c and d is closer to key c, node a is not
		// in replica set anymore, node a has to notify node c
		// replica set: c, d
		// responsible: c
		{ 0, 0, 1 }};
		int[][] leaveC =
		// leaving node b doesn't affect replica set or node a
		// replica set: c, d
		// responsible: c
		{ { 0, 0, 0 },
		// node c leaves, node a is now in replica set of key c, but
		// node d is responsible for replication, thus node a notifies
		// nobody (meanwhile node d should notify node a, not tested here)
		// replica set: a, d
		// responsible: d
		{ 0, 0, 0 },
		// node d leaves, node a becomes responsible for key c, but in
		// this test node a doesn't get notified by node d and therefore
		// node a doesn't know about it's responsibility
		// replica set: a
		// responsible: a
		{ 0, 0, 0 }};
		testReplication(keyC, replicationFactor, false, expectedC, leaveC);

		Number160 keyD = new Number160("0xd");
		int[][] expectedD = 
		// as first node, node a is responsible for key d
		{ { 1, 0, 0 },
		// node b joins, node a and b are in the replica set, closer
		// node b becomes responsible for key d, node a has to notify
		// node b 
		// replica set: a, b
		// responsible: b
		{ 0, 0, 1 },
		// node c joins, closer nodes b and c are in replica set of key
		// d, closer node c becomes responsible, node a is no more in
		// replica set of key d, node a has to notify node c
		// replica set: b, c
		// responsible: c
		{ 0, 0, 1 },
		// node d joins, closer nodes c and d are in replica set of key
		// d, closer node d becomes responsible, node a notifies nobody
		// replica set: c, d
		// responsible: d
		{ 0, 0, 0 }};
		int[][] leaveD =
		// node b leaves and doesn't affects replica set of key d
		// replica set: c, d
		// responsible: d
		{ { 0, 0, 0 },
		// node c leaves and node a joins replica set of key d, node d
		// would be responsible to notify node a
		// replica set: a, d
		// responsible: d
		{ 0, 0, 0 },
		// node d leaves and node a becomes responsible of key d, but
		// node a wasn't previously notified by node d
		// replica set: a
		// responsible: a
		{ 0, 0, 0 }};
		testReplication(keyD, replicationFactor, false, expectedD, leaveD);
	}

	/**
	 * Test the responsibility and the notifications for the 0-root replication
	 * approach with replication factor 3.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testReplication0Root3() throws Exception {
		int replicationFactor = 3;

		Number160 key = new Number160("0xa");
		int[][] expectedA = 
		// as first node, node a has to replicate key a
		{ { 1, 0, 0 },
		// node b joins, node b is in replica set of key a, node a
		// remains responsible, node a has to sync to node b
		// replica set: a, b
		// responsible: a
		{ 0, 1, 0 },
		// node c joins, node c is in replica set of key a, node a
		// remains responsible, node a has to sync to node c
		// replica set: a, b, c
		// responsible: a
		{ 0, 1, 0 },
		// node d joins, node d doesn't affects replica set of key a
		// replica set: a, b, c
		// responsible: a
		{ 0, 0, 0 }};
		int[][] leaveA =
		// node b leaves, node b was in replica set, node a remains responsible,
		// node a has to notify replica set
		// replica set: a, c, d
		// responsible: a
		{ { 1, 0, 0 },
		// node c leaves, node c was in replica set, node a remains
		// responsible, node a has to notify replica set
		// replica set: a, d
		// responsible: a
		{ 1, 0, 0 },
		// node d leaves, node d was in replica set, node a remains
		// responsible, node a has to notify replica set
		// replica set: a
		// responsible: a
		{ 1, 0, 0 }};
		testReplication(key, replicationFactor, false, expectedA, leaveA);

		key = new Number160("0xb");
		int[][] expectedB = 
		// as first node, node a has to replicate key b
		{{ 1, 0, 0 },
		// node b joins, node b is in replica set, node becomes
		// responsible for key b, node a has to notify node b
		// replica set: a, b
		// responsible: b
		{ 0, 0, 1 },
		// node c joins, node c is in replica set, node b remains
		// responsible, therefore node a doesn't have to notify
		// replica set: a, b, c
		// responsible: b
		{ 0, 0, 0 },
		// node d joins, node d is in replica set, node b remains
		// responsible, therefore node a doesn't have to notify
		// replica set: a, b, d
		// responsible: b
		{ 0, 0, 0 }};
		int[][] leaveB =
		// node b leaves, node a becomes responsible, node a notifies replica set
		// replica set: a, c, d
		// responsible: a
		{{ 1, 0, 0},
		// node c leaves, node a remains responsible, node c was in
		// replica set, node a notifies replica set
		// replica set: a, d
		// responsible: a
		{ 1, 0, 0 },
		// node d leaves, node a remains responsible, node d was in
		// replica set, node a notifies replica set
		// replica set: a
		// responsible: a
		{ 1, 0, 0 }};
		testReplication(key, replicationFactor, false, expectedB, leaveB);

		key = new Number160("0xc");
		int[][] expectedC = 
		// as first node, node a has to replicate key c
		{{ 1, 0, 0 },
		// node b joins, node b is in replica set of key c, node a
		// remains responsible, node a syncs to node b
		// replica set: a, b
		// responsible: a
		{ 0, 1, 0 },
		// node c joins, node c is in replica set of key c, node c
		// becomes responsible, node a notifies node c
		// replica set: a, b, c
		// responsible: c
		{ 0, 0, 1 },
		// node d joins, node d is in replica set of key c, node c
		// remains responsible, node a notifies no one
		// replica set: a, c, d
		// responsible: c
		{ 0, 0, 0 }};
		int[][] leaveC =
		// node b leaves, node b doesn't affect replica set of key c
		// replica set: a, c, d
		// responsible: c
		{{ 0, 0, 0},
		// node c leaves, node c was in replica set of key c, node d
		// becomes responsible, node a notifies node d about responsibility
		// replica set: a, d
		// responsible: d
		{ 0, 0, 1 },
		// node d leaves, node d was responsible for key c, node a
		// becomes responsible for key c, node a notifies replica set
		// replica set: a
		// responsible: a
		{ 1, 0, 0 }};
		testReplication(key, replicationFactor, false, expectedC, leaveC);

		key = new Number160("0xd");
		int[][] expectedD = 
		// as first node, node a has to replicate key d
		{ { 1, 0, 0 },
		// node b joins, node b is in replica set of key d, node b
		// becomes responsible, node a notifies node b
		// replica set: a, b
		// responsible: b
		{ 0, 0, 1 },
		// node c joins, node c is in replica set of key d, node c
		// becomes responsible, node a notifies node c
		// replica set: a, b, c
		// responsible: c
		{ 0, 0, 1 },
		// node d joins, node d is in replica set of key d, node d
		// becomes responsible, node a notifies node d
		// replica set: b, c, d
		// responsible: d
		{ 0, 0, 1 }};
		int[][] leaveD =
		// node a has no responsibilities to check
		{{ 0, 0, 0},
		// node a has no responsibilities to check
		{ 0, 0, 0 },
		// node a has no responsibilities to check
		{ 0, 0, 0 }};
		testReplication(key, replicationFactor, false, expectedD, leaveD);
	}
	
	/**
	 * Distances between a, b, c and d:
	 * a <-0001-> b <-0111-> c <-0001-> d <-0111-> a,
	 * a <-0110-> c, b <-0110-> d
	 */

	/**
	 * Test the responsibility and the notifications for the n-root replication
	 * approach with replication factor 1.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testReplicationNRoot1() throws Exception {
		final int replicationFactor = 1;

		Number160 keyA = new Number160("0xa");
		int[][] joinA = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node a is closer to key a than node b, node a doesn't have to notify
		// newly joined node b
		{ 0, 0, 0 },
		// node a is closer to key a than node c, node a doesn't have to notify
		// newly joined node c
		{ 0, 0, 0 },
		// node a is closer to key a than node d, node a doesn't have to notify
		// newly joined node d
		{ 0, 0, 0 }};
		int[][] leaveA =
		// node b leaves, node a has still to replicate key a
		{{ 0, 0, 0 },
		// node c leaves, node a has still to replicate key a
		{ 0, 0, 0 },
		// node d leaves, node a has still to replicate key a
		{ 0, 0, 0 }};
		testReplication(keyA, replicationFactor, true, joinA, leaveA);

		Number160 keyB = new Number160("0xb");
		int[][] expectedB = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node b is closer to key b than node a, node a has to notify newly
		// joined node b
		{ 0, 0, 1 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 }};
		int[][] leaveB =
		// node b leaves, node a has no replication responsibilities to check
		{{ 0, 0, 0 },
		// node c leaves, node a has no replication responsibilities to check
		{ 0, 0, 0 },
		// node d leaves, node a has no replication responsibilities to check
		{ 0, 0, 0 }};
		testReplication(keyB, replicationFactor, true, expectedB, leaveB);

		Number160 keyC = new Number160("0xc");
		int[][] expectedC =
		// as first node, node a is always replicating given key
		{ { 1, 0, 0 },
		// node a is closer to key c than node b, node a doesn't have to
		// notify newly joined node b
		{ 0, 0, 0 },
		// node c is closer to key c than node a, node a doesn't have to
		// replicate key c anymore, node a has to notify newly joined
		// node c
		{ 0, 0, 1 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 } };
		int[][] leaveC =
		// node a doesn't know any replication responsibilities for leaving node
		// b
		{ { 0, 0, 0 },
		// node a doesn't know any replication responsibilities for leaving node
		// c
		{ 0, 0, 0 },
		// node a doesn't know any replication responsibilities for leaving node
		// d
		{ 0, 0, 0 } };
		testReplication(keyC, replicationFactor, true, expectedC, leaveC);

		Number160 keyD = new Number160("0xd");
		int[][] expectedD = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node b is closer to key d than node a, node a has to notify newly
		// joined node b
		{ 0, 0, 1 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 }};
		int[][] leaveD =
		// node a doesn't know any replication responsibilities of
		// leaving node b
		{ { 0, 0, 0 },
		// node a doesn't know any replication responsibilities of
		// leaving node c
		{ 0, 0, 0 },
		// node a doesn't know any replication responsibilities of
		// leaving node d
		{ 0, 0, 0 }};
		testReplication(keyD, replicationFactor, true, expectedD, leaveD);
	}

	/**
	 * Test the responsibility and the notifications for the n-root replication
	 * approach with replication factor 2.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testReplicationNRoot2() throws Exception {
		int replicationFactor = 2;

		Number160 keyA = new Number160("0xa");
		int[][] expectedA = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node a and b have to replicate key a, node a has to notify newly
		// joined node b
		{ 0, 1, 0 },
		// node a and b still have to replicate key a (node a and b are closer
		// to key a than newly joined node c), node a doesn't has to notify
		// newly joined node c
		{ 0, 0, 0 },
		// node a and b still have to replicate key a (node b is closer to key a
		// than newly joined node d), node a doesn't has to notify newly joined
		// node d
		{ 0, 0, 0 }};
		int[][] leaveA =
		// leaving node b was also responsible for key a, node a has to
		// notify it's replications set (node a and c are closest to key a)
		{ { 1, 0, 0 },
		// leaving node c was also responsible for key a, node a has to
		// notify it's replications set (node a and d are closest to key a)
		{ 1, 0, 0 },
		// leaving node d was also responsible for key a, node a has to
		// notify it's replications set
		{ 1, 0, 0 }};
		testReplication(keyA, replicationFactor, true, expectedA, leaveA);

		Number160 keyB = new Number160("0xb");
		int[][] expectedB = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node a and b have to replicate key b, node a has to notify newly
		// joined node b
		{ 0, 1, 0 },
		// node a and b still have to replicate key b (node a and b are closer
		// to key b than newly joined c), node a doesn't has to notify newly
		// joined node c
		{ 0, 0, 0 },
		// node a and b still have to replicate key b (node a and b are closer
		// to key b than newly joined d), node a doesn't has to notify newly
		// joined node d
		{ 0, 0, 0 }};
		int[][] leaveB =
		// leaving node b was also responsible for key b, node a has to
		// notify it's replications set (node a and d are closest to key a)
		{ { 1, 0, 0 },
		// leaving node c was not responsible for key b, node a has not to
		// notify someone (node a and d are closest to key a)
		{ 0, 0, 0 },
		// leaving node d was also responsible for key a, node a has to
		// notify it's replications set
		{ 1, 0, 0 }};
		testReplication(keyB, replicationFactor, true, expectedB, leaveB);

		Number160 keyC = new Number160("0xc");
		int[][] expectedC = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node a and b have to replicate key c, node a has to notify newly
		// joined node b
		{ 0, 1, 0 },
		// node a and c have to replicate key c (node a and c are closer to key
		// c than node b), node a has to notify newly joined node c
		{ 0, 1, 0 },
		// node c and d have to replicate key c (node c and d are closer to key
		// c than node a), node a doesn't has to replicate anymore, node a has
		// to notify newly joined node d
		{ 0, 0, 1 }};
		int[][] leaveC =
		// node a doesn't know any replication responsibilities of
		// leaving node b
		{ { 0, 0, 0 },
		// node a doesn't know any replication responsibilities of
		// leaving node c
		{ 0, 0, 0 },
		// node a doesn't know any replication responsibilities of
		// leaving node d
		{ 0, 0, 0 }};
		testReplication(keyC, replicationFactor, true, expectedC, leaveC);

		Number160 keyD = new Number160("0xd");
		int[][] expectedD = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node a and b have to replicate key d, node a has to notify newly
		// joined node b
		{ 0, 1, 0 },
		// node b and c have to replicate key d (node b and c are closer to key
		// d than node a), node a has to notify newly joined node c
		{ 0, 0, 1 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 }};
		int[][] leaveD =
		// node a has no replication responsibilities to check
		{ { 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 }};
		testReplication(keyD, replicationFactor, true, expectedD, leaveD);
	}
	
	/**
	 * Test the responsibility and the notifications for the n-root replication
	 * approach with replication factor 3.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testReplicationNRoot3() throws Exception {
		int replicationFactor = 3;

		Number160 key = new Number160("0xa");
		int[][] expectedA = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node a and b have to replicate key a, node a has to notify newly
		// joined node b
		{ 0, 1, 0 },
		// node a, b and c have to replicate key a, node a has to notify newly
		// joined node c
		{ 0, 1, 0 },
		// node c is not closer to key a than node a, b and c, node a doesn't
		// have to notify newly joined node d
		{ 0, 0, 0 }};
		int[][] leaveA =
		// node b leaves, node b was also responsible for key a, notify replica
		// set
		{ { 1, 0, 0 },
		// node c leaves, node c was also responsible for key a, notify replica
		// set
		{ 1, 0, 0 },
		// node d leaves, node d was also responsible for key a, notify replica
		// set
		{ 1, 0, 0 }};
		testReplication(key, replicationFactor, true, expectedA, leaveA);

		key = new Number160("0xb");
		int[][] expectedB = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node a and b have to replicate key b, node a has to notify newly
		// joined node b
		{ 0, 1, 0 },
		// node a, b and c have to replicate key b, node a has to notify newly
		// joined node c
		{ 0, 1, 0 },
		// node a, b and d have to replicate key b (node a, b and d are closer
		// to key b than node c), node a has to notify newly joined node d
		{ 0, 1, 0 }};
		int[][] leaveB =
		// node b leaves, node b was also responsible for key a, notify replica
		// set
		{{ 1, 0, 0},
		// node c leaves, node c was also responsible for key a, notify replica
		// set
		{ 1, 0, 0 },
		// node d leaves, node d was also responsible for key a, notify replica
		// set
		{ 1, 0, 0 }};
		testReplication(key, replicationFactor, true, expectedB, leaveB);

		key = new Number160("0xc");
		int[][] expectedC = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node a and b have to replicate key c, node a has to notify newly
		// joined node b
		{ 0, 1, 0 },
		// node a, b and c have to replicate key c, node a has to notify newly
		// joined node c
		{ 0, 1, 0 },
		// node a, c and d have to replicate key c (node a, c and d are closer
		// to key b than node b), node a has to notify newly joined node d
		{ 0, 1, 0 }};
		int[][] leaveC =
		// node b leaves, node b was not responsible for key a
		{{ 0, 0, 0},
		// node c leaves, node c was also responsible for key a, notify replica
		// set
		{ 1, 0, 0 },
		// node d leaves, node d was also responsible for key a, notify replica
		// set
		{ 1, 0, 0 }};
		testReplication(key, replicationFactor, true, expectedC, leaveC);

		key = new Number160("0xd");
		int[][] expectedD = 
		// as first node, node a is always replicating given key
		{{ 1, 0, 0 },
		// node a and b have to replicate key d, node a has to notify newly
		// joined node b
		{ 0, 1, 0 },
		// node a, b and c have to replicate key d, node a has to notify newly
		// joined node c
		{ 0, 1, 0 },
		// node b, c and d have to replicate key d (node b, c and d are closer
		// to key d than node a), node a doesn't have to replicate anymore, node
		// a has to notify newly joined node d
		{ 0, 0, 1 }};
		int[][] leaveD =
		// node a has no replication responsibilities to check
		{{ 0, 0, 0},
		// node a has no replication responsibilities to check
		{ 0, 0, 0 },
		// node a has no replication responsibilities to check
		{ 0, 0, 0 }};
		testReplication(key, replicationFactor, true, expectedD, leaveD);
	}

	/**
	 * Distances between a, b, c and d:
	 * a <-0001-> b <-0111-> c <-0001-> d <-0111-> a,
	 * a <-0110-> c, b <-0110-> d
	 */
	private void testReplication(Number160 lKey, int replicationFactor, boolean nRoot, int[][] joins, int[][] leaves)
			throws IOException, InterruptedException {
		List<PeerDHT> peers = new ArrayList<PeerDHT>(joins.length);
		IndirectReplication ind = null;
		ChannelCreator cc = null;
		try {
			char[] letters = { 'a', 'b', 'c', 'd' };
			for (int i = 0; i < joins.length; i++) {
				if(i == 0) {
					PeerDHT peer = new PeerBuilderDHT(new PeerBuilder(new Number160("0x" + letters[i])).ports(Ports.DEFAULT_PORT + i)
							.start()).start();
					ind = new IndirectReplication(peer).replicationFactor(replicationFactor).nRoot(nRoot).start();
					peers.add(peer);
				} else {
					PeerDHT peer = new PeerBuilderDHT(new PeerBuilder(new Number160("0x" + letters[i])).ports(Ports.DEFAULT_PORT + i)
							.start()).start(); 
					new IndirectReplication(peer).replicationFactor(replicationFactor).nRoot(nRoot).start();
					peers.add(peer);	
				}
			}
			
			PeerDHT master = peers.get(0);
			
			// attach test listener for test verification
			final AtomicInteger replicateOther = new AtomicInteger(0);
			final AtomicInteger replicateI = new AtomicInteger(0);
			final AtomicInteger replicateWe = new AtomicInteger(0);
			ind.addResponsibilityListener(new ResponsibilityListener() {
				@Override
				public FutureDone<?> otherResponsible(final Number160 locationKey, final PeerAddress other) {
					replicateOther.incrementAndGet();
					return new FutureDone<Void>().done();
				}

				@Override
				public FutureDone<?> meResponsible(final Number160 locationKey) {
					replicateI.incrementAndGet();
					return new FutureDone<Void>().done();
				}

				@Override
				public FutureDone<?> meResponsible(Number160 locationKey, PeerAddress newPeer) {
					replicateWe.incrementAndGet();
					return new FutureDone<Void>().done();
				}
			});
			
			// create test data with given location key
			Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
			dataMap.put(Number160.ZERO, new Data("string"));
			PutBuilder putBuilder = new PutBuilder(master, lKey);
			putBuilder.domainKey(Number160.ZERO);
			putBuilder.dataMapContent(dataMap);
			putBuilder.versionKey(Number160.ZERO);

			FutureChannelCreator fcc = master.peer().connectionBean().reservation().create(0, 1);
			fcc.awaitUninterruptibly();
			cc = fcc.channelCreator();

			// put test data
			FutureResponse fr = master.storeRPC().put(master.peerAddress(), putBuilder, cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(joins[0][0], replicateI.get());
			replicateI.set(0);
			Assert.assertEquals(joins[0][1], replicateWe.get());
			replicateWe.set(0);
			Assert.assertEquals(joins[0][2], replicateOther.get());
			replicateOther.set(0);
			
			for (int i = 1; i < joins.length; i++) {
				// insert a peer
				final List<BaseFuture> bf = Collections.synchronizedList(new ArrayList<BaseFuture>());
				AutomaticFuture af = new AutomaticFuture() {
					@Override
					public void futureCreated(BaseFuture future) {
						bf.add(future);
					}
				};
				master.peer().addAutomaticFuture(af);
				master.peerBean().peerMap().peerFound(peers.get(i).peerAddress(), null, null, null);
				master.peer().removeAutomaticFuture(af);
				for(BaseFuture b:bf) {
					b.awaitUninterruptibly();
					b.awaitListenersUninterruptibly();
				}
				
				// verify replication notifications
				Assert.assertEquals(joins[i][0], replicateI.get());
				replicateI.set(0);
				Assert.assertEquals(joins[i][1], replicateWe.get());
				replicateWe.set(0);
				System.out.println(i);
				Assert.assertEquals(joins[i][2], replicateOther.get());
				replicateOther.set(0);
			}
			
			for (int i = 0; i < leaves.length; i++) {
				// remove a peer
				master.peerBean().peerMap().peerFailed(peers.get(i+1).peerAddress(), new PeerException(AbortCause.SHUTDOWN, "shutdown"));
				// verify replication notifications
				Assert.assertEquals(leaves[i][0], replicateI.get());
				replicateI.set(0);
				Assert.assertEquals(leaves[i][1], replicateWe.get());
				replicateWe.set(0);
				Assert.assertEquals(leaves[i][2], replicateOther.get());
				replicateOther.set(0);
			}
			
		} finally {
			if (cc != null) {
				cc.shutdown().awaitUninterruptibly();
			}
			for (PeerDHT peer: peers) {
				peer.shutdown().await();
			}
		}
	}
	
	

	@Test
	public void testHeavyLoadNRootReplication() throws Exception {
		PeerDHT master = null;
		Random rnd = new Random();
		try {
			// setup
			PeerDHT[] peers = Utils2.createNodes(10, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			
			for (int i = 0; i < peers.length; i++) {
				new net.tomp2p.replication.IndirectReplication(peers[i]).intervalMillis(500).nRoot()
				.replicationFactor(6).start();
			}

			NavigableMap<Number160, Data> sortedMap = new TreeMap<Number160, Data>();
			Number160 locationKey = Number160.createHash("location");
			Number160 domainKey = Number160.createHash("domain");
			Number160 contentKey = Number160.createHash("content");

			String content = "";
			for (int i = 0; i < 500; i++) {
				content += "a";
				Number160 vKey = generateVersionKey(i, content);
				Data data = new Data(content);
				if (!sortedMap.isEmpty()) {
					data.addBasedOn(sortedMap.lastKey());
				}
				data.prepareFlag();
				sortedMap.put(vKey, data);

				// put test data (prepare)
				FuturePut fput = peers[rnd.nextInt(10)].put(locationKey)
						.data(contentKey, sortedMap.get(vKey)).domainKey(domainKey).versionKey(vKey).start();
				fput.awaitUninterruptibly();
				fput.futureRequests().awaitUninterruptibly();
				fput.futureRequests().awaitListenersUninterruptibly();
				Assert.assertEquals(true, fput.isSuccess());

				// confirm put
				FuturePut futurePutConfirm = peers[rnd.nextInt(10)].put(locationKey).domainKey(domainKey)
						.data(contentKey, new Data()).versionKey(vKey).putConfirm().start();
				futurePutConfirm.awaitUninterruptibly();
				futurePutConfirm.awaitListenersUninterruptibly();

				// get latest version with digest
				FutureGet fget = peers[rnd.nextInt(10)].get(locationKey).domainKey(domainKey)
						.contentKey(contentKey).getLatest().withDigest().start();
				fget.awaitUninterruptibly();
				Assert.assertTrue(fget.isSuccess());

				// check result
				Map<Number640, Data> dataMap = fget.dataMap();
				Assert.assertEquals(1, dataMap.size());
				Number480 key480 = new Number480(locationKey, domainKey, contentKey);

				Number640 key = new Number640(key480, vKey);
				Assert.assertTrue(dataMap.containsKey(key));
				Assert.assertEquals(data.object(), dataMap.get(key).object());

				// check digest result
				DigestResult digestResult = fget.digest();
				Assert.assertEquals(sortedMap.size(), digestResult.keyDigest().size());
				for (Number160 versionKey : sortedMap.keySet()) {
					Number640 digestKey = new Number640(locationKey, domainKey, contentKey, versionKey);
					Assert.assertTrue(digestResult.keyDigest().containsKey(digestKey));
					Assert.assertEquals(sortedMap.get(versionKey).basedOnSet().size(), digestResult
							.keyDigest().get(digestKey).size());
					for (Number160 bKey : sortedMap.get(versionKey).basedOnSet()) {
						Assert.assertTrue(digestResult.keyDigest().get(digestKey).contains(bKey));
					}
				}
			}
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	private static Number160 generateVersionKey(long basedOnCounter, Serializable object) throws IOException {
		// get a MD5 hash of the object itself
		byte[] hash = generateMD5Hash(serializeObject(object));
		return new Number160(basedOnCounter, new Number160(Arrays.copyOf(hash, Number160.BYTE_ARRAY_SIZE)));
	}

	private static byte[] generateMD5Hash(byte[] data) {
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
		}
		md.reset();
		md.update(data, 0, data.length);
		return md.digest();
	}

	private static byte[] serializeObject(Serializable object) throws IOException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		byte[] result = null;

		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			result = baos.toByteArray();
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				if (oos != null)
					oos.close();
				if (baos != null)
					baos.close();
			} catch (IOException e) {
				throw e;
			}
		}
		return result;
	}

}
