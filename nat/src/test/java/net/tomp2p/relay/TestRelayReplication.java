package net.tomp2p.relay;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.relay.tcp.TCPRelayClientConfig;
import net.tomp2p.replication.IndirectReplication;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRelayReplication {

	private static final Random rnd = new Random(42);
	
	private final int totalNumberPeers;
	private final int replicationFactor;
	
	@SuppressWarnings("rawtypes")
	@Parameterized.Parameters(name = "Peers: {0}, ReplicationFactor: {1}")
	public static Collection data() throws Exception {
		return Arrays.asList(new Object[][] {
				{ 3, 5 },
				{ 3, 3 },
				{ 10, 5}});
	}

	public TestRelayReplication(int totalNumberPeers, int replicationFactor) {
		assert totalNumberPeers > 2;
		this.totalNumberPeers = totalNumberPeers;
		this.replicationFactor = replicationFactor;
	}
	
	private void startReplication(int intervalMillis, boolean nRoot, PeerDHT... peers) {
		for (PeerDHT peer : peers) {
			IndirectReplication replication = new IndirectReplication(peer);
			// set replication factor
			replication.replicationFactor(replicationFactor);
			// set replication frequency
			replication.intervalMillis(intervalMillis);
			// set kind of replication, default is 0Root
			if (nRoot) {
				replication.nRoot();
			}
			// set flag to keep data, even when peer looses replication responsibility
			replication.keepData(true);
			// start the indirect replication
			replication.start();
		}
	}
	
	@Test
	public void testRelayDHT() throws Exception {
		PeerDHT master = null;
		PeerDHT unreachablePeer = null;
		try {
			PeerDHT[] peers = UtilsNAT.createNodesDHT(totalNumberPeers - 1, rnd, 4000);
			master = peers[0]; // the relay peer
			UtilsNAT.perfectRouting(peers);
			for (PeerDHT peer : peers) {
				new PeerBuilderNAT(peer.peer()).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start())
					.start();
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer.peer()).start();
			
			startReplication(300000, true, peers);
			startReplication(300000, true, unreachablePeer);

			FutureRelayNAT fbn = uNat.startRelay(new TCPRelayClientConfig(), master.peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(fbn.isSuccess());

			Number160 contentKey = Number160.createHash(142);
			Number160 domainKey = Number160.createHash(921);
			
			FuturePut futurePut = peers[0].put(unreachablePeer.peerID()).data(contentKey, new Data("hello"), Number160.ZERO).domainKey(domainKey).start().awaitUninterruptibly();
			// the relayed one is the slowest, so we need to wait for it!
			futurePut.futureRequests().awaitUninterruptibly();
			Assert.assertTrue(futurePut.isSuccess());
			
			// we cannot see the peer in futurePut.rawResult, as the relayed is the slowest and we finish
			// earlier than that.
			Assert.assertTrue(unreachablePeer.storageLayer().contains(
					new Number640(unreachablePeer.peerID(), domainKey, contentKey, Number160.ZERO)));
			
			// Wait for the replication to calm down. Compare logs to see that many messages are sent forth and back over the relay peer
			Thread.sleep(5000);
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
			if (unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
		}
	}
}
