package net.tomp2p.relay.android;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.PutBuilder;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.buffer.MessageBufferConfiguration;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestAndroidRelay {

	private final TestRelay testRelay;
	
	private final MockedAndroidRelayClientConfig clientConfig;
	private final MockedAndroidRelayServerConfig serverConfig;

	@SuppressWarnings("rawtypes")
	@Parameterized.Parameters(name = "{0}")
	public static Collection data() throws Exception {
		return Arrays.asList(new Object[][] {
				{ new MessageBufferConfiguration().bufferAgeLimit(2000).bufferCountLimit(10).bufferSizeLimit(Long.MAX_VALUE), 10 },
				{ new MessageBufferConfiguration().bufferAgeLimit(Long.MAX_VALUE).bufferCountLimit(1).bufferSizeLimit(Long.MAX_VALUE), 10 } }
		);
	}
	
	public TestAndroidRelay(MessageBufferConfiguration bufferConfig, int peerMapIntervalS) {
		clientConfig = new MockedAndroidRelayClientConfig(peerMapIntervalS);
		serverConfig = new MockedAndroidRelayServerConfig(bufferConfig, clientConfig);
		testRelay = new TestRelay(RelayType.ANDROID, serverConfig, clientConfig);
	}

	@Test
	public void testSetupRelayPeers() throws Exception {
		testRelay.testSetupRelayPeers();
	}

	@Test
	public void testBoostrap() throws Exception {
		testRelay.testBoostrap();
	}

	/**
	 * Tests sending a message from an unreachable peer to another unreachable peer
	 */
	@Test
	public void testRelaySendDirect() throws Exception {
		testRelay.testRelaySendDirect();
	}
	
	/**
	 * Tests sending a message from a reachable peer to an unreachable peer
	 */
	@Test
	public void testRelaySendDirect2() throws Exception {
		testRelay.testRelaySendDirect2();
	}
	
	/**
	 * Tests sending a message from an unreachable peer to a reachable peer
	 */
	@Test
	public void testRelaySendDirect3() throws Exception {
		testRelay.testRelaySendDirect3();
	}

	@Test
	public void testRelayRouting() throws Exception {
		testRelay.testRelayRouting();
	}

	@Test
	public void testNoRelayDHT() throws Exception {
		testRelay.testNoRelayDHT();
	}

	@Test
	public void testRelayDHTSimple() throws Exception {
		testRelay.testRelayDHTSimple();
	}

	@Test
	public void testRelayDHT() throws Exception {
		testRelay.testRelayDHT();
	}

	@Test
	public void testRelayDHTPutGet() throws Exception {
		testRelay.testRelayDHTPutGet();
	}

	@Test
	public void testRelayDHTPutGet2() throws Exception {
		testRelay.testRelayDHTPutGet2();
	}

	@Test
    public void testRelayDHTPutGetSigned() throws Exception {
        testRelay.testRelayDHTPutGetSigned();
    }

	@Test
	public void testVeryFewPeers() throws Exception {
		testRelay.testVeryFewPeers();
	}

	@Test
	public void testRelaySlowPeer() throws Exception {
		// make the timeout very small, such that the mobile device is too slow to answer the request
		int slowResponseTimeoutS = 2;
		
		final Random rnd = new Random(42);
		PeerDHT master = null;
		PeerDHT unreachablePeer = null;
		try {
			PeerDHT[] peers = UtilsNAT.createNodesDHT(10, rnd, 4000);
			master = peers[0]; // the relay peer
			UtilsNAT.perfectRouting(peers);
			for (PeerDHT peer : peers) {
				new PeerBuilderNAT(peer.peer()).addRelayServerConfiguration(RelayType.ANDROID, serverConfig).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start()).start();
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer.peer()).start();

			FutureRelayNAT fbn = uNat.startRelay(clientConfig, master.peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(fbn.isSuccess());

			// wait to be about in the middle of two map updates
			Thread.sleep(clientConfig.peerMapUpdateInterval() * 500);
			// block message buffers transmitted through the map update task
			clientConfig.getClient(unreachablePeer.peerAddress()).bufferReceptionDelay(slowResponseTimeoutS * 1200);

			RoutingConfiguration r = new RoutingConfiguration(5, 1, 1);
            RequestP2PConfiguration rp = new RequestP2PConfiguration(1, 1, 0);
            
			PutBuilder builder = peers[8].put(unreachablePeer.peerID()).data(new Data("hello")).routingConfiguration(r).requestP2PConfiguration(rp);
			builder.slowResponseTimeoutSeconds(slowResponseTimeoutS);
			FuturePut futurePut = builder.start().awaitUninterruptibly();
			// the relayed one is the slowest, so we need to wait for it!
			futurePut.futureRequests().awaitUninterruptibly();

			System.err.println(futurePut.failedReason());
			// should be run into a timeout
			Assert.assertFalse(futurePut.isSuccess());
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
