package net.tomp2p.relay;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

public class TestSlow {

	private Peer reachable = null;
	private Peer slow = null;
	private Peer master = null;
	private Peer[] peers = null;

	private static final int PORTS = 4001;
	private static final int NUMBER_OF_NODES = 5;
	private static final Random RND = new Random();

	@Before
	public void setupRelay() throws Exception {
		// setup test peers
		peers = UtilsNAT.createNodes(NUMBER_OF_NODES, RND, PORTS);
		master = peers[0];
		reachable = peers[4];
		UtilsNAT.perfectRouting(peers);
		// every peer must own a PeerNAT in order to be able to be a relay and
		// set up a reverse connection
		for (Peer peer : peers) {
			new PeerBuilderNAT(peer).bufferTimeoutSeconds(10).start();
		}

		// Test setting up relay peers
		slow = new PeerBuilder(Number160.createHash(RND.nextInt())).ports(PORTS + 1).start();
		PeerAddress pa = slow.peerBean().serverPeerAddress();
		pa = pa.changeFirewalledTCP(true).changeFirewalledUDP(true).changeSlow(true);
		slow.peerBean().serverPeerAddress(pa);
		
		// find neighbors
		FutureBootstrap futureBootstrap = slow.bootstrap().peerAddress(peers[0].peerAddress()).start();
		futureBootstrap.awaitUninterruptibly();
		Assert.assertTrue(futureBootstrap.isSuccess());
		
		// setup relay
		PeerNAT uNat = new PeerBuilderNAT(slow).bufferTimeoutSeconds(10).peerMapUpdateIntervalSeconds(10).start();
		uNat.startRelay(master.peerAddress());
		Thread.sleep(5000);

		// Check if flags are set correctly
		Assert.assertTrue(slow.peerAddress().isRelayed());
		Assert.assertFalse(slow.peerAddress().isFirewalledTCP());
		Assert.assertFalse(slow.peerAddress().isFirewalledUDP());
		Assert.assertTrue(slow.peerAddress().isSlow());

		System.err.println("master = " + master.peerAddress());
		System.err.println("reachable = " + reachable.peerAddress());
		System.err.println("unreachable = " + slow.peerAddress());
	}

	@Test
	public void testSlow() throws Exception {
		System.err.println("testReverseConnection() start!");

		final String requestString = "This is a test String";
		final String replyString = "SUCCESS HIT";
		final CountDownLatch cLatch = new CountDownLatch(1);

		slow.objectDataReply(new ObjectDataReply() {

			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				if (requestString.equals((String) request)) {
					System.err.println("received: " + (String) request);
					cLatch.countDown();
				}
				return replyString;
			}
		});

		FutureDirect fd = reachable.sendDirect(slow.peerAddress()).object(requestString).start();
		fd.awaitUninterruptibly();

		checkFail(cLatch);

		System.err.println("testReverseConnection() end!");
	}

	private void checkFail(final CountDownLatch cLatch) throws InterruptedException {
		if (!cLatch.await(25, TimeUnit.SECONDS)) {
			Assert.fail("The test method did not complete successfully! Still has " + cLatch.getCount() + " counts.");
		}
	}

	@After
	public void shutdown() {
		System.err.println("shutdown initiated!");
		for (Peer ele : peers) {
			BaseFuture bf = ele.shutdown();
			bf.awaitUninterruptibly();
		}
		BaseFuture bf = slow.shutdown();
		bf.awaitUninterruptibly();
		System.err.println("shutdown done!");
	}
}
