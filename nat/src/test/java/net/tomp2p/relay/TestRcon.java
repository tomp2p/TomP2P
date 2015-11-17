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

public class TestRcon {

	private Peer reachable = null;
	private Peer unreachable = null;
	private Peer master = null;
	private Peer[] peers = null;
	private PeerNAT reachableNAT = null;

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
			if (peer.equals(reachable)) {
				reachableNAT = new PeerBuilderNAT(peer).start();
			} else {
				new PeerBuilderNAT(peer).start();
			}
		}

		// Test setting up relay peers
		unreachable = new PeerBuilder(Number160.createHash(RND.nextInt())).ports(PORTS + 1).start();
		PeerAddress pa = unreachable.peerBean().serverPeerAddress();
		pa = pa.withUnreachable(true);
		unreachable.peerBean().serverPeerAddress(pa);
		
		// find neighbors
		FutureBootstrap futureBootstrap = unreachable.bootstrap().peerAddress(peers[0].peerAddress()).start();
		futureBootstrap.awaitUninterruptibly();
		Assert.assertTrue(futureBootstrap.isSuccess());
		
		// setup relay
		PeerNAT uNat = new PeerBuilderNAT(unreachable).start();
		uNat.startRelay(master.peerAddress());
		Thread.sleep(5000);

		// Check if flags are set correctly
		Assert.assertTrue(unreachable.peerAddress().relays().size() > 0);

		System.err.println("master = " + master.peerAddress());
		System.err.println("reachable = " + reachable.peerAddress());
		System.err.println("unreachable = " + unreachable.peerAddress());
	}

	@Test
	public void testPermanentReverseConnection() throws Exception {
		System.err.println("testPermanentReverseConnection() start!");

		final String requestString = "This is a test String";
		final String replyString = "SUCCESS HIT";
		final CountDownLatch cLatch = new CountDownLatch(3);

		unreachable.objectDataReply(new ObjectDataReply() {
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				if (requestString.equals((String) request)) {
					System.err.println("received: " + (String) request);
					cLatch.countDown();
				}
				return replyString;
			}
		});

		FutureDone<PeerConnection> fd = reachableNAT.startSetupRcon(master.peerAddress(), unreachable.peerAddress());
		fd.addListener(new BaseFutureAdapter<FutureDone<PeerConnection>>() {

			@Override
			public void operationComplete(FutureDone<PeerConnection> future) throws Exception {
				if (future.isSuccess()) {
					System.err.println("received: " + future.object().toString());

					FutureDirect fd2 = reachable.sendDirect(future.object()).object(requestString).start();
					fd2.addListener(new BaseFutureAdapter<FutureDirect>() {

						@Override
						public void operationComplete(FutureDirect future) throws Exception {
							if (future.isSuccess()) {
								if (replyString.equals((String) future.object())) {
									System.err.println("received: " + (String) future.object());
									cLatch.countDown();
								}
							}
						}
					});
					cLatch.countDown();
				}
			}
		});
		fd.awaitUninterruptibly();

		checkFail(cLatch);

		System.err.println("testPermanentReverseConnection() end!");
	}

	@Test
	public void testReverseConnection() throws Exception {
		System.err.println("testReverseConnection() start!");

		final String requestString = "This is a test String";
		final String replyString = "SUCCESS HIT";
		final CountDownLatch cLatch = new CountDownLatch(1);

		unreachable.objectDataReply(new ObjectDataReply() {

			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				if (requestString.equals((String) request)) {
					System.err.println("received: " + (String) request);
					cLatch.countDown();
				}
				return replyString;
			}
		});

		FutureDirect fd = reachable.sendDirect(unreachable.peerAddress()).object(requestString).start();
		fd.awaitUninterruptibly();

		checkFail(cLatch);

		System.err.println("testReverseConnection() end!");
	}

	private void checkFail(final CountDownLatch cLatch) throws InterruptedException {
		if (!cLatch.await(10, TimeUnit.SECONDS)) {
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
		BaseFuture bf = unreachable.shutdown();
		bf.awaitUninterruptibly();
		System.err.println("shutdown done!");
	}
}
