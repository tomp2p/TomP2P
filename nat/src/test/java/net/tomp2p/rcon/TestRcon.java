package net.tomp2p.rcon;

import java.util.Random;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.FutureRelay;
import net.tomp2p.relay.UtilsNAT;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
		// every peer must own a PeerNAT in order to be able to be a relay and set up a reverse connection
		for (Peer peer : peers) {
			if (peer.equals(reachable)) {
				reachableNAT = new PeerNAT(peer);
			} else {
				new PeerNAT(peer);
			}
		}

		// Test setting up relay peers
		unreachable = new PeerBuilder(Number160.createHash(RND.nextInt())).ports(PORTS + 1).start();
		PeerAddress pa = unreachable.peerBean().serverPeerAddress();
		pa = pa.changeFirewalledTCP(true).changeFirewalledUDP(true);
		unreachable.peerBean().serverPeerAddress(pa);
		// find neighbors
		FutureBootstrap futureBootstrap = unreachable.bootstrap().peerAddress(peers[0].peerAddress()).start();
		futureBootstrap.awaitUninterruptibly();
		Assert.assertTrue(futureBootstrap.isSuccess());
		// setup relay
		PeerNAT uNat = new PeerNAT(unreachable);
		FutureRelay fr = uNat.startSetupRelay();
		fr.awaitUninterruptibly();
		Assert.assertTrue(fr.isSuccess());
		// Assert.assertEquals(2, fr.relays().size());

		// Check if flags are set correctly
		Assert.assertTrue(unreachable.peerAddress().isRelayed());
		Assert.assertFalse(unreachable.peerAddress().isFirewalledTCP());
		Assert.assertFalse(unreachable.peerAddress().isFirewalledUDP());

		System.err.println("master = " + master.peerAddress());
		System.err.println("reachable = " + reachable.peerAddress());
		System.err.println("unreachable = " + unreachable.peerAddress());
	}

	@Test
	public void testPermanentReverseConnection() throws Exception {
		System.err.println("testPermanentReverseConnection() start!");
		
		final String requestString = "This is a test String";
		final String replyString = "SUCCESS HIT";
		final int testTimeout = 60;

		unreachable.objectDataReply(new ObjectDataReply() {
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				Assert.assertEquals(requestString, ((String) request));
				System.err.println("received: " + (String) request);
				return replyString;
			}
		});

		FutureDone<PeerConnection> fd = reachableNAT.startSetupRcon(master.peerAddress(), unreachable.peerAddress(),
				testTimeout);
		fd.addListener(new BaseFutureAdapter<FutureDone<PeerConnection>>() {
			@Override
			public void operationComplete(FutureDone<PeerConnection> future) throws Exception {
				Assert.assertTrue(future.isSuccess());
				Assert.assertEquals(PeerConnection.class, future.object().getClass());
				Assert.assertEquals(testTimeout, future.object().timeout());
				FutureDirect fd2 = reachable.sendDirect(future.object()).object(requestString).start();
				fd2.addListener(new BaseFutureAdapter<FutureDirect>() {
					@Override
					public void operationComplete(FutureDirect future) throws Exception {
						Assert.assertTrue(future.isSuccess());
						Assert.assertEquals(replyString, (String) future.object());
					}
				});
			}
		});

		fd.awaitUninterruptibly();
		if (fd.isFailed()) {
			Assert.fail(fd.failedReason());
		}
		
		System.err.println("testPermanentReverseConnection() end!");
	}

	@Test
	public void testReverseConnection() throws Exception {
		System.err.println("testReverseConnection() start!");
		
		final String requestString = "This is a test String";
		final String replyString = "SUCCESS HIT";

		unreachable.objectDataReply(new ObjectDataReply() {
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				Assert.assertEquals(requestString, ((String) request));
				System.err.println("received: " + (String) request);
				return replyString;
			}
		});

		FutureDirect fd = reachable.sendDirect(unreachable.peerAddress()).object(requestString).start();
		fd.addListener(new BaseFutureAdapter<FutureDirect>() {

			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				Assert.assertTrue(future.isSuccess());
			}
		});
		fd.awaitUninterruptibly();
		
		System.err.println("testReverseConnection() end!");
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
