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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRcon {

	private static final Logger LOG = LoggerFactory.getLogger(TestRcon.class);

	private Peer reachable = null;
	private Peer unreachable = null;
	private Peer master = null;
	private Peer[] peers = new Peer[2];
	private PeerNAT reachableNAT = null;

	private static final int PORTS = 4001;
	private static final Number160 REACHABLE_ID = Number160.createHash("reachable");
	private static final Number160 UNREACHABLE_ID = Number160.createHash("unreachable");
	private static final Number160 RELAY_ID = Number160.createHash("relay");
	private static final int NUMBER_OF_NODES = 5;
	private static final Random RND = new Random();

	@Before
	public void setupRelay() throws Exception {
		// setup test peers
		peers = UtilsNAT.createNodes(NUMBER_OF_NODES, RND, PORTS);
		master = peers[0];
		reachable = peers[4];
		UtilsNAT.perfectRouting(peers);
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

	}
	
	@Test
	public void testPermanentReverseConnection() throws Exception {

//		setupRelay();

		final String requestString = "This is a test String";

		for (Peer ele : peers) {
			ele.objectDataReply(new ObjectDataReply() {

				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					Assert.assertEquals(requestString, ((String) request));
					return "SUCCESS HIT";
				}
			});
		}

		FutureDone<PeerConnection> fd = reachableNAT
				.startSetupRcon(master.peerAddress(), unreachable.peerAddress(), 60);
		fd.addListener(new BaseFutureAdapter<FutureDone<PeerConnection>>() {

			@Override
			public void operationComplete(FutureDone<PeerConnection> future) throws Exception {
				Assert.assertTrue(future.isSuccess());
				FutureDirect fd2 = reachable.sendDirect(future.object()).object(requestString).start();
				fd2.addListener(new BaseFutureAdapter<FutureDirect>() {

					@Override
					public void operationComplete(FutureDirect future) throws Exception {
						Assert.assertTrue(future.isSuccess());
						Assert.assertEquals("SUCCESS HIT", (String) future.object());
					}
				});
				fd2.awaitUninterruptibly();
			}
		});

		fd.awaitUninterruptibly();
		if (fd.isFailed()) {
			Assert.fail(fd.failedReason());
		}
	}

	@Test
	public void testReverseConnection() throws Exception {
//		setupRelay();

		final String requestString = "This is a test String";

		for (Peer ele : peers) {
			ele.objectDataReply(new ObjectDataReply() {

				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					Assert.assertEquals(requestString, ((String) request));
					return "SUCCESS HIT";
				}
			});
		}

		PeerAddress recipientAddress = master.peerAddress().changePeerId(UNREACHABLE_ID).changeFirewalledTCP(true)
				.changeFirewalledUDP(true).changeRelayed(true);
		FutureDirect fd = reachable.sendDirect(recipientAddress).object(requestString).start();
		fd.awaitUninterruptibly();

		if (fd.isSuccess()) {
			Assert.assertEquals("SUCCESS HIT", (String) fd.object());
		} else {
			Assert.fail(fd.failedReason());
		}

		Thread.sleep(2000);
	}

	@After
	public void shutdown() {
		for (Peer ele : peers) {
			BaseFuture bf = ele.shutdown();
			bf.awaitUninterruptibly();
		}
		BaseFuture bf = unreachable.shutdown();
		bf.awaitUninterruptibly();
	}
}
