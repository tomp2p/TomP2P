package net.tomp2p.holep;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayConfig;
import net.tomp2p.relay.UtilsNAT;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHolePuncher {
	
	private Peer master;
	private Peer unreachable1;
	private Peer unreachable2;
	
	private Peer[] peers = null;
	private static final Random RND = new Random();
	private static final int PORTS = 4001;
	private static final int NUMBER_OF_NODES = 5;
	
	@Before
	public void setupRelay() throws Exception {
		// setup test peers
		peers = UtilsNAT.createNodes(NUMBER_OF_NODES, RND, PORTS);
		master = peers[0];
		UtilsNAT.perfectRouting(peers);
		// every peer must own a PeerNAT in order to be able to be a relay and
		// set up a reverse connection
		for (Peer peer : peers) {
			new PeerBuilderNAT(peer).start();
		}

		unreachable1 = setUpRelayingWithNewPeer();
		unreachable2 = setUpRelayingWithNewPeer();
	}

	public Peer setUpRelayingWithNewPeer() throws IOException {
		// Test setting up relay peers
		Peer unreachable = new PeerBuilder(Number160.createHash(RND.nextInt())).ports(PORTS + 1).start();
		PeerAddress pa = unreachable.peerBean().serverPeerAddress();
		pa = pa.changeFirewalledTCP(true).changeFirewalledUDP(true);
		unreachable.peerBean().serverPeerAddress(pa);
		
		// find neighbors
		FutureBootstrap futureBootstrap = unreachable.bootstrap().peerAddress(peers[0].peerAddress()).start();
		futureBootstrap.awaitUninterruptibly();
		Assert.assertTrue(futureBootstrap.isSuccess());
		
		// setup relay
		PeerNAT uNat = new PeerBuilderNAT(unreachable).start();
		FutureRelayNAT frn = uNat.startRelay(RelayConfig.OpenTCP(), master.peerAddress());
		frn.awaitUninterruptibly();
		Assert.assertTrue(frn.isSuccess());

		// Check if flags are set correctly
		Assert.assertTrue(unreachable.peerAddress().isRelayed());
		Assert.assertFalse(unreachable.peerAddress().isFirewalledTCP());
		Assert.assertFalse(unreachable.peerAddress().isFirewalledUDP());
		
		System.err.println("unreachable = " + unreachable.peerAddress());
		return unreachable;
	}
	
	@Test
	public void testHolePunch() {
		System.err.println("testHolePunch() start!");

		final String requestString = "This is a test String";
		final String replyString = "SUCCESS HIT";
		
		unreachable2.objectDataReply(new ObjectDataReply() {
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				if (requestString.equals((String) request)) {
					Assert.assertEquals(requestString, request);
					System.err.println("received: " + (String) request);
				}
				return replyString;
			}
		});
		
		FutureDirect fd = unreachable1.sendDirect(unreachable2.peerAddress()).object(requestString).forceUDP(true).start();
		fd.addListener(new BaseFutureAdapter<FutureDirect>() {

			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				Assert.assertTrue(future.isSuccess());
				Assert.assertEquals(replyString, (String) future.object());
			}
		});
		fd.awaitUninterruptibly();
		
		shutdown();
	}
	
	@After
	public void shutdown() {
		System.err.println("shutdown initiated!");
		for (Peer ele : peers) {
			BaseFuture bf = ele.shutdown();
			bf.awaitUninterruptibly();
		}
		BaseFuture bf1 = unreachable1.shutdown();
		bf1.awaitUninterruptibly();
		System.err.println("shutdown done!");
		
		BaseFuture bf2 = unreachable2.shutdown();
		bf2.awaitUninterruptibly();
		System.err.println("shutdown done!");
	}
}
