package net.tomp2p.holep;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.UtilsNAT;
import net.tomp2p.relay.tcp.TCPRelayClientConfig;
import net.tomp2p.relay.tcp.TCPRelayServerConfig;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class AbstractTestHoleP {

	protected Peer master;
	protected Peer unreachable1;
	protected Peer unreachable2;

	protected Peer[] peers = null;
	protected static final Random RND = new Random();
	protected static final int PORTS = 4001;
	protected static final int NUMBER_OF_NODES = 5;
	protected static final int IDLE_UDP_SECONDS = 30;
	
	@Before
	public void setupRelay() throws Exception {
		// setup test peers
		peers = UtilsNAT.createNodes(NUMBER_OF_NODES, RND, PORTS);
		master = peers[0];
		UtilsNAT.perfectRouting(peers);
		for (int i = 0; i< peers.length; i++) {
			if (i == 0) {
				new PeerBuilderNAT(peers[i]).addRelayServerConfiguration(RelayType.OPENTCP, new TCPRelayServerConfig()).holePNumberOfHoles(8).start();
			} else {
				new PeerBuilderNAT(peers[i]).start();
			}
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
		FutureRelayNAT frn = uNat.startRelay(new TCPRelayClientConfig(), master.peerAddress());
		frn.awaitUninterruptibly();
		Assert.assertTrue(frn.isSuccess());

		// Check if flags are set correctly
		Assert.assertTrue(unreachable.peerAddress().isRelayed());
		Assert.assertFalse(unreachable.peerAddress().isFirewalledTCP());
		Assert.assertFalse(unreachable.peerAddress().isFirewalledUDP());

		System.err.println("unreachable = " + unreachable.peerAddress());
		return unreachable;
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
		System.err.println("shutdown unreachable1 done!");

		BaseFuture bf2 = unreachable2.shutdown();
		bf2.awaitUninterruptibly();
		System.err.println("shutdown unreachable 2 done!");
	}
}
