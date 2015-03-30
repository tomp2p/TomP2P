package net.tomp2p.holep;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.tcp.TCPRelayClientConfig;

import org.junit.Assert;
import org.junit.Test;

public class IntegrationTestHolePWithPeerNatBootstrapbuilder extends AbstractTestHoleP {

	@Test
	public void test() throws ClassNotFoundException, IOException {
		doTest();
	}

	@Override
	public Peer setUpRelayingWithNewPeer() throws IOException {
		// Test setting up relay peers
		Peer unreachable = new PeerBuilder(Number160.createHash(RND.nextInt())).ports(PORTS + 1).start();
		PeerAddress pa = unreachable.peerBean().serverPeerAddress();
		pa = pa.changeFirewalledTCP(true).changeFirewalledUDP(true);
		unreachable.peerBean().serverPeerAddress(pa);

		// find neighbors via bootstrapbuilder
		Collection<PeerAddress> bootstrapPeerAddresses = new ArrayList<PeerAddress>();
		bootstrapPeerAddresses.add(master.peerAddress());
		BootstrapBuilder builder = new BootstrapBuilder(unreachable).bootstrapTo(bootstrapPeerAddresses).ports(PORTS);
		FutureBootstrap fBoot = builder.start();
		fBoot.awaitUninterruptibly();
		Assert.assertTrue(fBoot.isSuccess());

		// setup relay
		PeerNAT uNat = new PeerBuilderNAT(unreachable).start();
		FutureRelayNAT frn = uNat.startRelay(new TCPRelayClientConfig(), builder);
		frn.awaitUninterruptibly();
		Assert.assertTrue(frn.isSuccess());

		// Check if flags are set correctly
		Assert.assertTrue(unreachable.peerAddress().isRelayed());
		Assert.assertFalse(unreachable.peerAddress().isFirewalledTCP());
		Assert.assertFalse(unreachable.peerAddress().isFirewalledUDP());

		System.err.println("unreachable = " + unreachable.peerAddress());
		return unreachable;
	}

}
