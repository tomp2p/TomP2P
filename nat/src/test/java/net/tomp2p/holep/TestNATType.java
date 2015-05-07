package net.tomp2p.holep;

import java.io.IOException;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.holep.strategy.NonPreservingSequentialStrategy;
import net.tomp2p.holep.strategy.PortPreservingStrategy;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.RPC.Commands;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNATType {

	private Peer peer;
	private Message msg;
	private int numberOfHoles;
	private int idleUDPSeconds;

	@SuppressWarnings("static-access")
	@Before
	public void setUp() throws IOException {
		peer = new PeerBuilder(Number160.createHash("test")).start();
		msg = new Message().type(Type.OK).command(Commands.DIRECT_DATA.getNr()).sender(peer.peerAddress());
		numberOfHoles = 3;
		idleUDPSeconds = peer.connectionBean().DEFAULT_UDP_IDLE_MILLIS * 1000;
	}

	@Test
	public void testGetHolePuncher() {
		Assert.assertEquals(PortPreservingStrategy.class, NATType.UNKNOWN.holePuncher(peer, numberOfHoles, idleUDPSeconds, msg)
				.getClass());
		Assert.assertEquals(PortPreservingStrategy.class, NATType.NO_NAT.holePuncher(peer, numberOfHoles, idleUDPSeconds, msg)
				.getClass());
		Assert.assertEquals(PortPreservingStrategy.class,
				NATType.PORT_PRESERVING.holePuncher(peer, numberOfHoles, idleUDPSeconds, msg).getClass());
		Assert.assertEquals(NonPreservingSequentialStrategy.class,
				NATType.NON_PRESERVING_SEQUENTIAL.holePuncher(peer, numberOfHoles, idleUDPSeconds, msg).getClass());
		Assert.assertEquals(null, NATType.NON_PRESERVING_OTHER.holePuncher(peer, numberOfHoles, idleUDPSeconds, msg));
	}
	
	@After
	public void shutDown() {
		BaseFuture future = peer.shutdown();
		future.awaitUninterruptibly();
	}
}
