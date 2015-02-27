package net.tomp2p.holep;

import java.io.IOException;

import net.tomp2p.connection.HolePInitiator;
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
	private static final int NUMBER_OF_HOLES = HolePInitiator.NUMBER_OF_HOLES;
	private static final int IDLE_UDP_SECONDS = HolePInitiator.IDLE_UDP_SECONDS;

	@Before
	public void setUp() throws IOException {
		peer = new PeerBuilder(Number160.createHash("test")).start();
		msg = new Message().type(Type.OK).command(Commands.DIRECT_DATA.getNr()).sender(peer.peerAddress());
	}

	@Test
	public void testGetHolePuncher() {
		Assert.assertEquals(PortPreservingStrategy.class, NATType.UNKNOWN.getHolePuncher(peer, NUMBER_OF_HOLES, IDLE_UDP_SECONDS, msg)
				.getClass());
		Assert.assertEquals(PortPreservingStrategy.class, NATType.NO_NAT.getHolePuncher(peer, NUMBER_OF_HOLES, IDLE_UDP_SECONDS, msg)
				.getClass());
		Assert.assertEquals(PortPreservingStrategy.class,
				NATType.PORT_PRESERVING.getHolePuncher(peer, NUMBER_OF_HOLES, IDLE_UDP_SECONDS, msg).getClass());
		Assert.assertEquals(NonPreservingSequentialStrategy.class,
				NATType.NON_PRESERVING_SEQUENTIAL.getHolePuncher(peer, NUMBER_OF_HOLES, IDLE_UDP_SECONDS, msg).getClass());
		Assert.assertEquals(null, NATType.NON_PRESERVING_OTHER.getHolePuncher(peer, NUMBER_OF_HOLES, IDLE_UDP_SECONDS, msg));
	}
	
	@After
	public void shutDown() {
		BaseFuture future = peer.shutdown();
		future.awaitUninterruptibly();
	}
}
