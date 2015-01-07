package net.tomp2p.holep;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.RPC.Commands;

import org.junit.Before;
import org.junit.Test;

public class UnitTestHolePuncherTest {

	private static final int NUMBER_OF_HOLES = 5;
	private static final int IDLE_UDP_SECONDS = 30;
	private static final int PORT = 4000;
	private static final int INT_VALUE = 99;
	private static final Random RAND = new Random();
	private Peer peer1;
	private Peer peer2;
	private Message originalMessage;

	@Before
	public void setUp() throws IOException {
		peer1 = new PeerBuilder(Number160.createHash(RAND.nextInt())).ports(PORT).behindFirewall(true).start();
		peer2 = new PeerBuilder(Number160.createHash(RAND.nextInt())).ports(PORT).behindFirewall(true).start();
	}

	@Test
	public void test() {

		originalMessage = new Message().type(Type.REQUEST_2).command(Commands.DIRECT_DATA.getNr()).intValue(INT_VALUE)
				.sender(peer1.peerAddress()).recipient(peer2.peerAddress());

		HolePuncher holePuncher = new HolePuncher(peer1, NUMBER_OF_HOLES, IDLE_UDP_SECONDS, originalMessage);
	}

}
