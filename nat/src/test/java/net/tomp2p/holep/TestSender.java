package net.tomp2p.holep;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.connection.Sender;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

import org.junit.Before;
import org.junit.Test;

public class TestSender {

	private final Random RND = new Random(42L);
	private Peer peer;
	
	
	@Before
	public void setUp() throws IOException {
		peer = new PeerBuilder(Number160.createHash(RND.nextInt())).behindFirewall(true).start();
	}
	
	@Test
	public void test() {
		//TODO insert Mockito
		Sender sender = peer.connectionBean().sender();
//		sender.sendUDP(handler, futureResponse, message, channelCreator, idleUDPSeconds, broadcast);
	}

}
