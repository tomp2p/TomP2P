package net.tomp2p.p2p;


import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Test;

public class TestBroadcast {
	private final static Random RND = new Random(42);

	@Test
	public void testBroadcastTCP() throws Exception {
		
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(1000, RND, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			master.broadcast(Number160.createHash("blub")).udp(false).start();
			StructuredBroadcastHandler d = (StructuredBroadcastHandler) master.broadcastRPC().broadcastHandler();
			int counter = 0;
			while (d.broadcastCounter() < 1000) {
				Thread.sleep(200);
				counter++;
				if (counter > 100) {
					System.out.println("did not broadcast to 1000 peers, but to " + d.broadcastCounter());
					Assert.fail("did not broadcast to 1000 peers, but to " + d.broadcastCounter());
				}
			}
			System.out.println("msg count: "+d.messageCounter());
			System.out.println("DONE: "+d.broadcastCounter());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	@Test
	public void testBroadcastUDP() throws Exception {
		
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(1000, RND, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			master.broadcast(Number160.createHash("blub")).start();
			StructuredBroadcastHandler d = (StructuredBroadcastHandler) master.broadcastRPC().broadcastHandler();
			int counter = 0;
			while (d.broadcastCounter() < 1000) {
				Thread.sleep(200);
				counter++;
				if (counter > 100) {
					System.out.println("did not broadcast to 1000 peers, but to " + d.broadcastCounter());
					Assert.fail("did not broadcast to 1000 peers, but to " + d.broadcastCounter());
				}
			}
			System.out.println("msg count: "+d.messageCounter());
			System.out.println("DONE: "+d.broadcastCounter());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
}
