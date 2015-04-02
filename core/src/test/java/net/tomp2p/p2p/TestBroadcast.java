package net.tomp2p.p2p;


import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Test;

public class TestBroadcast {
	private final static Random RND = new Random(42);

	@Test
	public void testBroadcast() throws Exception {
		
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(1000, RND, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			master.broadcast(Number160.createHash("blub")).udp(false).start();
			DefaultBroadcastHandler d = (DefaultBroadcastHandler) master.broadcastRPC().broadcastHandler();
			int counter = 0;
			while (d.getBroadcastCounter() < 400) {
				Thread.sleep(200);
				counter++;
				if (counter > 100) {
					
					System.out.println("did not broadcast to 1000 peers, but to " + d.getBroadcastCounter());
					Assert.fail("did not broadcast to 1000 peers, but to " + d.getBroadcastCounter());
				}
			}
			System.out.println("DONE: "+d.getBroadcastCounter());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	@Test
	public void testStructuredBroadcast() throws Exception {
		
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(1000, RND, 4001, true);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			master.broadcast(Number160.createHash("blub")).udp(false).start();
			DefaultBroadcastHandler d = (DefaultBroadcastHandler) master.broadcastRPC().broadcastHandler();
			int counter = 0;
			//TODO: make this work
			while (d.getBroadcastCounter() < 1000) {
				Thread.sleep(200);
				counter++;
				if (counter > 100) {
					
					System.out.println("did not broadcast to 1000 peers, but to " + d.getBroadcastCounter());
					Assert.fail("did not broadcast to 1000 peers, but to " + d.getBroadcastCounter());
				}
			}
			System.out.println("DONE: "+d.getBroadcastCounter());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
}
