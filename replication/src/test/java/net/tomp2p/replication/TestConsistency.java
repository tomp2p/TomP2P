package net.tomp2p.replication;

import java.util.Random;

import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestConsistency {
	
	@Test
	public void testConsistency() throws Exception {
		Random rnd = new Random(42);
		PeerDHT peers [] = UtilsReplication.createNodes(11, rnd, 4000, null);
		UtilsReplication.perfectRouting(peers);
		
		RoutingConfiguration r = new RoutingConfiguration(6, 10, 2);
		RequestP2PConfiguration p = new RequestP2PConfiguration(6, 10, 0);
		FuturePut fp = peers[5].put(Number160.MAX_VALUE).routingConfiguration(r).requestP2PConfiguration(p).data(new Data("test")).start();
		fp.awaitUninterruptibly();
		//now it is stored on 6 peers: 0, 1 , 2, 4, 5, 8
		for(PeerDHT p1:peers) {
			System.err.println(p1.peer().peerAddress()+":"+p1.storageLayer().get());
		}
		checkIfPresent(peers, 0, 1 , 2, 4, 5, 8);
		//now 5 peers go offline: 1 , 2, 4, 5, 8
		shutdown(peers, 1 , 2, 4, 5, 8);
		//now it gets replicated to the remaining peers
		Thread.sleep(4000);
		for(PeerDHT p1:peers) {
			System.err.println(p1.peer().peerAddress()+":"+p1.storageLayer().get());
		}
		checkIfPresent(peers, 0, 3 , 6, 7, 9, 10);
	}
	
	private void checkIfPresent(PeerDHT peers [], int... nrs) {
		for(int nr:nrs) {
			Assert.assertFalse(peers[nr].storageLayer().get().isEmpty());
		}
	}
	
	private void shutdown(PeerDHT peers [], int... nrs) {
		for(int nr:nrs) {
			//peers[nr].peer().announceShutdown().start().awaitUninterruptibly();
			peers[nr].shutdown();
		}
	}
}
