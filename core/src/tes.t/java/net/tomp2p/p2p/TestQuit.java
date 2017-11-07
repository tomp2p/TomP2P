package net.tomp2p.p2p;

import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.builder.ShutdownBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestQuit {
	
	@Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };
	
    @Test
    public void testGracefulhalt() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            sender.bootstrap().peerAddress(recv1.peerAddress()).start().awaitUninterruptibly();
            Assert.assertEquals(1, sender.peerBean().peerMap().all().size());
            Assert.assertEquals(1, recv1.peerBean().peerMap().allOverflow().size());
            // graceful shutdown
            
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            
            ShutdownBuilder builder = new ShutdownBuilder(sender);
            
            sender.quitRPC().quit(recv1.peerAddress(), builder, cc);
            sender.shutdown().await();
            // don't care about the sender
            Assert.assertEquals(0, recv1.peerBean().peerMap().all().size());

        } finally {
            if (cc != null) {
                cc.shutdown().await();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }
    
    /**
	 * Test the quit messages if they set a peer as offline.
	 * 
	 * @throws Exception .
	 */
	@Test
	public void testQuit() throws Exception {
		Peer master = null;
		try {
			// setup
			final int nrPeers = 200;
			final int port = 4001;
			Random rnd =new Random(42);
			Peer[] peers = Utils2.createNodes(nrPeers, rnd, port);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			final int peerTest = 10;
			System.err.println("counter: " + countOnline(peers, peers[peerTest].peerAddress()));
			FutureDone<Void> futureShutdown = peers[peerTest].announceShutdown().start();
			futureShutdown.awaitUninterruptibly();
			// we need to wait a bit, since the quit RPC is a fire and forget
			// and we return immediately
			Thread.sleep(2000);
			int counter = countOnline(peers, peers[peerTest].peerAddress());
			System.err.println("counter: " + counter);
			Assert.assertEquals(180, nrPeers - 20);
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	private static int countOnline(Peer[] peers, PeerAddress peerAddress) {
		int counter = 0;
		for (Peer peer : peers) {
			if (peer.peerBean().peerMap().contains(peerAddress)) {
				counter++;
			}
		}
		return counter;
	}
}
