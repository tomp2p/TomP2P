package net.tomp2p.p2p;


import java.io.IOException;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.Utils2;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Pair;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestBroadcast {
	private final static Random RND = new Random(42);
	
	@Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };

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
        
        @Test
	public void test4Peers() throws IOException, InterruptedException {
            //create
            Pair<Peer, MyStructuredBroadcastHandler> pair1 = createPeer(1);
            Pair<Peer, MyStructuredBroadcastHandler> pair2 = createPeer(2);
            Pair<Peer, MyStructuredBroadcastHandler> pair3 = createPeer(3);
            Pair<Peer, MyStructuredBroadcastHandler> pair4 = createPeer(4);
            //bootstrap
            bootstrap(pair1.element0(), pair2.element0());
            bootstrap(pair1.element0(), pair3.element0());
            bootstrap(pair1.element0(), pair4.element0());
            //broadcast
            NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
            try {
                dataMap.put(Number640.ZERO, new Data("Test"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            pair1.element0().broadcast(Number160.createHash("whatever")).dataMap(dataMap).start();
            
            pair1.element1().await();
            pair2.element1().await();
            pair3.element1().await();
            pair4.element1().await();
        }
        
        private void bootstrap(Peer master, Peer peer) {
            BootstrapBuilder bootstrapBuilder = peer.bootstrap();
            bootstrapBuilder.peerAddress(master.peerAddress());
            bootstrapBuilder.start().awaitUninterruptibly();
        }
        
        private Pair<Peer, MyStructuredBroadcastHandler> createPeer(int id) throws IOException {
            Number160 peerId = Number160.createHash(id);
            PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId).peerNoVerification());
            MyStructuredBroadcastHandler m = new MyStructuredBroadcastHandler(id);
            Peer peer = new PeerBuilder(peerId).broadcastHandler(m).enableBroadcast(true).peerMap(peerMap).ports(2000+id).start();
            return new Pair<Peer, MyStructuredBroadcastHandler>(peer, m);
        }       
}

class MyStructuredBroadcastHandler extends StructuredBroadcastHandler {
    private final int id;
    private CountDownLatch received = new CountDownLatch(1);
    public MyStructuredBroadcastHandler(int id) {
        this.id=id;
    }

    @Override
    public StructuredBroadcastHandler receive(Message message) {
        System.out.println("Peer "+id+" got the message");
        received.countDown();
        return super.receive(message);
    }
    
    public void await() throws InterruptedException {
        received.await();
    }
    
}
