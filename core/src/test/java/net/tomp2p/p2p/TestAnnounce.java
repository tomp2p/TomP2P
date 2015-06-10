package net.tomp2p.p2p;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.connection.DiscoverResults;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureAnnounce;
import net.tomp2p.futures.FuturePing;
import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestAnnounce {
	
	@Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };
	
	/**
	 * Start with -Djava.net.preferIPv4Stack=true
	 * @throws Exception
	 */
	@Test
    public void testLocalAnnounce() throws Exception {
        final Random rnd = new Random(42);
        Peer master = null;
        try {
            // setup
            Peer[] peers = Utils2.createNonMaintenanceNodes(100, rnd, 4001);
            master = peers[0];
            if(!hasBroadcastAddress(master)) {
            	return;
            }
            // do testing
            List<FutureAnnounce> tmp = new ArrayList<FutureAnnounce>();
            // we start from 1, because a broadcast to ourself will not get
            // replied.
            for (int i = 1; i < peers.length; i++) {
                FutureAnnounce res = peers[i].localAnnounce().port(4001).start();
                tmp.add(res);
            }
            for(FutureAnnounce f:tmp) {
            	f.awaitUninterruptibly();
            }
            //check the local map
            for(int i=0;i<peers.length;i++) {
            	Peer peer = peers[i];
            	int size = peer.peerBean().localMap().size();
            	if(i == 0) {
            		int counter = 0;
            		while(size < 99 && counter < 20) {
            			Thread.sleep(100);
            			size = peer.peerBean().localMap().size();
            			counter ++;
            		}
            		Assert.assertEquals(99, size);
            	} else {
            		Assert.assertEquals(0, size);
            	}
            }
        } finally {
            if (master != null) {
                master.shutdown().await();
            }
        }
    }

	/**
	 * Start with -Djava.net.preferIPv4Stack=true
	 * @throws Exception
	 */
    @Test
    public void testLocalAnnounceTranslate() throws Exception {
        final Random rnd = new Random(42);
        Peer master = null;
        Peer slave = null;
        try {
            master = new PeerBuilder(new Number160(rnd)).ports(4001).start();
            slave = new PeerBuilder(new Number160(rnd)).ports(4002).start();
            if(!hasBroadcastAddress(slave)) {
            	return;
            }
            BaseFuture res = slave.localAnnounce().port(4001).start();
            res.awaitUninterruptibly();
            System.err.println(res.failedReason());
            Assert.assertEquals(true, res.isSuccess());
            
            int size = master.peerBean().localMap().size();
            int counter = 0;
    		while(size < 1 && counter < 20) {
    			Thread.sleep(100);
    			size = master.peerBean().localMap().size();
    			counter ++;
    		}
    		Assert.assertEquals(1, size);
    		//1.2.3.4 gets rewritten
    		FuturePing fp = master.ping().peerAddress(slave.peerAddress().changeAddress(InetAddress.getByName("1.2.3.4"))).start();
    		fp.awaitUninterruptibly();
    		Assert.assertEquals(true, fp.isSuccess());
    		Assert.assertEquals(InetAddress.getByName("1.2.3.4"), fp.remotePeer().inetAddress());
        } finally {
            if (master != null) {
                master.shutdown().await();
            }
            if (slave != null) {
                slave.shutdown().await();
            }
        }
    }
    
    private boolean hasBroadcastAddress(Peer peer) {
    	final DiscoverResults discoverResults = peer.connectionBean().channelServer().discoverNetworks().currentDiscoverResults();
        final Collection<InetAddress> broadcastAddresses = discoverResults.existingBroadcastAddresses();
        return broadcastAddresses.size() > 0;
    }
}
