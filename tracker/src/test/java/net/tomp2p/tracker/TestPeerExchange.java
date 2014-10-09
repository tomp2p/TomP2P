package net.tomp2p.tracker;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePing;
import net.tomp2p.message.TrackerData;
import net.tomp2p.p2p.AutomaticFuture;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestPeerExchange {
    @Test
    public void testPex() throws Exception {
        PeerTracker sender = null;
        PeerTracker recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderTracker(new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start()).peerExchangeRefreshSec(1).start();
 
            Peer recv1Peer = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            
            
            final CountDownLatch c = new CountDownLatch(2);
            TrackerStorage trackerStorage = new TrackerStorage(60, new int[]{1,2}, 20, recv1Peer.peerBean().peerMap(), recv1.peerAddress(), false);
            Random rnd = new Random(42);
            PeerBuilderTracker.DefaultPeerExchangeHandler pe = new PeerBuilderTracker.DefaultPeerExchangeHandler(trackerStorage, recv1Peer.peerAddress(), rnd) {
            	@Override
            	public boolean put(Number320 key, TrackerData trackerData, PeerAddress referrer) {
            	    boolean retVal = super.put(key, trackerData, referrer);
            	    c.countDown();
            	    return retVal;
            	}
            };
            
            recv1 = new PeerBuilderTracker(recv1Peer).peerExchangeHandler(pe).peerExchangeRefreshSec(1).start();
            
            Number160 locationKey = new Number160("0x5555");
            Number160 domainKey = new Number160("0x7777");
            Number320 key = new Number320(locationKey, domainKey);
            
            sender.trackerStorage().put(key, recv1.peerAddress(), null, new Data("test"));
            PeerStatatistic ps = sender.trackerStorage().nextForMaintenance(new ArrayList<PeerAddress>());
            FuturePing fp = sender.peer().ping().peerAddress(ps.peerAddress()).start().awaitListeners();
            Assert.assertEquals(true, fp.isSuccess());
            
            
            sender.peer().addAutomaticFuture(new AutomaticFuture() {
				@Override
				public void futureCreated(BaseFuture future) {
					if(future instanceof FutureDone) {
						future.addListener(new BaseFutureAdapter<BaseFuture>() {
							@Override
                            public void operationComplete(BaseFuture future) throws Exception {
	                            if(future.isSuccess()) {
	                            	c.countDown();
	                            }
                            }
						});
					}
				}
			});
            
            c.await();
            Assert.assertEquals(1, recv1.trackerStorage().sizeUnverified() + recv1.trackerStorage().size());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.peer().shutdown().await();
            }
            if (recv1 != null) {
                recv1.peer().shutdown().await();
            }
        }
    }
}
