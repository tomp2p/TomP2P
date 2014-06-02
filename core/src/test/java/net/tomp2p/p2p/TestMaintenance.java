package net.tomp2p.p2p;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.Utils2;
import net.tomp2p.futures.BaseFuture;

import org.junit.Assert;
import org.junit.Test;

public class TestMaintenance {
    @Test
    public void testMaintenance() throws Exception {
        final Random rnd = new Random(42L);
        Peer master = null;
        try {
            // setup
            AtomicInteger counter = new AtomicInteger(0);
            Peer[] peers = Utils2.createRealNodes(10, rnd, 4001, new Rep(counter));
            master = peers[0];
            //give the master one of the peers,
            master.peerBean().peerMap().peerFound(peers[1].peerAddress(), peers[2].peerAddress());
            // wait for 1 sec.
            
            master.peerBean().peerMap().peerFound(peers[1].peerAddress(), peers[2].peerAddress());
            Thread.sleep(3000);
            
            //both peers pinged each other
            Assert.assertEquals(2, counter.get());
        } finally {
            if (master != null) {
                master.shutdown().await();
            }
        }
        
        
    }
    
    private static class Rep implements AutomaticFuture {
        
        private final AtomicInteger counter;
        
        public Rep(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void futureCreated(BaseFuture future) {
            counter.incrementAndGet();
        }
    }
}
