package net.tomp2p.task;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;
import net.tomp2p.Utils2;
import net.tomp2p.futures.FutureTask;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.junit.Test;

public class TestTaskDHT {
    final private static Random rnd = new Random(42L);

    @Test
    public void testTaskSubmit1() throws Exception {
        Peer master = null;
        try {
            // setup
            Peer[] peers = Utils2.createNodes(200, rnd, 4001);
            master = peers[0];
            Utils2.perfectRouting(peers);
            // do testing
            Number160 locationKey = new Number160(rnd);
            FutureTask ft = peers[12].submit(locationKey, new Worker2())
                    .setRequestP2PConfiguration(new RequestP2PConfiguration(1, 0, 0)).start();
            ft.awaitUninterruptibly();
            Assert.assertEquals(true, ft.isSuccess());
            Assert.assertEquals(1, ft.getRawDataMap().size());
        } finally {
            System.out.println("done");
            master.halt();
        }
    }

    @Test
    public void testTaskSubmit2() throws Exception {
        Peer master = null;
        try {
            // setup
            Peer[] peers = Utils2.createNodes(200, rnd, 4001);
            master = peers[0];
            Utils2.perfectRouting(peers);
            // do testing
            Number160 locationKey = new Number160(rnd);
            FutureTask ft = peers[12].submit(locationKey, new Worker2())
                    .setRequestP2PConfiguration(new RequestP2PConfiguration(2, 0, 0)).start();
            ft.awaitUninterruptibly();
            Assert.assertEquals(true, ft.isSuccess());
            Assert.assertEquals(2, ft.getRawDataMap().size());
        } finally {
            System.out.println("done");
            master.halt();
        }
    }
}

class Worker2 implements Worker {
    private static final long serialVersionUID = 106846602205331838L;

    @Override
    public Map<Number160, Data> execute(Peer peer, Number160 taskId, Map<Number160, Data> inputData) throws Exception {
        System.out.println("executed");
        Map<Number160, Data> retVal = new HashMap<Number160, Data>();
        retVal.put(Number160.ONE, new Data(1));
        return retVal;
    }
};
