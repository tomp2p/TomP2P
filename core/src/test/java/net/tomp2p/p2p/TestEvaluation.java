package net.tomp2p.p2p;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.Utils2;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestEvaluation {

    @Test
    public void testEvaluationData1() throws Exception {
        byte[] me11 = new byte[] { 1, 1 };
        byte[] me12 = new byte[] { 2, 2 };
        byte[] me13 = new byte[] { 3, 3 };
        byte[] me14 = new byte[] { 4, 4 };
        Map<PeerAddress, Map<Number640, Data>> rawData = new HashMap<PeerAddress, Map<Number640, Data>>();
        PeerAddress pa1 = Utils2.createAddress(19);
        Map<Number640, Data> test1 = new HashMap<Number640, Data>();
        test1.put(new Number640(new Number160(12),new Number160(12),new Number160(12),new Number160(0)), new Data(me12));
        test1.put(new Number640(new Number160(13),new Number160(13),new Number160(13),new Number160(0)), new Data(me13));
        test1.put(new Number640(new Number160(14),new Number160(14),new Number160(14),new Number160(0)), new Data(me14));
        rawData.put(pa1, test1);
        //
        PeerAddress pa2 = Utils2.createAddress(20);
        Map<Number640, Data> test2 = new HashMap<Number640, Data>();
        test2.put(new Number640(new Number160(12),new Number160(12),new Number160(12),new Number160(0)), new Data(me12));
        test2.put(new Number640(new Number160(13),new Number160(13),new Number160(13),new Number160(0)), new Data(me13));
        rawData.put(pa2, test2);
        //
        PeerAddress pa3 = Utils2.createAddress(21);
        Map<Number640, Data> test3 = new HashMap<Number640, Data>();
        test3.put(new Number640(new Number160(11),new Number160(11),new Number160(11),new Number160(0)), new Data(me11));
        test3.put(new Number640(new Number160(13),new Number160(13),new Number160(13),new Number160(0)), new Data(me13));
        test3.put(new Number640(new Number160(14),new Number160(14),new Number160(14),new Number160(0)), new Data(me14));
        rawData.put(pa3, test3);
        VotingSchemeDHT evs = new VotingSchemeDHT();
        Map<Number640, Data> tmp = evs.evaluate2(rawData);
        Assert.assertEquals(false, tmp.containsKey(new Number640(new Number160(11),new Number160(11),new Number160(11),new Number160(0))));
        Assert.assertEquals(true, tmp.containsKey(new Number640(new Number160(12),new Number160(12),new Number160(12),new Number160(0))));
        Assert.assertEquals(true, tmp.containsKey(new Number640(new Number160(13),new Number160(13),new Number160(13),new Number160(0))));
        Assert.assertEquals(true, tmp.containsKey(new Number640(new Number160(14),new Number160(14),new Number160(14),new Number160(0))));
    }

    @Test
    public void testEvaluationData2() throws Exception {
        byte[] me11 = new byte[] { 1, 1 };
        byte[] me12 = new byte[] { 2, 2 };
        byte[] me13 = new byte[] { 3, 3 };
        byte[] me14 = new byte[] { 4, 4 };
        Map<PeerAddress, Map<Number640, Data>> rawData = new HashMap<PeerAddress, Map<Number640, Data>>();
        PeerAddress pa1 = Utils2.createAddress(19);
        Map<Number640, Data> test1 = new HashMap<Number640, Data>();
        test1.put(new Number640(new Number160(12),new Number160(12),new Number160(12),new Number160(0)), new Data(me12));
        test1.put(new Number640(new Number160(13),new Number160(13),new Number160(13),new Number160(0)), new Data(me13));
        test1.put(new Number640(new Number160(14),new Number160(14),new Number160(14),new Number160(0)), new Data(me14));
        rawData.put(pa1, test1);
        //
        PeerAddress pa2 = Utils2.createAddress(20);
        Map<Number640, Data> test2 = new HashMap<Number640, Data>();
        test2.put(new Number640(new Number160(12),new Number160(12),new Number160(12),new Number160(0)), new Data(me11));
        test2.put(new Number640(new Number160(13),new Number160(13),new Number160(13),new Number160(0)), new Data(me13));
        rawData.put(pa2, test2);
        //
        PeerAddress pa3 = Utils2.createAddress(21);
        Map<Number640, Data> test3 = new HashMap<Number640, Data>();
        test3.put(new Number640(new Number160(11),new Number160(11),new Number160(11),new Number160(0)), new Data(me11));
        test3.put(new Number640(new Number160(13),new Number160(13),new Number160(13),new Number160(0)), new Data(me13));
        test3.put(new Number640(new Number160(14),new Number160(14),new Number160(14),new Number160(0)), new Data(me14));
        
        rawData.put(pa3, test3);
        VotingSchemeDHT evs = new VotingSchemeDHT();
        Map<Number640, Data> tmp = evs.evaluate2(rawData);
        
        Assert.assertEquals(false, tmp.containsKey(new Number640(new Number160(11),new Number160(11),new Number160(11),new Number160(0))));
        Assert.assertEquals(false, tmp.containsKey(new Number640(new Number160(12),new Number160(12),new Number160(12),new Number160(0))));
        Assert.assertEquals(true, tmp.containsKey(new Number640(new Number160(13),new Number160(13),new Number160(13),new Number160(0))));
        Assert.assertEquals(true, tmp.containsKey(new Number640(new Number160(14),new Number160(14),new Number160(14),new Number160(0))));
    }
}
