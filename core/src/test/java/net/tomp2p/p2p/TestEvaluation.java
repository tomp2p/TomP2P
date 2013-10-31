package net.tomp2p.p2p;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import net.tomp2p.Utils2;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestEvaluation {
    @Test
    public void testEvaluationKeys() throws Exception {

        Map<PeerAddress, Map<Number480, Byte>> rawKeys = new HashMap<PeerAddress, Map<Number480, Byte>>();
        PeerAddress pa1 = Utils2.createAddress(19);
        Map<Number480, Byte> test1 = new HashMap<Number480, Byte>();
        test1.put(new Number480(new Number160(4), new Number160(5),new Number160(12)), (byte)0);
        test1.put(new Number480(new Number160(4), new Number160(5),new Number160(13)), (byte)0);
        test1.put(new Number480(new Number160(4), new Number160(5),new Number160(14)), (byte)0);
        rawKeys.put(pa1, test1);
        //
        PeerAddress pa2 = Utils2.createAddress(20);
        Map<Number480, Byte> test2 = new HashMap<Number480, Byte>();
        test2.put(new Number480(new Number160(4), new Number160(5),new Number160(12)), (byte)0);
        test2.put(new Number480(new Number160(4), new Number160(5),new Number160(13)), (byte)0);
        rawKeys.put(pa2, test2);
        //
        PeerAddress pa3 = Utils2.createAddress(21);
        Map<Number480, Byte> test3 = new HashMap<Number480, Byte>();
        test3.put(new Number480(new Number160(4), new Number160(5),new Number160(11)), (byte)0);
        test3.put(new Number480(new Number160(4), new Number160(5),new Number160(13)), (byte)0);
        test3.put(new Number480(new Number160(4), new Number160(5),new Number160(14)), (byte)0);
        rawKeys.put(pa3, test3);
        VotingSchemeDHT evs = new VotingSchemeDHT();
        
        Collection<Number480> tmp = evs.evaluate7(rawKeys);
        Assert.assertEquals(false, tmp.contains(new Number480(new Number160(4), new Number160(5), new Number160(11))));
        Assert.assertEquals(true, tmp.contains(new Number480(new Number160(4), new Number160(5), new Number160(12))));
        Assert.assertEquals(true, tmp.contains(new Number480(new Number160(4), new Number160(5), new Number160(13))));
        Assert.assertEquals(true, tmp.contains(new Number480(new Number160(4), new Number160(5), new Number160(14))));
    }

    @Test
    public void testEvaluationData1() throws Exception {
        byte[] me11 = new byte[] { 1, 1 };
        byte[] me12 = new byte[] { 2, 2 };
        byte[] me13 = new byte[] { 3, 3 };
        byte[] me14 = new byte[] { 4, 4 };
        Map<PeerAddress, Map<Number480, Data>> rawData = new HashMap<PeerAddress, Map<Number480, Data>>();
        PeerAddress pa1 = Utils2.createAddress(19);
        Map<Number480, Data> test1 = new HashMap<Number480, Data>();
        test1.put(new Number480(new Number160(12),new Number160(12),new Number160(12)), new Data(me12));
        test1.put(new Number480(new Number160(13),new Number160(13),new Number160(13)), new Data(me13));
        test1.put(new Number480(new Number160(14),new Number160(14),new Number160(14)), new Data(me14));
        rawData.put(pa1, test1);
        //
        PeerAddress pa2 = Utils2.createAddress(20);
        Map<Number480, Data> test2 = new HashMap<Number480, Data>();
        test2.put(new Number480(new Number160(12),new Number160(12),new Number160(12)), new Data(me12));
        test2.put(new Number480(new Number160(13),new Number160(13),new Number160(13)), new Data(me13));
        rawData.put(pa2, test2);
        //
        PeerAddress pa3 = Utils2.createAddress(21);
        Map<Number480, Data> test3 = new HashMap<Number480, Data>();
        test3.put(new Number480(new Number160(11),new Number160(11),new Number160(11)), new Data(me11));
        test3.put(new Number480(new Number160(13),new Number160(13),new Number160(13)), new Data(me13));
        test3.put(new Number480(new Number160(14),new Number160(14),new Number160(14)), new Data(me14));
        rawData.put(pa3, test3);
        VotingSchemeDHT evs = new VotingSchemeDHT();
        Map<Number480, Data> tmp = evs.evaluate2(rawData);
        Assert.assertEquals(false, tmp.containsKey(new Number480(new Number160(11),new Number160(11),new Number160(11))));
        Assert.assertEquals(true, tmp.containsKey(new Number480(new Number160(12),new Number160(12),new Number160(12))));
        Assert.assertEquals(true, tmp.containsKey(new Number480(new Number160(13),new Number160(13),new Number160(13))));
        Assert.assertEquals(true, tmp.containsKey(new Number480(new Number160(14),new Number160(14),new Number160(14))));
    }

    @Test
    public void testEvaluationData2() throws Exception {
        byte[] me11 = new byte[] { 1, 1 };
        byte[] me12 = new byte[] { 2, 2 };
        byte[] me13 = new byte[] { 3, 3 };
        byte[] me14 = new byte[] { 4, 4 };
        Map<PeerAddress, Map<Number480, Data>> rawData = new HashMap<PeerAddress, Map<Number480, Data>>();
        PeerAddress pa1 = Utils2.createAddress(19);
        Map<Number480, Data> test1 = new HashMap<Number480, Data>();
        test1.put(new Number480(new Number160(12),new Number160(12),new Number160(12)), new Data(me12));
        test1.put(new Number480(new Number160(13),new Number160(13),new Number160(13)), new Data(me13));
        test1.put(new Number480(new Number160(14),new Number160(14),new Number160(14)), new Data(me14));
        rawData.put(pa1, test1);
        //
        PeerAddress pa2 = Utils2.createAddress(20);
        Map<Number480, Data> test2 = new HashMap<Number480, Data>();
        test2.put(new Number480(new Number160(12),new Number160(12),new Number160(12)), new Data(me11));
        test2.put(new Number480(new Number160(13),new Number160(13),new Number160(13)), new Data(me13));
        rawData.put(pa2, test2);
        //
        PeerAddress pa3 = Utils2.createAddress(21);
        Map<Number480, Data> test3 = new HashMap<Number480, Data>();
        test3.put(new Number480(new Number160(11),new Number160(11),new Number160(11)), new Data(me11));
        test3.put(new Number480(new Number160(13),new Number160(13),new Number160(13)), new Data(me13));
        test3.put(new Number480(new Number160(14),new Number160(14),new Number160(14)), new Data(me14));
        rawData.put(pa3, test3);
        VotingSchemeDHT evs = new VotingSchemeDHT();
        Map<Number480, Data> tmp = evs.evaluate2(rawData);
        Assert.assertEquals(false, tmp.containsKey(new Number480(new Number160(11),new Number160(11),new Number160(11))));
        Assert.assertEquals(false, tmp.containsKey(new Number480(new Number160(12),new Number160(12),new Number160(12))));
        Assert.assertEquals(true, tmp.containsKey(new Number480(new Number160(13),new Number160(13),new Number160(13))));
        Assert.assertEquals(true, tmp.containsKey(new Number480(new Number160(14),new Number160(14),new Number160(14))));
    }
}
