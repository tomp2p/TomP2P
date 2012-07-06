package net.tomp2p.p2p;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import junit.framework.Assert;

import net.tomp2p.Utils2;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.junit.Test;

public class TestEvaluation
{
    @Test
    public void testEvaluationKeys()
        throws Exception
    {

        Map<PeerAddress, Collection<Number160>> rawKeys = new HashMap<PeerAddress, Collection<Number160>>();
        PeerAddress pa1 = Utils2.createAddress( 19 );
        Collection<Number160> test1 = new HashSet<Number160>();
        test1.add( new Number160( 12 ) );
        test1.add( new Number160( 13 ) );
        test1.add( new Number160( 14 ) );
        rawKeys.put( pa1, test1 );
        //
        PeerAddress pa2 = Utils2.createAddress( 20 );
        Collection<Number160> test2 = new HashSet<Number160>();
        test2.add( new Number160( 12 ) );
        test2.add( new Number160( 13 ) );
        rawKeys.put( pa2, test2 );
        //
        PeerAddress pa3 = Utils2.createAddress( 21 );
        Collection<Number160> test3 = new HashSet<Number160>();
        test3.add( new Number160( 11 ) );
        test3.add( new Number160( 13 ) );
        test3.add( new Number160( 14 ) );
        rawKeys.put( pa3, test3 );
        VotingSchemeDHT evs = new VotingSchemeDHT();
        Collection<Number160> tmp = evs.evaluate1( rawKeys );
        Assert.assertEquals( false, tmp.contains( new Number160( 11 ) ) );
        Assert.assertEquals( true, tmp.contains( new Number160( 12 ) ) );
        Assert.assertEquals( true, tmp.contains( new Number160( 13 ) ) );
        Assert.assertEquals( true, tmp.contains( new Number160( 14 ) ) );
    }

    @Test
    public void testEvaluationData1()
        throws Exception
    {
        byte[] me11 = new byte[] { 1, 1 };
        byte[] me12 = new byte[] { 2, 2 };
        byte[] me13 = new byte[] { 3, 3 };
        byte[] me14 = new byte[] { 4, 4 };
        Map<PeerAddress, Map<Number160, Data>> rawData = new HashMap<PeerAddress, Map<Number160, Data>>();
        PeerAddress pa1 = Utils2.createAddress( 19 );
        Map<Number160, Data> test1 = new HashMap<Number160, Data>();
        test1.put( new Number160( 12 ), new Data( me12 ) );
        test1.put( new Number160( 13 ), new Data( me13 ) );
        test1.put( new Number160( 14 ), new Data( me14 ) );
        rawData.put( pa1, test1 );
        //
        PeerAddress pa2 = Utils2.createAddress( 20 );
        Map<Number160, Data> test2 = new HashMap<Number160, Data>();
        test2.put( new Number160( 12 ), new Data( me12 ) );
        test2.put( new Number160( 13 ), new Data( me13 ) );
        rawData.put( pa2, test2 );
        //
        PeerAddress pa3 = Utils2.createAddress( 21 );
        Map<Number160, Data> test3 = new HashMap<Number160, Data>();
        test3.put( new Number160( 11 ), new Data( me11 ) );
        test3.put( new Number160( 13 ), new Data( me13 ) );
        test3.put( new Number160( 14 ), new Data( me14 ) );
        rawData.put( pa3, test3 );
        VotingSchemeDHT evs = new VotingSchemeDHT();
        Map<Number160, Data> tmp = evs.evaluate2( rawData );
        Assert.assertEquals( false, tmp.containsKey( new Number160( 11 ) ) );
        Assert.assertEquals( true, tmp.containsKey( new Number160( 12 ) ) );
        Assert.assertEquals( true, tmp.containsKey( new Number160( 13 ) ) );
        Assert.assertEquals( true, tmp.containsKey( new Number160( 14 ) ) );
    }

    @Test
    public void testEvaluationData2()
        throws Exception
    {
        byte[] me11 = new byte[] { 1, 1 };
        byte[] me12 = new byte[] { 2, 2 };
        byte[] me13 = new byte[] { 3, 3 };
        byte[] me14 = new byte[] { 4, 4 };
        Map<PeerAddress, Map<Number160, Data>> rawData = new HashMap<PeerAddress, Map<Number160, Data>>();
        PeerAddress pa1 = Utils2.createAddress( 19 );
        Map<Number160, Data> test1 = new HashMap<Number160, Data>();
        test1.put( new Number160( 12 ), new Data( me12 ) );
        test1.put( new Number160( 13 ), new Data( me13 ) );
        test1.put( new Number160( 14 ), new Data( me14 ) );
        rawData.put( pa1, test1 );
        //
        PeerAddress pa2 = Utils2.createAddress( 20 );
        Map<Number160, Data> test2 = new HashMap<Number160, Data>();
        test2.put( new Number160( 12 ), new Data( me11 ) );
        test2.put( new Number160( 13 ), new Data( me13 ) );
        rawData.put( pa2, test2 );
        //
        PeerAddress pa3 = Utils2.createAddress( 21 );
        Map<Number160, Data> test3 = new HashMap<Number160, Data>();
        test3.put( new Number160( 11 ), new Data( me11 ) );
        test3.put( new Number160( 13 ), new Data( me13 ) );
        test3.put( new Number160( 14 ), new Data( me14 ) );
        rawData.put( pa3, test3 );
        VotingSchemeDHT evs = new VotingSchemeDHT();
        Map<Number160, Data> tmp = evs.evaluate2( rawData );
        Assert.assertEquals( false, tmp.containsKey( new Number160( 11 ) ) );
        Assert.assertEquals( false, tmp.containsKey( new Number160( 12 ) ) );
        Assert.assertEquals( true, tmp.containsKey( new Number160( 13 ) ) );
        Assert.assertEquals( true, tmp.containsKey( new Number160( 14 ) ) );
    }
}
