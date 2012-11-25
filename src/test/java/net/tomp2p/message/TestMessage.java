package net.tomp2p.message;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

import net.tomp2p.Utils2;
import net.tomp2p.message.Message.Command;
import net.tomp2p.peers.DefaultMapAcceptHandler;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.storage.Data;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;

public class TestMessage
{
    /**
     * Tests encoding of an empty message
     */

    @Test
    public void testEncodeDecode()
        throws Exception
    {
        testEncodeDecode( true );
        testEncodeDecode( false );
    }

    private void testEncodeDecode( boolean tcp )
        throws Exception
    {
        // encode
        Message m1 = Utils2.createDummyMessage();
        ChannelBuffer buffer = encode( m1, tcp );
        // decode
        Message m2 = decode( buffer, tcp );
        compareMessage( m1, m2 );
    }

    /**
     * Tests a different command, type and integer
     */

    @Test
    public void testEncodeDecode2()
        throws Exception
    {
        testEncodeDecode2( true );
        testEncodeDecode2( false );
    }

    private void testEncodeDecode2( boolean tcp )
        throws Exception
    {
        // encode
        Message m1 = Utils2.createDummyMessage();
        m1.setCommand( Command.NEIGHBORS );
        m1.setType( Message.Type.DENIED );
        Number160 key1 = new Number160( 5667 );
        Number160 key2 = new Number160( 5667 );
        m1.setKeyKey( key1, key2 );
        List<Number160> tmp2 = new ArrayList<Number160>();
        tmp2.add( new Number160( "0x234567890" ) );
        tmp2.add( new Number160( "0x77" ) );
        m1.setKeys( tmp2 );
        ChannelBuffer buffer = encode( m1, tcp );
        // decode
        Message m2 = decode( buffer, tcp );
        Assert.assertEquals( false, m2.getKeyKey1() == null );
        Assert.assertEquals( false, m2.getKeys() == null );
        compareMessage( m1, m2 );
    }

    @Test
    public void testEncodeDecode3()
        throws Exception
    {
        testEncodeDecode3( true );
        testEncodeDecode3( false );
    }

    /**
     * Tests Number160 and string
     */
    private void testEncodeDecode3( boolean tcp )
        throws Exception
    {
        // encode
        Message m1 = Utils2.createDummyMessage();
        m1.setType( Message.Type.DENIED );
        m1.setLong( 8888888 );
        byte[] me = new byte[10000];
        ChannelBuffer tmp = ChannelBuffers.wrappedBuffer( me );
        m1.setPayload( tmp );
        ChannelBuffer buffer = encode( m1, tcp );
        // decode
        Message m2 = decode( buffer, tcp );
        Assert.assertEquals( false, m2.getPayload1() == null );
        compareMessage( m1, m2 );
    }

    /**
     * Tests neighbors and payload
     */
    @Test
    public void testEncodeDecode4()
        throws Exception
    {
        testEncodeDecode4( true );
        testEncodeDecode4( false );
    }

    private void testEncodeDecode4( boolean tcp )
        throws Exception
    {
        // setup
        DummyCoder coder = new DummyCoder( tcp );
        // encode
        Message m1 = Utils2.createDummyMessage();
        m1.setType( Message.Type.DENIED );
        KeyPairGenerator gen = KeyPairGenerator.getInstance( "DSA" );
        KeyPair pair1 = gen.generateKeyPair();
        m1.setPublicKeyAndSign( pair1 );
        Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
        dataMap.put( new Number160( 45 ), new Data( new byte[] { 3, 4, 5 } ) );
        dataMap.put( new Number160( 46 ), new Data( new byte[] { 4, 5, 6 } ) );
        Data data = new Data( new byte[] { 5, 6, 7 } );
        data.setProtectedEntry( true );
        dataMap.put( new Number160( 47 ), data );
        m1.setDataMap( dataMap );
        Map<Number160, Number160> keyMap = new HashMap<Number160, Number160>();
        keyMap.put( new Number160( 55 ), new Number160( 66 ) );
        keyMap.put( new Number160( 551 ), new Number160( 661 ) );
        keyMap.put( new Number160( 5511 ), new Number160( 66111 ) );
        m1.setKeyMap( keyMap );

        Message m2 = coder.decode( coder.encode( m1 ) );
        // test
        Assert.assertEquals( false, m2.getDataMap() == null );
        Assert.assertEquals( false, m2.getKeyMap() == null );
        compareMessage( m1, m2 );
    }

    @Test
    public void testEncodeDecode6()
        throws Exception
    {
        testEncodeDecode6( true );
        testEncodeDecode6( false );
    }

    private void testEncodeDecode6( boolean tcp )
        throws Exception
    {
        for ( int i = 0; i < 4; i++ )
        {
            // encode and test for is firewallend and ipv4
            Message m1 = Utils2.createDummyMessage( ( i & 1 ) > 0, ( i & 2 ) > 0 );
            m1.setType( Message.Type.DENIED );
            ChannelBuffer buffer = encode( m1, tcp );
            Message m2 = decode( buffer, tcp );
            compareMessage( m1, m2 );
        }
    }

    /**
     * Test encode decode performance. On my laptop IBM T60 its around 190 enc-dec/ms, so we can encode and decode with
     * a single core 192'000 messages per second. For a message smallest size with 59bytes, this means that we can
     * saturate an 86mbit/s link. The larger the message, the more bandwidth it is used.
     * 
     * @throws Exception
     */
    @Test
    public void testPerformanceTCPEncoder()
        throws Exception
    {
        // setup
        SocketAddress sockRemote = new InetSocketAddress( 2000 );
        SocketAddress sockLocal = new InetSocketAddress( 1000 );
        DummyChannel dc = new DummyChannel( sockRemote, sockLocal );
        // encode
        Message m1 = Utils2.createDummyMessage();
        m1.setType( Message.Type.UNKNOWN_ID );
        List<Number160> tmp = new ArrayList<Number160>();
        tmp.add( new Number160( "0x1234567890" ) );
        tmp.add( new Number160( "0x777" ) );
        m1.setKeys( tmp );
        // System.in.read();ChannelBuffer
        long start = System.currentTimeMillis();
        int len = 5000000;
        ChannelBuffer buffer = null;
        DummyCoder coder = new DummyCoder( true );
        TomP2PDecoderTCP dec = new TomP2PDecoderTCP();
        for ( int i = 0; i < len; i++ )
        {
            // encode
            buffer = coder.encode( m1 );
            // decode
            dec.decode( null, dc, buffer );
        }
        long stop = System.currentTimeMillis() - start;
        System.out.println( "Performance: " + ( len / stop ) + " enc-dec/ms (reference: QuadCore~330 enc-dec/ms)" );
    }

    @Test
    public void testOrder()
    {
        Number160 b1 = new Number160( "0x5" );
        Number160 b2 = new Number160( "0x32" );
        Number160 b3 = new Number160( "0x1F4" );
        Number160 b4 = new Number160( "0x1388" );
        PeerAddress n1 = new PeerAddress( b1 );
        PeerAddress n2 = new PeerAddress( b2 );
        PeerAddress n3 = new PeerAddress( b3 );
        PeerAddress n4 = new PeerAddress( b4 );
        PeerMap routingMap = new PeerMap( b1, 2, 60 * 1000, 3, new int[0], 100, new DefaultMapAcceptHandler( false ) );
        final NavigableSet<PeerAddress> queue = new TreeSet<PeerAddress>( routingMap.createPeerComparator( b3 ) );
        queue.add( n1 );
        queue.add( n2 );
        queue.add( n3 );
        queue.add( n4 );
        Assert.assertEquals( queue.pollFirst(), n3 );
        Assert.assertEquals( queue.pollFirst(), n2 );
        Assert.assertEquals( queue.pollLast(), n4 );
    }

    @Test
    public void testNumber160Conversion()
    {
        Number160 i1 = new Number160( "0x9908836242582063284904568868592094332017" );
        Number160 i2 = new Number160( "0x9609416068124319312270864915913436215856" );
        Number160 i3 = new Number160( "0x7960941606812431931227086491591343621585" );
        byte[] me = i1.toByteArray();
        Assert.assertEquals( i1, new Number160( me ) );
        me = i2.toByteArray();
        Assert.assertEquals( i2, new Number160( me ) );
        me = i3.toByteArray();
        byte[] me2 = new byte[20];
        System.arraycopy( me, 0, me2, me2.length - me.length, me.length );
        Assert.assertEquals( i3, new Number160( me2 ) );
    }

    @Test
    public void testBigData()
        throws Exception
    {
        final int size = 50 * 1024 * 1024;
        Message m1 = Utils2.createDummyMessage();
        Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
        Data data = new Data( new byte[size] );
        dataMap.put( Number160.ONE, data );
        m1.setDataMap( dataMap );
        // encode
        ChannelBuffer buffer = encode( m1, true );
        // decode
        Message m2 = decode( buffer, true );
        compareMessage( m1, m2 );
    }

    @Test
    public void testEncodeDecode480Map()
        throws Exception
    {
        // encode
        Message m1 = Utils2.createDummyMessage();
        m1.setType( Message.Type.PARTIALLY_OK );
        KeyPairGenerator gen = KeyPairGenerator.getInstance( "DSA" );
        KeyPair pair1 = gen.generateKeyPair();
        m1.setPublicKeyAndSign( pair1 );
        Map<Number480, Data> dataMap = new HashMap<Number480, Data>();
        Random rnd = new Random( 42l );
        for ( int i = 0; i < 1000; i++ )
        {
            dataMap.put( new Number480( new Number160( rnd ), new Number160( rnd ), new Number160( rnd ) ),
                         new Data( new byte[] { (byte) rnd.nextInt(), (byte) rnd.nextInt(), (byte) rnd.nextInt(),
                             (byte) rnd.nextInt(), (byte) rnd.nextInt() } ) );
        }
        m1.setDataMap480( dataMap );
        Message m2 = decode( encode( m1, true ), true );
        compareMessage( m1, m2 );
    }

    @Test
    public void testEncodeDecode480Set()
        throws Exception
    {
        // encode
        Message m1 = Utils2.createDummyMessage();
        m1.setType( Message.Type.NOT_FOUND );
        KeyPairGenerator gen = KeyPairGenerator.getInstance( "DSA" );
        KeyPair pair1 = gen.generateKeyPair();
        m1.setPublicKeyAndSign( pair1 );
        Collection<Number480> list = new ArrayList<Number480>();
        Random rnd = new Random( 42l );
        for ( int i = 0; i < 1000; i++ )
        {
            list.add( new Number480( new Number160( rnd ), new Number160( rnd ), new Number160( rnd ) ) );
        }
        m1.setKeys480( list );
        Message m2 = decode( encode( m1, true ), true );
        compareMessage( m1, m2 );
    }

    private ChannelBuffer encode( Message m1, boolean tcp )
        throws Exception
    {
        DummyCoder coder = new DummyCoder( tcp );
        return coder.encode( m1 );
    }

    private Message decode( ChannelBuffer buffer, boolean tcp )
        throws Exception
    {
        SocketAddress sockRemote = new InetSocketAddress( 2000 );
        SocketAddress sockLocal = new InetSocketAddress( 1000 );
        DummyChannel dc = new DummyChannel( sockRemote, sockLocal );
        if ( tcp )
        {
            TomP2PDecoderTCP dec = new TomP2PDecoderTCP();
            Object obj = dec.decode( null, dc, buffer );
            return (Message) obj;
        }
        else
        {
            TomP2PDecoderUDP dec = new TomP2PDecoderUDP();
            Object obj = dec.decode( null, dc, buffer, sockLocal );
            return (Message) obj;
        }
    }

    private void compareMessage( Message m1, Message m2 )
    {
        Assert.assertEquals( m1.getMessageId(), m2.getMessageId() );
        Assert.assertEquals( m1.getVersion(), m2.getVersion() );
        Assert.assertEquals( m1.getCommand(), m2.getCommand() );
        Assert.assertEquals( m1.getContentType1(), m2.getContentType1() );
        Assert.assertEquals( m1.getContentType2(), m2.getContentType2() );
        Assert.assertEquals( m1.getRecipient(), m2.getRecipient() );
        Assert.assertEquals( m1.getType(), m2.getType() );
        Assert.assertEquals( m1.getSender(), m2.getSender() );
        Assert.assertEquals( m1.getBloomFilter1(), m2.getBloomFilter1() );
        Assert.assertEquals( m1.getBloomFilter2(), m2.getBloomFilter2() );
        Assert.assertEquals( m1.getPublicKey(), m2.getPublicKey() );
        Assert.assertEquals( m1.getKeyKey1(), m2.getKeyKey1() );
        Assert.assertEquals( m1.getKeyKey2(), m2.getKeyKey2() );
        Assert.assertEquals( m1.getLong(), m2.getLong() );
        Assert.assertEquals( m1.getInteger(), m2.getInteger() );
        Assert.assertEquals( m1.getSender().isFirewalledTCP(), m2.getSender().isFirewalledTCP() );
        Assert.assertEquals( m1.getSender().isFirewalledUDP(), m2.getSender().isFirewalledUDP() );
        //
        if ( m1.getNeighbors() != null || m2.getNeighbors() != null )
        {
            Assert.assertNotNull( m1.getNeighbors() );
            Assert.assertNotNull( m2.getNeighbors() );
            Assert.assertEquals( m1.getNeighbors().size(), m2.getNeighbors().size() );
            Iterator<PeerAddress> it1 = m1.getNeighbors().iterator();
            Iterator<PeerAddress> it2 = m2.getNeighbors().iterator();
            while ( it1.hasNext() && it2.hasNext() )
            {
                PeerAddress p1 = it1.next();
                PeerAddress p2 = it2.next();
                Assert.assertEquals( p1, p2 );
                Assert.assertEquals( p1.createSocketTCP(), p2.createSocketTCP() );
                Assert.assertEquals( p1.createSocketUDP(), p2.createSocketUDP() );
            }
        }
        if ( m1.getKeys() != null || m2.getKeys() != null )
        {
            Assert.assertNotNull( m1.getKeys() );
            Assert.assertNotNull( m2.getKeys() );
            Assert.assertEquals( m1.getKeys().size(), m2.getKeys().size() );
            Iterator<Number160> it1 = m1.getKeys().iterator();
            Iterator<Number160> it2 = m2.getKeys().iterator();
            while ( it1.hasNext() && it2.hasNext() )
            {
                Number160 key1 = it1.next();
                Number160 key2 = it2.next();
                Assert.assertEquals( key1, key2 );
            }
        }
        
        if ( m1.getKeys480() != null || m2.getKeys480() != null )
        {
            Assert.assertNotNull( m1.getKeys480() );
            Assert.assertNotNull( m2.getKeys480() );
            Assert.assertEquals( m1.getKeys480().size(), m2.getKeys480().size() );
            Iterator<Number480> it1 = m1.getKeys480().iterator();
            Iterator<Number480> it2 = m2.getKeys480().iterator();
            while ( it1.hasNext() && it2.hasNext() )
            {
                Number480 key1 = it1.next();
                Number480 key2 = it2.next();
                Assert.assertEquals( key1, key2 );
            }
        }
        
        //
        if ( m1.getDataMap() != null || m2.getDataMap() != null )
        {
            Assert.assertNotNull( m1.getDataMap() );
            Assert.assertNotNull( m2.getDataMap() );
            Assert.assertEquals( m1.getDataMap().size(), m2.getDataMap().size() );
            Iterator<Number160> it1 = m1.getDataMap().keySet().iterator();
            Iterator<Number160> it2 = m2.getDataMap().keySet().iterator();
            while ( it1.hasNext() && it2.hasNext() )
            {
                Number160 key1 = it1.next();
                Number160 key2 = it2.next();
                Assert.assertEquals( key1, key2 );
                Assert.assertEquals( m1.getDataMap().get( key1 ).getLength(), m2.getDataMap().get( key2 ).getLength() );
                Data d1 = m1.getDataMap().get( key1 );
                Data d2 = m2.getDataMap().get( key2 );
                Assert.assertArrayEquals( d1.getData(), d2.getData() );
            }
        }

        if ( m1.getDataMap480() != null || m2.getDataMap480() != null )
        {
            Assert.assertNotNull( m1.getDataMap480() );
            Assert.assertNotNull( m2.getDataMap480() );
            Assert.assertEquals( m1.getDataMap480().size(), m2.getDataMap480().size() );
            Iterator<Number480> it1 = m1.getDataMap480().keySet().iterator();
            Iterator<Number480> it2 = m2.getDataMap480().keySet().iterator();
            while ( it1.hasNext() && it2.hasNext() )
            {
                Number480 key1 = it1.next();
                Number480 key2 = it2.next();
                Assert.assertEquals( key1, key2 );
                Assert.assertEquals( m1.getDataMap480().get( key1 ).getLength(),
                                     m2.getDataMap480().get( key2 ).getLength() );
                Data d1 = m1.getDataMap480().get( key1 );
                Data d2 = m2.getDataMap480().get( key2 );
                Assert.assertArrayEquals( d1.getData(), d2.getData() );
            }
        }

        if ( m1.getKeyMap() != null || m2.getKeyMap() != null )
        {
            Assert.assertNotNull( m1.getKeyMap() );
            Assert.assertNotNull( m2.getKeyMap() );
            Assert.assertEquals( m1.getKeyMap().size(), m2.getKeyMap().size() );
            Iterator<Number160> it1 = m1.getKeyMap().keySet().iterator();
            Iterator<Number160> it2 = m2.getKeyMap().keySet().iterator();
            while ( it1.hasNext() && it2.hasNext() )
            {
                Number160 key1 = it1.next();
                Number160 key2 = it2.next();
                Assert.assertEquals( key1, key2 );
                Assert.assertEquals( m1.getKeyMap().get( key1 ), m2.getKeyMap().get( key2 ) );
            }
        }

        comparePayload( m1.getPayload1(), m2.getPayload1() );
        comparePayload( m1.getPayload2(), m2.getPayload2() );
    }

    private void comparePayload( ChannelBuffer ch1, ChannelBuffer ch2 )
    {
        if ( ch1 != null || ch2 != null )
        {
            Assert.assertNotNull( ch1 );
            Assert.assertNotNull( ch2 );
            byte[] me1 = new byte[ch1.capacity()];
            ch1.getBytes( 0, me1 );
            byte[] me2 = new byte[ch2.capacity()];
            ch2.getBytes( 0, me2 );
            Assert.assertArrayEquals( me1, me2 );
        }
    }
}
