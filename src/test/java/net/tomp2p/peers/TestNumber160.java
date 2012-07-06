package net.tomp2p.peers;

import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;

import junit.framework.Assert;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerMap;

import org.junit.Test;

public class TestNumber160
{
    private final Random rnd = new Random();

    @Test
    public void testLength()
    {
        Number160 bi1 = new Number160( "0x7f" );
        Number160 bi2 = new Number160( "0x80" );
        Number160 bi3 = new Number160( "0xff" );
        Assert.assertEquals( false, bi1.compareTo( bi2 ) == 0 );
        Assert.assertEquals( bi3, PeerMap.distance( bi1, bi2 ) );
        Assert.assertEquals( 7, PeerMap.classMember( bi1, bi2 ) );
        Assert.assertEquals( 6, PeerMap.classMember( bi2, bi3 ) );
    }

    @Test
    public void testXor()
    {
        BigInteger bi1 = new BigInteger( "1234567890abcdef1234567890abcdef12345678", 16 );
        Number160 ki1 = new Number160( "0x1234567890abcdef1234567890abcdef12345678" );
        BigInteger bi2 = new BigInteger( "357116889007843534245232322114545905234a", 16 );
        Number160 ki2 = new Number160( "0x357116889007843534245232322114545905234a" );
        Assert.assertEquals( "0x" + bi1.toString( 16 ), ki1.toString( true ) );
        Assert.assertEquals( "0x" + bi2.toString( 16 ), ki2.toString( true ) );
        Number160 ki3 = ki1.xor( ki2 );
        BigInteger bi3 = bi1.xor( bi2 );
        Assert.assertEquals( "0x" + bi3.toString( 16 ), ki3.toString( true ) );
    }

    @Test
    public void testRandomXor()
    {
        for ( int i = 0; i < 1000; i++ )
        {
            BigInteger bi1 = new BigInteger( 160, rnd );
            BigInteger bi2 = new BigInteger( 160, rnd );
            if ( bi1.toString( 16 ).length() == 40 && bi2.toString( 16 ).length() == 40 )
            {
                Number160 ki1 = new Number160( "0x" + bi1.toString( 16 ) );
                Number160 ki2 = new Number160( "0x" + bi2.toString( 16 ) );
                //
                Number160 ki3 = ki1.xor( ki2 );
                BigInteger bi3 = bi1.xor( bi2 );
                Assert.assertEquals( ki3.toString( true ), "0x" + bi3.toString( 16 ) );
            }
        }
    }

    @Test
    public void testFromIntArray()
    {
        int[] tmp = new int[] { 1, 2, 3, 4, 5 };
        Number160 ki2 = new Number160( tmp );
        Assert.assertEquals( "0x0000000100000002000000030000000400000005", ki2.toString( false ) );
        tmp = new int[] { -1, -1, -1, -1, -1 };
        ki2 = new Number160( tmp );
        Assert.assertEquals( "0xffffffffffffffffffffffffffffffffffffffff", ki2.toString( false ) );
        ki2 = new Number160( new int[] { 1, 2 } );
        Assert.assertEquals( "0x100000002", ki2.toString() );
        ki2 = new Number160( new int[] { 1 } );
        Assert.assertEquals( "0x1", ki2.toString() );
        ki2 = new Number160( new int[] { 1, 1, 1, 1, 1 } );
        Assert.assertEquals( "0x0000000100000001000000010000000100000001", ki2.toString( false ) );
    }

    @Test
    public void testFromString()
    {
        Number160 ki2 = new Number160( "0x9" );
        Assert.assertEquals( "0x9", ki2.toString( true ) );
        ki2 = new Number160( "0x22" );
        Assert.assertEquals( "0x22", ki2.toString( true ) );
        ki2 = new Number160( "0x0000000000000002000000030000000400000005" );
        Assert.assertEquals( "0x2000000030000000400000005", ki2.toString( true ) );
        Assert.assertEquals( "0x0000000000000002000000030000000400000005", ki2.toString( false ) );
        ki2 = new Number160( "0x0400000005" );
        Assert.assertEquals( "0x400000005", ki2.toString( true ) );
        ki2 = new Number160( "0x0000000000000002000000000000000000000000" );
        Assert.assertEquals( "0x2000000000000000000000000", ki2.toString( true ) );
        Assert.assertEquals( "0x0000000000000002000000000000000000000000", ki2.toString( false ) );
        ki2 = new Number160( "0x1000000000000000000000000000000000000000" );
        Assert.assertEquals( "0x1000000000000000000000000000000000000000", ki2.toString( true ) );
        ki2 = new Number160( "0x100000000000000000000" );
        Assert.assertEquals( "0x100000000000000000000", ki2.toString( true ) );
    }

    @Test
    public void testFromInt()
    {
        Number160 ki2 = new Number160( 0x2c );
        Assert.assertEquals( "0x2c", ki2.toString() );
    }

    @Test
    public void testFromByteArray()
    {
        Number160 ki2 = new Number160( new byte[] { 1 } );
        Assert.assertEquals( "0x1", ki2.toString() );
        ki2 = new Number160( 1, 2 );
        Assert.assertEquals( "0x100000002", ki2.toString() );
        ki2 = new Number160( new byte[] { 1, 2 } );
        Assert.assertEquals( "0x102", ki2.toString() );
        //
        ki2 = new Number160( new byte[] { 1 } );
        Number160 ki3 = new Number160( new byte[] { 2 } );
        Assert.assertEquals( true, ki2.compareTo( ki3 ) < 0 );
        //
        ki2 = new Number160( new byte[] { 3 } );
        ki3 = new Number160( new byte[] { 2 } );
        Assert.assertEquals( true, ki2.compareTo( ki3 ) > 0 );
        //
        ki2 = new Number160( new byte[] { 3 } );
        ki3 = new Number160( new byte[] { 3 } );
        Assert.assertEquals( true, ki2.compareTo( ki3 ) == 0 );
        //
        ki2 = new Number160( new byte[] { 2, 3 } );
        ki3 = new Number160( new byte[] { 3, 2 } );
        Assert.assertEquals( true, ki2.compareTo( ki3 ) < 0 );
        //
        ki2 = new Number160( new byte[] { 1, 2, 3 } );
        ki3 = new Number160( new byte[] { 3, 2 } );
        Assert.assertEquals( true, ki2.compareTo( ki3 ) > 0 );
    }

    @Test
    public void testSerialization()
    {
        Number160 ki2 = new Number160( "0x357116889007843534245232322114545905234a" );
        byte[] me = ki2.toByteArray();
        Number160 ki3 = new Number160( me );
        Assert.assertEquals( ki2, ki3 );
        //
        ki2 = Number160.MAX_VALUE;
        me = ki2.toByteArray();
        ki3 = new Number160( me );
        Assert.assertEquals( ki2, ki3 );
        //
        ki2 = new Number160( "0x1234567890abcdef1234567890abcdef12345678" );
        me = ki2.toByteArray();
        ki3 = new Number160( me );
        Assert.assertEquals( ki2, ki3 );
    }

    @Test
    public void testIsZero()
    {
        Number160 ki2 = new Number160( new byte[] { 0 } );
        Number160 ki3 = new Number160( new byte[] { 2 } );
        Assert.assertEquals( true, ki2.isZero() );
        Assert.assertEquals( false, ki3.isZero() );
    }

    @Test
    public void testBitLength()
    {
        Number160 ki2 = Number160.ONE;
        Assert.assertEquals( 1, ki2.bitLength() );
        ki2 = new Number160( "0x2" );
        Assert.assertEquals( 2, ki2.bitLength() );
        ki2 = new Number160( "0x3" );
        Assert.assertEquals( 2, ki2.bitLength() );
        ki2 = new Number160( "0xfa" );
        Assert.assertEquals( 8, ki2.bitLength() );
        ki2 = new Number160( "0x100000002" );
        Assert.assertEquals( 33, ki2.bitLength() );
        ki2 = new Number160( "0x1100000002" );
        Assert.assertEquals( 37, ki2.bitLength() );
        try
        {
            ki2 = new Number160( "0x-1" );
            Assert.fail();
        }
        catch ( RuntimeException e )
        {
        }
        ki2 = Number160.MAX_VALUE;
        Assert.assertEquals( 160, ki2.bitLength() );
    }

    @Test
    public void testDouble()
    {
        BigInteger bi1 = new BigInteger( "1234567890abcdef1234567890abcdef12345678", 16 );
        Number160 ki1 = new Number160( "0x1234567890abcdef1234567890abcdef12345678" );
        BigInteger bi2 = new BigInteger( "357116889007843534245232322114545905234a", 16 );
        Number160 ki2 = new Number160( "0x357116889007843534245232322114545905234a" );
        Assert.assertEquals( 0, Double.compare( bi1.doubleValue(), ki1.doubleValue() ) );
        Assert.assertEquals( 0, Double.compare( bi2.doubleValue(), ki2.doubleValue() ) );
    }

    @Test
    public void testFloat()
    {
        BigInteger bi1 = new BigInteger( "1234567890abcdef1234567890abcdef12345678", 16 );
        Number160 ki1 = new Number160( "0x1234567890abcdef1234567890abcdef12345678" );
        BigInteger bi2 = new BigInteger( "357116889007843534245232322114545905234a", 16 );
        Number160 ki2 = new Number160( "0x357116889007843534245232322114545905234a" );
        Assert.assertEquals( 0, Float.compare( bi1.floatValue(), ki1.floatValue() ) );
        Assert.assertEquals( 0, Float.compare( bi2.floatValue(), ki2.floatValue() ) );
    }

    @Test
    public void testPerformance()
    {
        int runs = 10000;
        BigInteger bi1[] = new BigInteger[runs];
        BigInteger bi2[] = new BigInteger[runs];
        for ( int i = 0; i < runs; i++ )
        {
            bi1[i] = new BigInteger( 160, rnd );
            bi2[i] = new BigInteger( 160, rnd );
            if ( bi1[i].toString( 16 ).length() != 40 || bi2[i].toString( 16 ).length() != 40 )
                i--;
        }
        Number160 ki1[] = new Number160[runs];
        Number160 ki2[] = new Number160[runs];
        for ( int i = 0; i < runs; i++ )
        {
            ki1[i] = new Number160( "0x" + bi1[i].toString( 16 ) );
            ki2[i] = new Number160( "0x" + bi2[i].toString( 16 ) );
        }
        long start = System.currentTimeMillis();
        BigInteger bi3 = BigInteger.TEN;
        for ( int j = 1; j < 1000; j++ )
        {
            for ( int i = 0; i < runs; i++ )
                // int i = 0;
                bi3 = bi1[( i * j ) % runs].xor( bi2[i] ).xor( bi3 );
        }
        System.err.println( "BigInteger time " + ( System.currentTimeMillis() - start ) + ", " + bi3.toString( 16 ) );
        start = System.currentTimeMillis();
        Number160 ki3 = new Number160( 10 );
        for ( int j = 1; j < 1000; j++ )
        {
            for ( int i = 0; i < runs; i++ )
                ki3 = ki1[( i * j ) % runs].xor( ki2[i] ).xor( ki3 );
        }
        System.err.println( "Key time " + ( System.currentTimeMillis() - start ) + ", " + ki3 );
        Assert.assertEquals( "0x" + bi3.toString( 16 ), ki3.toString() );
    }

    @Test
    public void testPerformance2()
    {
        ConcurrentSkipListSet<BigInteger> test1 = new ConcurrentSkipListSet<BigInteger>();
        ConcurrentSkipListSet<Number160> test2 = new ConcurrentSkipListSet<Number160>();
        int runs = 10000;
        BigInteger bi1[] = new BigInteger[runs];
        for ( int i = 0; i < runs; i++ )
        {
            bi1[i] = new BigInteger( 160, rnd );
            if ( bi1[i].toString( 16 ).length() != 40 )
                i--;
        }
        Number160 ki1[] = new Number160[runs];
        for ( int i = 0; i < runs; i++ )
            ki1[i] = new Number160( "0x" + bi1[i].toString( 16 ) );
        long start = System.currentTimeMillis();
        for ( int j = 0; j < 500; j++ )
        {
            test1.clear();
            for ( int i = 0; i < runs; i++ )
                test1.add( bi1[i] );
        }
        System.err.println( "BigInteger time " + ( System.currentTimeMillis() - start ) );
        start = System.currentTimeMillis();
        for ( int j = 0; j < 500; j++ )
        {
            test2.clear();
            for ( int i = 0; i < runs; i++ )
                test2.add( ki1[i] );
        }
        System.err.println( "Key time " + ( System.currentTimeMillis() - start ) );
        Assert.assertEquals( "0x" + test1.first().toString( 16 ), test2.first().toString() );
    }

    @Test
    public void testLong()
    {
        Number160 n1 = new Number160( 1 );
        Number160 n2 = new Number160( 1L );
        Assert.assertEquals( n1, n2 );
        //
        n2 = new Number160( Long.MAX_VALUE );
        Assert.assertEquals( "0x7FFFFFFFFFFFFFFF".toLowerCase(), n2.toString() );
    }

    @Test
    public void testBitShiftRight()
    {
        Number160 n1 = new Number160( 1 );
        Number160 n2 = n1.shiftRight( 1 );
        Assert.assertEquals( 0, n2.intValue() );
        n1 = new Number160( 2 );
        n2 = n1.shiftRight( 1 );
        Assert.assertEquals( 1, n2.intValue() );
        n1 = new Number160( "0x100000000" );
        n2 = n1.shiftRight( 1 );
        Assert.assertEquals( "0x80000000", n2.toString() );
        n1 = new Number160( "0x100000000" );
        n2 = n1.shiftRight( 2 );
        Assert.assertEquals( "0x40000000", n2.toString() );
        n1 = Number160.MAX_VALUE;
        n2 = n1.shiftRight( 128 );
        Assert.assertEquals( "0xffffffff", n2.toString() );
        n1 = new Number160( "0xff00000000000000000000000000000000000000" );
        n2 = n1.shiftRight( 128 );
        Assert.assertEquals( "0xff000000", n2.toString() );
        n1 = new Number160( "0xff00000110000000000000000000000000000000" );
        n2 = n1.shiftRight( 96 );
        Assert.assertEquals( "0xff00000110000000", n2.toString() );
    }

    @Test
    public void testBitShiftLeft()
    {
        Number160 n1 = new Number160( 1 );
        Number160 n2 = n1.shiftLeft( 1 );
        Assert.assertEquals( 2, n2.intValue() );
        n1 = new Number160( 2 );
        n2 = n1.shiftLeft( 1 );
        Assert.assertEquals( 4, n2.intValue() );
        //
        n1 = new Number160( 1 );
        n2 = n1.shiftLeft( 32 );
        Assert.assertEquals( "0x100000000", n2.toString() );
        //
        n1 = new Number160( 1 );
        n2 = n1.shiftLeft( 33 );
        Assert.assertEquals( "0x200000000", n2.toString() );
        n1 = new Number160( 3 );
        n2 = n1.shiftLeft( 33 );
        Assert.assertEquals( "0x600000000", n2.toString() );
    }

    @Test
    public void testBitShift1()
    {
        Random rnd = new Random( 42L );
        Number160 n1 = new Number160( rnd );
        long last = n1.unsignedInt( 0 );
        for ( int i = 0; i < 100; i++ )
        {
            int r = rnd.nextInt( 128 ) + 1;
            Number160 n2 = n1.shiftRight( r );
            n1 = n2.shiftLeft( r );
            Assert.assertEquals( last, n1.unsignedInt( 0 ) );
        }
    }

    @Test
    public void testBitShift2()
    {
        Random rnd = new Random( 42L );
        Number160 n1 = new Number160( rnd );
        long last = n1.unsignedInt( 4 );
        for ( int i = 0; i < 100; i++ )
        {
            int r = rnd.nextInt( 128 ) + 1;
            Number160 n2 = n1.shiftLeft( r );
            n1 = n2.shiftRight( r );
            Assert.assertEquals( last, n1.unsignedInt( 4 ) );
        }
    }

}
