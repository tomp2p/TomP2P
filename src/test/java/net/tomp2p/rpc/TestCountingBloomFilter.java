package net.tomp2p.rpc;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author Thomas Bocek
 */
public class TestCountingBloomFilter
{
    @Test
    public void testCountingBloomFilter()
    {
        int[] counting = new int[10];
        CountingBloomFilter<String> cbs = new CountingBloomFilter<String>( 40, counting );

        cbs.add( "abc" );
        cbs.add( "abc" );
        cbs.add( "abc" );
        cbs.add( "abd" );
        cbs.add( "abe" );

        Assert.assertEquals( 3, cbs.approximateCount( "abc" ) );
        Assert.assertEquals( 1, cbs.approximateCount( "abd" ) );
        Assert.assertEquals( 0, cbs.approximateCount( "abg" ) );

    }

    @Test
    public void testCountingBloomFilter2()
    {
        int[] counting = new int[20];
        CountingBloomFilter<String> cbs = new CountingBloomFilter<String>( 200, counting );
        
        System.out.println(cbs.expectedFalsePositiveProbability());

        for ( int i = 0; i < 100; i++ )
        {
            cbs.add( "abc" );
        }
        for ( int i = 0; i < 100; i++ )
        {
            cbs.add( "abc" + i );
        }

        Assert.assertEquals( 100, cbs.approximateCount( "abc" ) );

    }
    
    @Test
    public void testCountingBloomFilter3()
    {
        int[] counting = new int[10];
        CountingBloomFilter<String> cbs = new CountingBloomFilter<String>( 40, counting );

        cbs.add( "abc" );
        cbs.add( "abc" );
        cbs.add( "abc" );
        cbs.add( "abd" );
        cbs.add( "abe" );

        Assert.assertEquals( true, cbs.contains( "abc" ) );
    }
}
