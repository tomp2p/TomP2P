package net.tomp2p.connection;

import junit.framework.Assert;
import net.tomp2p.futures.FutureDiscover;

import org.junit.Test;

public class TestScheduler
{
    @Test
    public void testOrder()
    {
        Scheduler s = new Scheduler( 5, 5 );
        FutureDiscover fd = new FutureDiscover();
        s.scheduleTimeout( fd, 3000, "third" );
        s.scheduleTimeout( fd, 1000, "first" );
        s.scheduleTimeout( fd, 2000, "second" );
        Assert.assertEquals( "first", s.poll() );
        Assert.assertEquals( "second", s.poll() );
        Assert.assertEquals( "third", s.poll() );
    }
}
