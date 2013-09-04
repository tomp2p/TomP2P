package net.tomp2p.examples;

import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.utils.ConcurrentCacheMap;
import net.tomp2p.utils.Timings;

public class ExampleCache
{
    public static void main( String[] args )
    {
        final ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>( 6, 1024 );
        test.put( "hallo0", "test0" );
        Timings.sleepUninterruptibly( 3000 );
        final AtomicBoolean failed = new AtomicBoolean( false );
        for ( int i = 1; i < 800; i++ )
        {
            final int ii = i;
            new Thread( new Runnable()
            {
                @Override
                public void run()
                {
                    test.put( "hallo" + ii, "test" + ii );
                    new Thread( new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            String val = test.get( "hallo" + ii );
                            if ( !( "test" + ii ).equals( val ) )
                            {
                                failed.set( true );
                            }
                        }
                    } ).start();
                }
            } ).start();
        }
        Timings.sleepUninterruptibly( 3000 );
        System.out.println( "expected: " + ( 800 - 1 ) + ", got: " + test.size() + ", failed: " + failed.get()
            + " - expired " + test.expiredCounter() );
    }
}
