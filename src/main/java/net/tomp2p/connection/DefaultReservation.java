/*
 * Copyright 2009 Thomas Bocek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.tomp2p.connection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import net.tomp2p.futures.FutureRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultReservation
    implements Reservation
{
    final private static Logger logger = LoggerFactory.getLogger( DefaultReservation.class );

    final private Map<Long, Boolean> threads = new ConcurrentHashMap<Long, Boolean>();

    private volatile boolean shutdown = false;

    /*
     * (non-Javadoc)
     * @see net.tomp2p.connection.Reservation#shutdown()
     */
    @Override
    public void shutdown()
    {
        shutdown = true;
    }

    /*
     * (non-Javadoc)
     * @see net.tomp2p.connection.Reservation#acquire(java.util.concurrent.Semaphore, int)
     */
    @Override
    public boolean acquire( Semaphore semaphore, int permits )
    {
        boolean acquired = false;
        while ( !acquired && !shutdown )
        {
            try
            {
                acquired = semaphore.tryAcquire( permits );
                if ( !acquired )
                {
                    if ( logger.isDebugEnabled() )
                    {
                        logger.debug( "cannot acquire " + permits + ", now we have " + semaphore.availablePermits() );
                    }
                    synchronized ( semaphore )
                    {
                        semaphore.wait( 250 );
                    }
                }
                else
                {
                    if ( logger.isDebugEnabled() )
                    {
                        logger.debug( "acquired " + permits + ", now we have " + semaphore.availablePermits() );
                    }
                }
            }
            catch ( InterruptedException e )
            {
                return false;
            }
        }
        return acquired;
    }

    @Override
    public void runDeadLockProof( Scheduler scheduler, FutureRunnable futureRunnable )
    {
        // Here we need to figure out if we can block or not. We need to care
        // for two cases: (1) when we come from a netty thread, we should not
        // block, otherwise we may block nio worker threads which will slow down
        // network and (2) if we previously reserved a connection from a specify
        // thread. In this case we should not block, otherwise we will see a
        // deadlock. The deadlock can happend becauese if we have 10 connections
        // and 10 threads reserving one connection, then from those thread we
        // want to reserve one more connection -> boom and we have a deadlock.
        //
        // If we use simgrid, we have to use a single thread, thus disable this with
        // useReservationThread
        if ( Thread.currentThread().getName().startsWith( ConnectionHandler.THREAD_NAME )
            || threads.containsKey( Thread.currentThread().getId() ) )
        {
            scheduler.addQueue( futureRunnable );
        }
        else
        {
            futureRunnable.run();
        }
    }

    @Override
    public void prepareDeadLockCheck()
    {
        threads.put( Thread.currentThread().getId(), Boolean.TRUE );
    }

    @Override
    public void removeDeadLockCheck( long creatorThread )
    {
        threads.remove( creatorThread );
    }
}