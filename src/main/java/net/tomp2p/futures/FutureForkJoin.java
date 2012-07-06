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
package net.tomp2p.futures;

import java.util.ArrayList;
import java.util.List;

/**
 * The key future for recursive loops. A first version with the fork-join framework did not reduce the code complexity
 * significantly, thus I decided to write this class. The basic idea is that you can create parallel loops. For example
 * in a routing process (loop to find closest peers), one starts to ask 3 peers in parallel, the first that returns
 * result gets evaluated for new information about other peers, and a new peer is asked. If two peers finish, then two
 * other peers are asked. Thus, we keep always 3 connections running until we get the result.
 * 
 * @author Thomas Bocek
 * @param <K>
 */
public class FutureForkJoin<K extends BaseFuture>
    extends BaseFutureImpl<FutureForkJoin<K>>
    implements BaseFuture
{
    // setup
    final private K[] forks;

    final private int nrFutures;

    final private int nrFinishFuturesSuccess;

    final private boolean cancelFuturesOnFinish;

    final private List<K> forksCopy = new ArrayList<K>();

    // all these values are accessed within synchronized blocks
    private K last;

    private int counter = 0;

    private int successCounter = 0;

    // indication if this future is done. Compared to complete, this may be used
    // outside of a synchronized block.
    volatile private boolean completedJoin = false;

    /**
     * Facade if we expect everything to return successfully
     * 
     * @param forks The futures
     */
    public FutureForkJoin( K... forks )
    {
        this( forks.length, false, forks );
    }

    /**
     * Create a future fork join object
     * 
     * @param nrFinishFuturesSuccess Is the number of futures that we expect to succeed.
     * @param cancelFuturesOnFinish Tells use if we should cancel the remaining futures. For get() it makes sense to
     *            cancel, for store() it does not.
     * @param forks The future array, that may contain null futures.
     */
    public FutureForkJoin( int nrFinishFuturesSuccess, boolean cancelFuturesOnFinish, K... forks )
    {
        this.nrFinishFuturesSuccess = nrFinishFuturesSuccess;
        this.forks = forks;
        this.cancelFuturesOnFinish = cancelFuturesOnFinish;
        // the futures array may have null entries, so count first.
        nrFutures = forks.length;
        if ( this.nrFutures <= 0 )
        {
            setFailed( "We have no futures: " + nrFutures );
        }
        else
        {
            join();
        }
        self( this );
    }

    /**
     * Adds listeners and evaluates the result and when to notify the listeners.
     */
    private void join()
    {
        for ( int i = 0; i < nrFutures; i++ )
        {
            if ( completedJoin )
                return;
            final int index = i;
            if ( forks[index] != null )
            {
                forks[index].addListener( new BaseFutureAdapter<K>()
                {
                    @Override
                    public void operationComplete( final K future )
                        throws Exception
                    {
                        evaluate( future, index );
                    }
                } );
            }
            else
            {
                boolean notifyNow = false;
                synchronized ( lock )
                {
                    if ( completed )
                    {
                        return;
                    }
                    // if counter reaches nrFutures, that means we are finished
                    // and in this case, we failed otherwise, in evaluate,
                    // successCounter would finish first
                    if ( ++counter >= nrFutures )
                    {
                        notifyNow = setFinish( FutureType.FAILED );
                    }
                }
                if ( notifyNow )
                {
                    notifyListerenrs();
                    cancelAll();
                    return;
                }
            }
        }
    }

    /**
     * Evaluates one future and determines if this future is finished.
     * 
     * @param finished The future to evaluate
     * @param index the index in the array.
     */
    private void evaluate( K finished, int index )
    {
        boolean notifyNow = false;
        synchronized ( lock )
        {
            if ( completed )
                return;
            // add the future that we have evaluated
            forksCopy.add( finished );
            this.last = finished;
            forks[index] = null;
            if ( finished.isSuccess() && ++successCounter >= nrFinishFuturesSuccess )
            {
                notifyNow = setFinish( FutureType.OK );
            }
            else if ( ++counter >= nrFutures )
            {
                notifyNow = setFinish( FutureType.FAILED );
            }
        }
        if ( notifyNow )
        {
            notifyListerenrs();
            cancelAll();
        }
    }

    /**
     * Cancels all remaining futures if requested by the user
     */
    private void cancelAll()
    {
        if ( cancelFuturesOnFinish )
        {
            for ( K future : forks )
            {
                if ( future != null )
                    future.cancel();
            }
        }
    }

    /**
     * Sets this future to complete. Always call this from a synchronized block
     * 
     * @param last The last future that set this future to complete
     * @return True if other listener should get notified
     */
    private boolean setFinish( FutureType type )
    {
        if ( !setCompletedAndNotify() )
            return false;
        this.completedJoin = true;
        this.type = type;
        return true;
    }

    @Override
    public String getFailedReason()
    {
        synchronized ( lock )
        {
            StringBuilder sb = new StringBuilder( "FFJ:" ).append( reason );
            sb.append( ", type:" ).append( type );
            if ( last != null )
            {
                sb.append( ", last:" ).append( last.getFailedReason() ).append( "rest:" );
            }
            for ( K k : getCompleted() )
            {
                sb.append( "," ).append( k.getFailedReason() );
            }
            return sb.toString();
        }
    }

    /**
     * Returns the last evaluated future. This method may return null if an array with null values have been has been
     * used.
     * 
     * @return The last evaluated future.
     */
    public K getLast()
    {
        synchronized ( lock )
        {
            return last;
        }
    }

    /**
     * Returns a list of evaluated futures. The last completed future is the same as retrieved with {@link #getLast()}.
     * 
     * @return A list of evaluated futures.
     */
    public List<K> getCompleted()
    {
        synchronized ( lock )
        {
            return forksCopy;
        }
    }

    /**
     * Returns the number of successful finished futures
     * 
     * @return The number of successful finished futures
     */
    public int getSuccessCounter()
    {
        synchronized ( lock )
        {
            return successCounter;
        }
    }
}
