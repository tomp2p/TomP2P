/*
 * Copyright 2011 Thomas Bocek
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

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.peers.PeerAddress;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

/**
 * This is a handle for permanent connections. With this class the permanent connection can be reused or closed. The is
 * only 1 connection allowed for a given destination. It is important that before Peer.shutdown, all permanent
 * connections are closed. Otherwise shutdown will block forever and will wait until the permanent connection is closed.
 * 
 * @author Thomas Bocek
 */
public class PeerConnection
{
    final private PeerAddress destination;

    final private ChannelCreator channelCreator;

    final private ConnectionReservation connectionReservation;

    final private int idleTCPMillis;

    final private Semaphore oneConnection = new Semaphore( 1 );

    final private AtomicBoolean closed = new AtomicBoolean( false );

    public PeerConnection( PeerAddress destination, ConnectionReservation connectionReservation,
                           ChannelCreator channelCreator, int idleTCPMillis )
    {
        this.destination = destination;
        this.channelCreator = channelCreator;
        this.connectionReservation = connectionReservation;
        this.idleTCPMillis = idleTCPMillis;
    }

    /**
     * Closes and releases the connection from the reservation. Closes the connection in the background.
     */
    public void close()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            ChannelFuture cf = channelCreator.close( getDestination() );
            if ( cf != null )
            {
                // since we did not reset the timeout counter if we receive the data, we need to cancel the timeout here
                Channel channel = cf.getChannel();
                ReplyTimeoutHandler timoutHandler = (ReplyTimeoutHandler) channel.getPipeline().get( "timeout" );
                // abort the old timeouthandler. If we have not dealt with it
                // (should not happen), then abort and throw exception
                timoutHandler.cancel();
                cf.addListener( new ChannelFutureListener()
                {
                    @Override
                    public void operationComplete( ChannelFuture future )
                        throws Exception
                    {
                        connectionReservation.release( channelCreator );
                    }
                } );
            }
            else
            {
                connectionReservation.release( channelCreator );
            }
        }
    }

    /**
     * @return The destination of the permanent connection. There can only be one permanent connection to a destination.
     */
    public PeerAddress getDestination()
    {
        return destination;
    }

    /**
     * @return The channel creator unless the connection is not closed. Otherwise it throws a Runtime Exception
     */
    public ChannelCreator getChannelCreator()
    {
        if ( closed.get() )
        {
            throw new RuntimeException( "cannot used a closed channel" );
        }
        return channelCreator;
    }

    /**
     * @return True if this permanent connection has been closed.F
     */
    public boolean isClosed()
    {
        return closed.get();
    }

    /**
     * @return The number of milliseconds a connection is considered idle and will be closed.
     */
    public int getIdleTCPMillis()
    {
        return idleTCPMillis;
    }

    /**
     * Acquire one connection. Blocks until a connection becomes available.
     * 
     * @throws InterruptedException
     */
    public void aquireSingleConnection()
        throws InterruptedException
    {
        oneConnection.acquire();
    }

    /**
     * Releases one connection.
     */
    public void releaseSingleConnection()
    {
        oneConnection.release();
    }
}