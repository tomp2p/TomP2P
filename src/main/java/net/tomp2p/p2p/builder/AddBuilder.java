/*
 * Copyright 2012 Thomas Bocek
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

package net.tomp2p.p2p.builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureCreator;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class AddBuilder
    extends DHTBuilder<AddBuilder>
{
    private Collection<Data> dataSet;

    private Data data;

    private boolean list = false;

    public AddBuilder( Peer peer, Number160 locationKey )
    {
        super( peer, locationKey );
        self( this );
    }

    public Collection<Data> getDataSet()
    {
        return dataSet;
    }

    public AddBuilder setDataSet( Collection<Data> dataSet )
    {
        this.dataSet = dataSet;
        return this;
    }

    public Data getData()
    {
        return data;
    }

    public AddBuilder setData( Data data )
    {
        this.data = data;
        return this;
    }

    public AddBuilder setObject( Object object )
        throws IOException
    {
        return setData( new Data( object ) );
    }

    public boolean isList()
    {
        return list;
    }

    public AddBuilder setList( boolean list )
    {
        this.list = list;
        return this;
    }

    public AddBuilder setList()
    {
        this.list = true;
        return this;
    }

    @Override
    public FutureDHT start()
    {
        if ( peer.isShutdown() )
        {
            return FUTURE_DHT_SHUTDOWN;
        }
        preBuild( "add-builder" );
        if ( dataSet == null )
        {
            dataSet = new ArrayList<Data>( 1 );
        }
        if ( data != null )
        {
            dataSet.add( data );
        }
        if ( dataSet.size() == 0 )
        {
            throw new IllegalArgumentException(
                                                "You must either set data via setDataMap() or setData(). Cannot add nothing." );
        }

        final FutureDHT futureDHT =
            peer.getDistributedHashMap().add( locationKey, domainKey, dataSet, routingConfiguration,
                                              requestP2PConfiguration, protectDomain, signMessage, manualCleanup, list,
                                              futureCreate, futureChannelCreator,
                                              peer.getConnectionBean().getConnectionReservation() );
        if ( directReplication )
        {
            if ( defaultDirectReplication == null )
            {
                defaultDirectReplication = new DefaultDirectReplication();
            }
            Runnable runner = new Runnable()
            {
                @Override
                public void run()
                {
                    FutureDHT futureDHTReplication = defaultDirectReplication.create();
                    futureDHT.repeated( futureDHTReplication );
                }
            };
            ScheduledFuture<?> tmp =
                peer.getConnectionBean().getScheduler().getScheduledExecutorServiceReplication().scheduleAtFixedRate( runner,
                                                                                                                      refreshSeconds,
                                                                                                                      refreshSeconds,
                                                                                                                      TimeUnit.SECONDS );
            setupCancel( futureDHT, tmp );
        }
        return futureDHT;
    }

    private class DefaultDirectReplication
        implements FutureCreator<FutureDHT>
    {
        @Override
        public FutureDHT create()
        {
            final FutureChannelCreator futureChannelCreator =
                peer.reserve( routingConfiguration, requestP2PConfiguration, "submit-builder-direct-replication" );
            FutureDHT futureDHT =
                peer.getDistributedHashMap().add( locationKey, domainKey, dataSet, routingConfiguration,
                                                  requestP2PConfiguration, protectDomain, signMessage, manualCleanup,
                                                  list, futureCreate, futureChannelCreator,
                                                  peer.getConnectionBean().getConnectionReservation() );
            return futureDHT;
        }
    }
}