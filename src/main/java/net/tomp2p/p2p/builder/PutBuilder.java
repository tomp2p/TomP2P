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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureCreator;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class PutBuilder
    extends DHTBuilder<PutBuilder>
{
    private Entry<Number160, Data> data;

    private Map<Number160, Data> dataMap;

    //
    private boolean putIfAbsent = false;

    public PutBuilder( Peer peer, Number160 locationKey )
    {
        super( peer, locationKey );
        self( this );
    }

    public Entry<Number160, Data> getData()
    {
        return data;
    }

    public PutBuilder setData( final Data data )
    {
        return setData( Number160.ZERO, data );
    }

    public PutBuilder setData( final Number160 key, final Data data )
    {
        this.data = new Entry<Number160, Data>()
        {
            @Override
            public Data setValue( Data value )
            {
                return null;
            }

            @Override
            public Data getValue()
            {
                return data;
            }

            @Override
            public Number160 getKey()
            {
                return key;
            }
        };
        return this;
    }

    public PutBuilder setObject( Data data )
        throws IOException
    {
        return setData( new Data( data ) );
    }

    public PutBuilder setKeyObject( Number160 contentKey, Data data )
        throws IOException
    {
        return setData( contentKey, new Data( data ) );
    }

    public Map<Number160, Data> getDataMap()
    {
        return dataMap;
    }

    public PutBuilder setDataMap( Map<Number160, Data> dataMap )
    {
        this.dataMap = dataMap;
        return this;
    }

    public boolean isPutIfAbsent()
    {
        return putIfAbsent;
    }

    public PutBuilder setPutIfAbsent( boolean putIfAbsent )
    {
        this.putIfAbsent = putIfAbsent;
        return this;
    }

    public PutBuilder setPutIfAbsent()
    {
        this.putIfAbsent = true;
        return this;
    }

    @Override
    public FutureDHT start()
    {
        if ( peer.isShutdown() )
        {
            return FUTURE_DHT_SHUTDOWN;
        }
        preBuild( "put-builder" );
        if ( dataMap == null )
        {
            setDataMap( new HashMap<Number160, Data>( 1 ) );
        }
        if ( data != null )
        {
            getDataMap().put( getData().getKey(), getData().getValue() );
        }
        if ( dataMap.size() == 0 )
        {
            throw new IllegalArgumentException(
                                                "You must either set data via setDataMap() or setData(). Cannot add nothing." );
        }
        final FutureDHT futureDHT =
            peer.getDistributedHashMap().put( locationKey, domainKey, dataMap, routingConfiguration,
                                              requestP2PConfiguration, putIfAbsent, protectDomain, signMessage,
                                              manualCleanup, futureCreate, futureChannelCreator,
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
                peer.getDistributedHashMap().put( locationKey, domainKey, dataMap, routingConfiguration,
                                                  requestP2PConfiguration, putIfAbsent, protectDomain, signMessage,
                                                  manualCleanup, futureCreate, futureChannelCreator,
                                                  peer.getConnectionBean().getConnectionReservation() );
            return futureDHT;
        }
    }
}
