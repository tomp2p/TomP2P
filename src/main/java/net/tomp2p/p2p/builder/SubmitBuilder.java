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

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureTask;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.task.Worker;

public class SubmitBuilder
{
    private final static FutureTask FUTURE_TASK_SHUTDOWN = new FutureTask().setFailed( "Peer is shutting down" );

    private final static Map<Number160, Data> EMPTY_MAP = new HashMap<Number160, Data>();

    private final Number160 locationKey;

    private final Worker worker;

    private final Peer peer;

    //
    private Map<Number160, Data> dataMap;

    private RoutingConfiguration routingConfiguration;

    private RequestP2PConfiguration requestP2PConfiguration;

    private FutureChannelCreator futureChannelCreator;

    private boolean signMessage = false;

    private boolean isManualCleanup = false;

    //
    public SubmitBuilder( Peer peer, Number160 locationKey, Worker worker )
    {
        this.peer = peer;
        this.locationKey = locationKey;
        this.worker = worker;
    }

    public Map<Number160, Data> getDataMap()
    {
        return dataMap;
    }

    public SubmitBuilder setDataMap( Map<Number160, Data> dataMap )
    {
        this.dataMap = dataMap;
        return this;
    }

    public RoutingConfiguration getRoutingConfiguration()
    {
        return routingConfiguration;
    }

    public SubmitBuilder setRoutingConfiguration( RoutingConfiguration routingConfiguration )
    {
        this.routingConfiguration = routingConfiguration;
        return this;
    }

    public RequestP2PConfiguration getRequestP2PConfiguration()
    {
        return requestP2PConfiguration;
    }

    public SubmitBuilder setRequestP2PConfiguration( RequestP2PConfiguration requestP2PConfiguration )
    {
        this.requestP2PConfiguration = requestP2PConfiguration;
        return this;
    }

    public FutureChannelCreator getFutureChannelCreator()
    {
        return futureChannelCreator;
    }

    public SubmitBuilder setFutureChannelCreator( FutureChannelCreator futureChannelCreator )
    {
        this.futureChannelCreator = futureChannelCreator;
        return this;
    }

    public boolean isSignMessage()
    {
        return signMessage;
    }

    public SubmitBuilder setSignMessage( boolean signMessage )
    {
        this.signMessage = signMessage;
        return this;
    }

    public SubmitBuilder signMessage()
    {
        this.signMessage = true;
        return this;
    }

    public boolean isManualCleanup()
    {
        return isManualCleanup;
    }

    public SubmitBuilder setManualCleanup( boolean isManualCleanup )
    {
        this.isManualCleanup = isManualCleanup;
        return this;
    }

    public SubmitBuilder manualCleanup()
    {
        this.isManualCleanup = true;
        return this;
    }

    public FutureTask start()
    {
        if ( peer.isShutdown() )
        {
            return FUTURE_TASK_SHUTDOWN;
        }
        if ( dataMap == null )
        {
            dataMap = EMPTY_MAP;
        }
        if ( routingConfiguration == null )
        {
            routingConfiguration = new RoutingConfiguration( 3, 5, 10, 2 );
        }
        if ( requestP2PConfiguration == null )
        {
            requestP2PConfiguration = new RequestP2PConfiguration( 1, 0, 1 );
        }
        if ( futureChannelCreator == null )
        {
            futureChannelCreator = peer.reserve( routingConfiguration, requestP2PConfiguration, "submit-builder" );
        }
        return peer.getDistributedTask().submit( locationKey, dataMap, worker, routingConfiguration,
                                                 requestP2PConfiguration, futureChannelCreator, signMessage,
                                                 isManualCleanup, peer.getConnectionBean().getConnectionReservation() );
    }
}