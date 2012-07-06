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

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class BroadcastBuilder
{
    final private Peer peer;

    final private Number160 messageKey;

    private Map<Number160, Data> dataMap;

    private Boolean isUDP;

    public BroadcastBuilder( Peer peer, Number160 messageKey )
    {
        this.peer = peer;
        this.messageKey = messageKey;
    }

    public void start()
    {
        if ( isUDP == null )
        {
            // not set, decide based on the data
            if ( getDataMap() == null )
            {
                setIsUDP( true );
            }
            else
            {
                setIsUDP( false );
            }
        }
        if ( dataMap == null )
        {
            setDataMap( new HashMap<Number160, Data>() );
        }
        peer.getBroadcastRPC().getBroadcastHandler().receive( messageKey, dataMap, 0, isUDP );
    }

    public Map<Number160, Data> getDataMap()
    {
        return dataMap;
    }

    public BroadcastBuilder setDataMap( Map<Number160, Data> dataMap )
    {
        this.dataMap = dataMap;
        return this;
    }

    public boolean isUDP()
    {
        if ( isUDP == null )
        {
            return false;
        }
        return isUDP;
    }

    public BroadcastBuilder setIsUDP( boolean isUDP )
    {
        this.isUDP = isUDP;
        return this;
    }
}