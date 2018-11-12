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

import java.util.NavigableMap;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number256;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class BroadcastBuilder extends DefaultConnectionConfiguration {

    private final Peer peer;
    
    private final Number256 messageKey;

    //private NavigableMap<Number640, Data> dataMap;

    
    private int hopCounter;

    public BroadcastBuilder(Peer peer, Number256 messageKey) {
        this.peer = peer;
        this.messageKey = messageKey;
    }

    public void start() {
        Message message = new Message();
        //TODO: needs SCTP mayby
        //message.setDataMap(new DataMap(dataMap));
        


        peer.broadcastRPC().broadcastHandler().receive(message);
    }
    
    public Number256 messageKey() {
        return messageKey;
    }

    /*public NavigableMap<Number640, Data> dataMap() {
        return dataMap;
    }*/

    /*public BroadcastBuilder dataMap(NavigableMap<Number640, Data> dataMap) {
        this.dataMap = dataMap;
        return this;
    }*/
    
    public int hopCounter() {
        return hopCounter;
    }
    
    public BroadcastBuilder hopCounter(int hopCounter) {
        this.hopCounter = hopCounter;
        return this;
    }

    public PeerAddress remotePeer() {
        return peer.peerAddress();
    }
}
