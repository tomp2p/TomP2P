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
package net.tomp2p.message;

import java.util.Iterator;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.storage.Data;

public class TrackerData {

    public final static Data EMTPY_DATA = new Data(0, 0);

    private final Map<PeerStatatistic, Data> peerAddresses;

    final private boolean couldProvideMoreData;

    public TrackerData(Map<PeerStatatistic, Data> peerAddresses) {
        this(peerAddresses, false);
    }

    public TrackerData(Map<PeerStatatistic, Data> peerAddresses, boolean couldProvideMoreData) {
        this.peerAddresses = peerAddresses;
        this.couldProvideMoreData = couldProvideMoreData;
    }

    public Map<PeerStatatistic, Data> peerAddresses() {
        return peerAddresses;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("p:").append(peerAddresses).append(",l:");
        return sb.toString();
    }

    public boolean couldProvideMoreData() {
        return couldProvideMoreData;
    }

    public int size() {
        return peerAddresses.size();
    }

    public void put(PeerStatatistic remotePeer, Data attachement) {
        peerAddresses.put(remotePeer, attachement == null ? EMTPY_DATA : attachement);
    }

    public Map.Entry<PeerStatatistic, Data> remove(Number160 remotePeerId) {
        for (Iterator<Map.Entry<PeerStatatistic, Data>> iterator = peerAddresses.entrySet().iterator(); iterator
                .hasNext();) {
            Map.Entry<PeerStatatistic, Data> entry = iterator.next();
            if (entry.getKey().peerAddress().peerId().equals(remotePeerId)) {
                iterator.remove();
                return entry;
            }
        }
        return null;
    }

    public boolean containsKey(Number160 tmpKey) {
        for (Iterator<Map.Entry<PeerStatatistic, Data>> iterator = peerAddresses.entrySet().iterator(); iterator
                .hasNext();) {
            Map.Entry<PeerStatatistic, Data> entry = iterator.next();
            if (entry.getKey().peerAddress().peerId().equals(tmpKey)) {
                return true;
            }
        }
        return false;
    }
    
    public Map.Entry<PeerStatatistic, Data> get(Number160 tmpKey) {
        for (Iterator<Map.Entry<PeerStatatistic, Data>> iterator = peerAddresses.entrySet().iterator(); iterator
                .hasNext();) {
            Map.Entry<PeerStatatistic, Data> entry = iterator.next();
            if (entry.getKey().peerAddress().peerId().equals(tmpKey)) {
                return entry;
            }
        }
        return null;
    }
}
