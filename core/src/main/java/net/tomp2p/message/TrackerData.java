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
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class TrackerData {

    public final static Data EMTPY_DATA = new Data(0, 0);

    private final Map<PeerAddress, Data> peerAddresses;

    final private PeerAddress referrer;

    final private boolean couldProvideMoreData;

    public TrackerData(Map<PeerAddress, Data> peerAddresses, PeerAddress referrer) {
        this(peerAddresses, referrer, false);
    }

    public TrackerData(Map<PeerAddress, Data> peerAddresses, PeerAddress referrer,
            boolean couldProvideMoreData) {
        this.peerAddresses = peerAddresses;
        this.referrer = referrer;
        this.couldProvideMoreData = couldProvideMoreData;
    }

    public Map<PeerAddress, Data> peerAddresses() {
        return peerAddresses;
    }

    public PeerAddress referrer() {
        return referrer;
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

    public Map<PeerAddress, Data> map() {
        return peerAddresses;
    }

    public void put(PeerAddress remotePeer, Data attachement) {
        peerAddresses.put(remotePeer, attachement == null ? EMTPY_DATA : attachement);
    }

    public Map.Entry<PeerAddress, Data> remove(Number160 remotePeerId) {
        for (Iterator<Map.Entry<PeerAddress, Data>> iterator = peerAddresses.entrySet().iterator(); iterator
                .hasNext();) {
            Map.Entry<PeerAddress, Data> entry = iterator.next();
            if (entry.getKey().peerId().equals(remotePeerId)) {
                iterator.remove();
                return entry;
            }
        }
        return null;
    }

    public boolean containsKey(Number160 tmpKey) {
        for (Iterator<Map.Entry<PeerAddress, Data>> iterator = peerAddresses.entrySet().iterator(); iterator
                .hasNext();) {
            Map.Entry<PeerAddress, Data> entry = iterator.next();
            if (entry.getKey().peerId().equals(tmpKey)) {
                return true;
            }
        }
        return false;
    }
}
