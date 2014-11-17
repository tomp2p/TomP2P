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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Pair;

public class TrackerData {

    private final static Data EMTPY_DATA = new Data(0, 0);

    private final Map<PeerAddress, Data> peerAddresses;

    final private boolean couldProvideMoreData;

    public TrackerData(Map<PeerAddress, Data> peerAddresses) {
        this(peerAddresses, false);
    }

    public TrackerData(Map<PeerAddress, Data> peerAddresses, boolean couldProvideMoreData) {
        this.peerAddresses = peerAddresses;
        this.couldProvideMoreData = couldProvideMoreData;
    }

    public TrackerData(Collection<Pair<PeerStatistic, Data>> value) {
    	this.peerAddresses = new HashMap<PeerAddress, Data>(value.size());
	    for(Pair<PeerStatistic, Data> pair:value) {
	    	peerAddresses.put(pair.element0().peerAddress(), pair.element1());
	    }
	    this.couldProvideMoreData = false;
    }

	public Map<PeerAddress, Data> peerAddresses() {
    	if(peerAddresses == null) {
    		return Collections.<PeerAddress, Data>emptyMap();
    	}
        return peerAddresses;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("tdata:");
        if(peerAddresses != null) {
        	sb.append("p:").append(peerAddresses);
        }
        return sb.toString();
    }

    public boolean couldProvideMoreData() {
        return couldProvideMoreData;
    }

    public int size() {
        return peerAddresses.size();
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
    
    public Map.Entry<PeerAddress, Data> get(Number160 tmpKey) {
        for (Iterator<Map.Entry<PeerAddress, Data>> iterator = peerAddresses.entrySet().iterator(); iterator
                .hasNext();) {
            Map.Entry<PeerAddress, Data> entry = iterator.next();
            if (entry.getKey().peerId().equals(tmpKey)) {
                return entry;
            }
        }
        return null;
    }

	public boolean isEmpty() {
	    return peerAddresses.isEmpty();
    }
}
