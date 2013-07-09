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
package net.tomp2p.peers;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.utils.Timings;

public class PeerStatatistics {
    private final Map<PeerAddress, PeerStatatistic> mapStat = new HashMap<PeerAddress, PeerStatatistic>();

    public void removeStat(PeerAddress peerAddress) {
        synchronized (mapStat) {
            mapStat.remove(peerAddress);
        }
    }

    public void setSeenOnlineTime(PeerAddress peerAddress) {
        PeerStatatistic peerStat = getOrCreate(peerAddress);
        peerStat.setLastSeenOnline(Timings.currentTimeMillis());
    }

    public long getLastSeenOnlineTime(PeerAddress peerAddress) {
        PeerStatatistic peerStat = getOrCreate(peerAddress);
        return peerStat.getLastSeenOnline();
    }

    public void incChecked(PeerAddress peerAddress) {
        PeerStatatistic peerStat = getOrCreate(peerAddress);
        peerStat.successfullyChecked();
    }

    private PeerStatatistic getOrCreate(PeerAddress peerAddress) {
        PeerStatatistic peerStat;
        synchronized (mapStat) {
            peerStat = mapStat.get(peerAddress);
            if (peerStat == null) {
                peerStat = new PeerStatatistic();
                mapStat.put(peerAddress, peerStat);
            }
        }
        return peerStat;
    }

    public int getChecked(PeerAddress peerAddress) {
        PeerStatatistic peerStat;
        synchronized (mapStat) {
            peerStat = mapStat.get(peerAddress);
        }
        if (peerStat == null)
            return 0;
        return peerStat.getSuccessfullyChecked();
    }

    public long online(PeerAddress peerAddress) {
        PeerStatatistic peerStat = getOrCreate(peerAddress);
        return peerStat.onlineTime();
    }
    
    public void addRTT(PeerAddress peerAddress, long rtt) {
        PeerStatatistic peerStat = getOrCreate(peerAddress);
        peerStat.addRTT(rtt);
    }
    
    public long getMeanRTT(PeerAddress peerAddress) {
        PeerStatatistic peerStat;
        synchronized (mapStat) {
            peerStat = mapStat.get(peerAddress);
        }
        if (peerStat == null) {
            return 3*1000;
        }
        return peerStat.getMeanRTT();
    }
}