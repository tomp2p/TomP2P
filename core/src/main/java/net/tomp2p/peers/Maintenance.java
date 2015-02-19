/*
 * Copyright 2013 Thomas Bocek
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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.tomp2p.utils.ConcurrentCacheMap;

/**
 * Maintenance is important in an iterative P2P network. Thus we need to identify the important peers and start polling
 * them.
 * 
 * @author Thomas Bocek
 * 
 */
public interface Maintenance {

    /**
     * Initializes the maintenance class. This may result in a new class
     * 
     * @param peerMapVerified
     *            The map with the bags of verified peers
     * @param peerMapNonVerified
     *            The map with the bags of non verified peers
     * @param offlineMap
     *            The map with the offline peers
     * @param shutdownMap The map with the peers that quit friendly
     * @param exceptionMap The map with the peers that caused an exception
     * @return The same or a new maintenance class
     */
    Maintenance init(List<Map<Number160, PeerStatistic>> peerMapVerified,
            List<Map<Number160, PeerStatistic>> peerMapNonVerified,
            ConcurrentCacheMap<Number160, PeerAddress> offlineMap, 
            ConcurrentCacheMap<Number160, PeerAddress> shutdownMap, ConcurrentCacheMap<Number160, PeerAddress> exceptionMap);

    /**
     * @return The next peer that needs maintenance or null if no maintenance is needed
     */
    PeerStatistic nextForMaintenance(Collection<PeerAddress> notInterestedAddresses);

}
