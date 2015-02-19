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
import java.util.concurrent.TimeUnit;

import net.tomp2p.utils.ConcurrentCacheMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default maintenance implementation.
 * 
 * @author Thomas Bocek
 * 
 */
public class DefaultMaintenance implements Maintenance {
    
    private static final Logger LOG = LoggerFactory.getLogger(DefaultMaintenance.class);

    private final int peerUrgency;
    private final int[] intervalSeconds;

    private final List<Map<Number160, PeerStatistic>> peerMapVerified;
    private final List<Map<Number160, PeerStatistic>> peerMapNonVerified;
    private final ConcurrentCacheMap<Number160, PeerAddress> offlineMap;
    private final ConcurrentCacheMap<Number160, PeerAddress> shutdownMap;
    private final ConcurrentCacheMap<Number160, PeerAddress> exceptionMap;

    /**
     * Creates a new maintenance class with the verified and non verified map.
     * 
     * @param peerMapVerified
     *            The verified map
     * @param peerMapNonVerified
     *            The non-verified map
     * @param offlineMap
     *            The offline map
     * @param exceptionMap 
     * @param shutdownMap 
     * @param peerUrgency
     *            The number of peers that should be in the verified map. If the number is lower, urgency is set to yes
     *            and we are looking for peers in the non verified map
     * @param intervalSeconds
     *            The number of intervals to test a peer. The longer a peer is available the less often we need to check
     * 
     */
    private DefaultMaintenance(final List<Map<Number160, PeerStatistic>> peerMapVerified,
            final List<Map<Number160, PeerStatistic>> peerMapNonVerified,
            final ConcurrentCacheMap<Number160, PeerAddress> offlineMap, 
            final ConcurrentCacheMap<Number160, PeerAddress> shutdownMap, 
            final ConcurrentCacheMap<Number160, PeerAddress> exceptionMap, final int peerUrgency,
            final int[] intervalSeconds) {
        this.peerMapVerified = peerMapVerified;
        this.peerMapNonVerified = peerMapNonVerified;
        this.offlineMap = offlineMap;
        this.shutdownMap = shutdownMap;
        this.exceptionMap = exceptionMap;
        this.peerUrgency = peerUrgency;
        this.intervalSeconds = intervalSeconds;
    }

    /**
     * Constructor that initializes the maps as null references. To use this class init must be called that creates a
     * new class with the private constructor.
     * 
     * @param peerUrgency
     *            The number of peers that should be in the verified map. If the number is lower, urgency is set to yes
     *            and we are looking for peers in the non verified map
     * @param intervalSeconds
     *            The number of intervals to test a peer. The longer a peer is available the less often we need to check
     */
    public DefaultMaintenance(final int peerUrgency, final int[] intervalSeconds) {
        this.peerMapVerified = null;
        this.peerMapNonVerified = null;
        this.offlineMap = null;
        this.shutdownMap = null;
        this.exceptionMap = null;
        this.peerUrgency = peerUrgency;
        this.intervalSeconds = intervalSeconds;
    }

    @Override
    public Maintenance init(final List<Map<Number160, PeerStatistic>> peerMapVerified,
            final List<Map<Number160, PeerStatistic>> peerMapNonVerified,
            final ConcurrentCacheMap<Number160, PeerAddress> offlineMap, 
            final ConcurrentCacheMap<Number160, PeerAddress> shutdownMap, 
            final ConcurrentCacheMap<Number160, PeerAddress> exceptionMap) {
        return new DefaultMaintenance(peerMapVerified, peerMapNonVerified, offlineMap, shutdownMap, exceptionMap, peerUrgency,
                intervalSeconds);
    }

    /**
     * Finds the next peer that should have a maintenance check. Returns null if no maintenance is needed at the moment.
     * It will return the most important peers first. Importance is as follows: The most important peers are the close
     * ones in the verified peer map. If a certain threshold in a bag is not reached, the unverified becomes important
     * too.
     * 
     * @return The next most important peer to check if it is still alive.
     */
    public PeerStatistic nextForMaintenance(Collection<PeerAddress> notInterestedAddresses) {
        if (peerMapVerified == null || peerMapNonVerified == null || offlineMap == null 
                || shutdownMap == null || exceptionMap == null) {
            throw new IllegalArgumentException("Did not initialize some of the maintenance maps.");
        }
        int peersBefore = 0;
        for (int i = 0; i < Number160.BITS; i++) {
            final Map<Number160, PeerStatistic> mapVerified = peerMapVerified.get(i);
            boolean urgent = false;
            synchronized (mapVerified) {
                final int size = mapVerified.size();
                peersBefore += size;
                urgent = isUrgent(i, size, peersBefore);
            }
            if (urgent) {
                final Map<Number160, PeerStatistic> mapNonVerified = peerMapNonVerified.get(i);
                final PeerStatistic readyForMaintenance = next(mapNonVerified);
                if (readyForMaintenance != null
                        && !notInterestedAddresses.contains(readyForMaintenance.peerAddress())) {
                    LOG.debug("check peer {} from the non-verified map.", readyForMaintenance.peerAddress());
                    return readyForMaintenance;
                }
            }
            final PeerStatistic readyForMaintenance = next(mapVerified);
            if (readyForMaintenance != null
                    && !notInterestedAddresses.contains(readyForMaintenance.peerAddress())) {
                return readyForMaintenance;
            }
        }
        return null;
    }

    /**
     * Returns a peer with its statistics from a bag that needs maintenance.
     * 
     * @param map
     *            The bag with all the peers
     * @return A peer that needs maintenance
     */
    private PeerStatistic next(final Map<Number160, PeerStatistic> map) {
        synchronized (map) {
            for (PeerStatistic peerStatistic : map.values()) {
                if (needMaintenance(peerStatistic, intervalSeconds)) {
                    return peerStatistic;
                }
            }
        }
        return null;
    }

    /**
     * Indicates if it is urgent to search for a peer. This means that we have not enough peers in the verified map and
     * we need to get one from the non-verified map.
     * 
     * @param bagIndex
     *            The number of the bag index. The smaller the index, the more important the peer
     * @param bagSize
     *            The size of the current bag
     * @param peersBefore
     *            The number of peers we have that are smaller than in this bag index
     * @return True, if we need urgently a peer from the non-verified map
     */
    protected boolean isUrgent(final int bagIndex, final int bagSize, final int peersBefore) {
        return bagSize < peerUrgency;
    }

    /**
     * Indicates if a peer needs a maintenance check.
     * 
     * @param peerStatatistic
     *            The peer with its statistics
     * @return True if the peer needs a maintenance check
     */
    public static boolean needMaintenance(final PeerStatistic peerStatistic, final int[] intervalSeconds) {
        final int onlineSec = peerStatistic.onlineTime() / 1000;
        int index;
        if (onlineSec <= 0) {
            index = 0;
        } else {
        	index = intervalSeconds.length - 1;
        	for(int i=0;i<intervalSeconds.length;i++) {
        		//interval is 2,4,8,16,32,64
        		//examples
        		//I have seen a peer online for 5 sec -> next interval to check is 8
        		//I have seen a peer online for 4 sec -> next interval to check is 4
        		//I have seen a peer online for 17 sec -> next interval to check is 32
        		//I have seen a peer online for 112321 sec -> next interval to check is 64
        		if(intervalSeconds[i]>=onlineSec) {
        			index=i;
        			break;
        		}
        	}
        }
        final int time = intervalSeconds[index];
        final long lastTimeWhenChecked = System.currentTimeMillis() - peerStatistic.lastSeenOnline();
        return lastTimeWhenChecked > TimeUnit.SECONDS.toMillis(time);
    }
}
