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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.p2p.MaintenanceTask;
import net.tomp2p.utils.ConcurrentCacheMap;
import net.tomp2p.utils.Timings;

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

    private final List<Map<Number160, PeerStatatistic>> peerMapVerified;
    private final List<Map<Number160, PeerStatatistic>> peerMapNonVerified;
    private final ConcurrentCacheMap<Number160, PeerAddress> offlineMap;

    /**
     * Creates a new maintenance class with the verified and non verified map.
     * 
     * @param peerMapVerified
     *            The verified map
     * @param peerMapNonVerified
     *            The non-verified map
     * @param offlineMap
     *            The offline map
     * @param peerUrgency
     *            The number of peers that should be in the verified map. If the number is lower, urgency is set to yes
     *            and we are looking for peers in the non verified map
     * @param intervalSeconds
     *            The number of intervals to test a peer. The longer a peer is available the less often we need to check
     * 
     */
    private DefaultMaintenance(final List<Map<Number160, PeerStatatistic>> peerMapVerified,
            final List<Map<Number160, PeerStatatistic>> peerMapNonVerified,
            final ConcurrentCacheMap<Number160, PeerAddress> offlineMap, final int peerUrgency,
            final int[] intervalSeconds) {
        this.peerMapVerified = peerMapVerified;
        this.peerMapNonVerified = peerMapNonVerified;
        this.offlineMap = offlineMap;
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
        this.peerUrgency = peerUrgency;
        this.intervalSeconds = intervalSeconds;
    }

    @Override
    public Maintenance init(final List<Map<Number160, PeerStatatistic>> peerMapVerified,
            final List<Map<Number160, PeerStatatistic>> peerMapNonVerified,
            final ConcurrentCacheMap<Number160, PeerAddress> offlineMap) {
        return new DefaultMaintenance(peerMapVerified, peerMapNonVerified, offlineMap, peerUrgency,
                intervalSeconds);
    }

    /**
     * Finds the next peer that should have a maintenance check. Returns null if no maintenance is needed at the moment.
     * It will return the most important peers first. Importance is as follows: The most important peers are the close
     * ones in the verified peer map. If a certain threshold in a bag is not reached, the unverified becomes important
     * too.
     * 
     * @return The next most important peer to check if its still alive.
     */
    public PeerStatatistic nextForMaintenance(Collection<PeerAddress> notInterestedAddresses) {
        if (peerMapVerified == null || peerMapNonVerified == null || offlineMap == null) {
            throw new IllegalArgumentException("did not initialize this maintenance class");
        }
        int peersBefore = 0;
        for (int i = 0; i < Number160.BITS; i++) {
            final Map<Number160, PeerStatatistic> mapVerified = peerMapVerified.get(i);
            boolean urgent = false;
            synchronized (mapVerified) {
                final int size = mapVerified.size();
                peersBefore += size;
                urgent = isUrgent(i, size, peersBefore);
            }
            if (urgent) {
                final Map<Number160, PeerStatatistic> mapNonVerified = peerMapNonVerified.get(i);
                final PeerStatatistic readyForMaintenance = next(mapNonVerified);
                if (readyForMaintenance != null
                        && !notInterestedAddresses.contains(readyForMaintenance.getPeerAddress())) {
                    LOG.debug("check peer {} from the non verified map",readyForMaintenance.getPeerAddress());
                    return readyForMaintenance;
                }
            }
            final PeerStatatistic readyForMaintenance = next(mapVerified);
            if (readyForMaintenance != null
                    && !notInterestedAddresses.contains(readyForMaintenance.getPeerAddress())) {
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
    private PeerStatatistic next(final Map<Number160, PeerStatatistic> map) {
        synchronized (map) {
            for (PeerStatatistic peerStatatistic : map.values()) {
                if (needMaintenance(peerStatatistic)) {
                    return peerStatatistic;
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
     *            The number of the bagindex. The smaller the index, the more important the peer
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
    protected boolean needMaintenance(final PeerStatatistic peerStatatistic) {
        // go for the time 5, 10, 20, 40, 80, 160
        final int divide = 5;
        final int online = peerStatatistic.onlineTime() / divide;
        final int index;
        if (online <= 0) {
            index = 0;
        } else {
            index = Math.min(intervalSeconds.length - 1, log2(online) + 1);
        }
        final int time = intervalSeconds[index];
        final long lastTimeWhenChecked = Timings.currentTimeMillis() - peerStatatistic.getLastSeenOnline();
        return lastTimeWhenChecked > TimeUnit.SECONDS.toMillis(time);
    }

    /**
     * As seen in: http://stackoverflow.com/questions/3305059/how-do-you-calculate-log-base-2-in-java-for-integers.
     * 
     * @param n
     *            The number
     * @return the logarithm base 2
     */
    public static int log2(final int n) {
        if (n <= 0) {
            throw new IllegalArgumentException();
        }
        return (Integer.SIZE - 1) - Integer.numberOfLeadingZeros(n);
    }

}
