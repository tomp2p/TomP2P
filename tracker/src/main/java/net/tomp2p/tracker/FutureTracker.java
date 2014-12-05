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
package net.tomp2p.tracker;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.TrackerData;
import net.tomp2p.p2p.EvaluatingSchemeTracker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

/**
 * This class holds the object for future results from the tracker get() and
 * add(). FutureTracker can fail, if the search did not return any results.
 * 
 * @author Thomas Bocek
 */
public class FutureTracker extends BaseFutureImpl<FutureTracker> {
    // since we receive results from multiple peers, we need to summarize them
    final private EvaluatingSchemeTracker evaluatingSchemeTracker;

    // a set of know peers that we don't want in the result set.
    final private Set<Number160> knownPeers;

    // results
    private Set<PeerAddress> potentialTrackers;

    private Set<PeerAddress> directTrackers;

    private Map<PeerAddress, TrackerData> peersOnTracker;
    
    private FutureDone<Void> futureDone;

    public FutureTracker() {
        this(null, null);
    }

    /**
     * Sets all the values for this future object.
     * 
     * @param evaluatingSchemeTracker
     *            Since we receive results from multiple peers, we need to
     *            summarize them
     * @param knownPeers
     *            A set of know peers that we don't want in the result set.
     * @param futureCreate
     *            Keeps track of futures that are based on this future
     */
    public FutureTracker(EvaluatingSchemeTracker evaluatingSchemeTracker, Set<Number160> knownPeers) {
        this.evaluatingSchemeTracker = evaluatingSchemeTracker;
        this.knownPeers = knownPeers;
        self(this);
    }

    /**
     * Set the result of the tracker process.
     * 
     * @param potentialTrackers
     *            The trackers that are close to the key, also containing the
     *            direct trackers.
     * @param directTrackers
     *            Those peers that are close and reported to have the key.
     * @param peersOnTracker
     *            The data from the trackers.
     * @param futureDone 
     */
    public void trackers(Set<PeerAddress> potentialTrackers, Set<PeerAddress> directTrackers,
            Map<PeerAddress, TrackerData> peersOnTracker, FutureDone<Void> futureDone) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return;
            }
            this.potentialTrackers = potentialTrackers;
            this.directTrackers = directTrackers;
            this.peersOnTracker = peersOnTracker;
            this.futureDone = futureDone;
            this.type = ((potentialTrackers.size() == 0) && (directTrackers.size() == 0)) ? BaseFuture.FutureType.FAILED
                    : BaseFuture.FutureType.OK;
            if (this.type == BaseFuture.FutureType.FAILED) {
                this.reason = "we did not find anything, are you sure you are serching for the right tracker?";
            }
        }
        notifyListeners();
    }

    /**
     * @return The trackers that are close to the key, also containing the
     *         direct trackers.
     */
    public Set<PeerAddress> potentialTrackers() {
        synchronized (lock) {
            return potentialTrackers;
        }
    }

    /**
     * @return Those peers that are close and reported to have the key.
     */
    public Set<PeerAddress> directTrackers() {
        synchronized (lock) {
            return directTrackers;
        }
    }

    /**
     * @return the raw data, which means all the data the trackers reported.
     */
    public Map<PeerAddress, TrackerData> rawPeersOnTracker() {
        synchronized (lock) {
            return peersOnTracker;
        }
    }

    /**
     * @return The peer address that send back data.
     */
    public Set<PeerAddress> peersOnTracker() {
        synchronized (lock) {
            return peersOnTracker.keySet();
        }
    }

    /**
     * @return The list of peers which we already have in our result set.
     */
    public Set<Number160> knownPeers() {
        synchronized (lock) {
            return knownPeers;
        }
    }

    /**
     * Evaluates the data from the trackers. Since we receive multiple results,
     * we evaluate them before we give the data to the user. If the user wants
     * to see the raw data, use {@link #rawPeersOnTracker()}.
     * 
     * @return The data from the trackers.
     */
    public Collection<TrackerData> trackers() {
        synchronized (lock) {
            return evaluatingSchemeTracker.evaluateSingle(peersOnTracker);
        }
    }
    
    public FutureDone<Void> futuresCompleted() {
    	synchronized (lock) {
    		return futureDone;
    	}
    }
}
