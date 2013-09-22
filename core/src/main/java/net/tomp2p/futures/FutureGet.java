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
package net.tomp2p.futures;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.tomp2p.p2p.EvaluatingSchemeDHT;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.storage.Data;

/**
 * The future object for put() operations including routing.
 * 
 * @author Thomas Bocek
 */
public class FutureGet extends FutureDHT<FutureGet> {
    // The minimum number of expected results. This is also used for put()
    // operations to decide if a future failed or not.
    private final int min;

    // Since we receive multiple results, we have an evaluation scheme to
    // simplify the result
    private final EvaluatingSchemeDHT evaluationScheme;

    // A pointer to the routing process that run before the DHT operations
    private FutureRouting futureRouting;

    private final List<Cancel> cleanup = new ArrayList<Cancel>(1);

    // Storage of results
    private Map<PeerAddress, Map<Number480, Data>> rawData;
    private Map<PeerAddress, DigestResult> rawDigest;

    private Number160 locationKey;

    private Number160 domainKey;

    // Flag indicating if the minimum operations for put have been reached.
    private boolean minReached;

    /**
     * Default constructor.
     */
    public FutureGet() {
        this(0, new VotingSchemeDHT());
    }

    /**
     * Creates a new DHT future object that keeps track of the status of the DHT operations.
     * 
     * @param min
     *            The minimum of expected results
     * @param evaluationScheme
     *            The scheme to evaluate results from multiple peers
     */
    public FutureGet(final int min, final EvaluatingSchemeDHT evaluationScheme) {
        this.min = min;
        this.evaluationScheme = evaluationScheme;
        self(this);
    }

    /**
     * Finish the future and set the keys and data that have been received.
     * 
     * @param domainKey
     *            The domain key
     * @param locationKey
     *            The location key
     * @param rawData
     *            The keys and data that have been received with information from which peer it has been received.
     */
    public void setReceivedData(final Number160 locationKey, final Number160 domainKey,
            final Map<PeerAddress, Map<Number480, Data>> rawData) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.locationKey = locationKey;
            this.domainKey = domainKey;
            this.rawData = rawData;
            final int size = rawData.size();
            this.minReached = size >= min;
            this.type = size > 0 ? FutureType.OK : FutureType.FAILED;
            this.reason = size > 0 ? "Minimun number of results reached" : "Expected >0 result, but got " + size;
        }
        notifyListerenrs();
    }
    
    /**
     * Finishes the future and set the digest information that have been received.
     * 
     * @param domainKey
     *            The domain key
     * @param locationKey
     *            The location key
     * @param rawDigest
     *            The hashes of the content stored with information from which peer it has been received.
     */
    public void setReceivedDigest(final Number160 locationKey, final Number160 domainKey,
            final Map<PeerAddress, DigestResult> rawDigest) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.locationKey = locationKey;
            this.domainKey = domainKey;
            this.rawDigest = rawDigest;
            final int size = rawDigest.size();
            this.minReached = size >= min;
            this.type = size > 0 ? FutureType.OK : FutureType.FAILED;
            this.reason = size > 0 ? "Minimun number of results reached" : "Expected >0 result, but got " + size;
        }
        notifyListerenrs();
    }

    /**
     * Returns the raw data from the get operation.
     * 
     * @return The raw data and the information which peer has been contacted
     */
    public Map<PeerAddress, Map<Number480, Data>> getRawData() {
        synchronized (lock) {
            return rawData;
        }
    }
    
    /**
     * @return The raw digest information with hashes of the content and the information which peer has been contacted
     */
    public Map<PeerAddress, DigestResult> getRawDigest() {
        synchronized (lock) {
            return rawDigest;
        }
    }

    /**
     * Return the data from get() after evaluation. The evaluation gets rid of the PeerAddress information, by either a
     * majority vote or cumulation.
     * 
     * @return The evaluated data that have been received.
     */
    public Map<Number480, Data> getDataMap() {
        synchronized (lock) {
            return evaluationScheme.evaluate2(rawData);
        }
    }
    
    /**
     * Return the digest information from the get() after evaluation. The evaluation gets rid of the PeerAddress
     * information, by either a majority vote or cumulation.
     * 
     * @return The evaluated digest information that have been received.
     */
    public DigestResult getDigest() {
        synchronized (lock) {
            return evaluationScheme.evaluate5(rawDigest);
        }
    }
    
    /**
     * @return The first data object from get() after evaluation.
     */
    public Data getData() {
        Map<Number480, Data> dataMap = getDataMap();
        if (dataMap.size() == 0) {
            return null;
        }
        return dataMap.values().iterator().next();
    }

    /**
     * Checks if the minimum of expected results have been reached. This flag is also used for determining the success
     * or failure of this future for put and send_direct.
     * 
     * @return True, if expected minimum results have been reached.
     */
    public boolean isMinReached() {
        synchronized (lock) {
            return minReached;
        }
    }

    /**
     * @return The location key used for this future request
     */
    public Number160 getLocationKey() {
        synchronized (lock) {
            return locationKey;
        }
    }

    /**
     * 
     * @return The domain key used for this future request
     */
    public Number160 getDomainKey() {
        synchronized (lock) {
            return domainKey;
        }
    }

    /**
     * Returns the future object that was used for the routing. Before the FutureDHT is used, FutureRouting has to be
     * completed successfully.
     * 
     * @return The future object during the previous routing, or null if routing failed completely.
     */
    public FutureRouting getFutureRouting() {
        synchronized (lock) {
            return futureRouting;
        }
    }

    /**
     * Sets the future object that was used for the routing. Before the FutureDHT is used, FutureRouting has to be
     * completed successfully.
     * 
     * @param futureRouting
     *            The future object to set
     */
    public void setFutureRouting(final FutureRouting futureRouting) {
        synchronized (lock) {
            this.futureRouting = futureRouting;
        }
    }

    

    /**
     * Add cancel operations. These operations are called when a future is done, and we want to cancel all pending
     * operations.
     * 
     * @param cancellable
     *            The operation that can be canceled.
     */
    public void addCleanup(final Cancel cancellable) {
        synchronized (lock) {
            cleanup.add(cancellable);
        }
    }

    /**
     * Shutdown cancels all pending futures.
     */
    public void shutdown() {
        // Even though, this future is completed, there may be tasks than can be
        // canceled due to scheduled futures attached to this event.
        synchronized (lock) {
            for (final Cancel cancellable : cleanup) {
                cancellable.cancel();
            }
        }
    }
}
