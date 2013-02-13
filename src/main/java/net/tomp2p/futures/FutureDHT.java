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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.tomp2p.p2p.EvaluatingSchemeDHT;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * The future object for the get() and put() operations including routing.
 * 
 * @author Thomas Bocek
 */
public class FutureDHT extends BaseFutureImpl<FutureDHT> implements FutureCleanup {
    // The minimum number of expected results. This is also used for put()
    // operations to decide if a future failed or not.
    final private int min;

    // Since we receive multiple results, we have an evaluation scheme to
    // simplify the result
    final private EvaluatingSchemeDHT evaluationScheme;

    // This is a pointer to the other futures created based on this one.
    final private FutureCreate<FutureDHT> futureCreate;

    // A pointer to the routing process that run before the DHT operations
    private FutureRouting futureRouting;

    // Stores futures of DHT operations, 6 is the maximum of futures being
    // generates as seen in Configurations (min.res + parr.diff)
    final private List<FutureResponse> requests = new ArrayList<FutureResponse>(6);

    final private List<Cancellable> cleanup = new ArrayList<Cancellable>(1);

    // Storage of results
    private Map<PeerAddress, Collection<Number160>> rawKeys;

    private Map<PeerAddress, Collection<Number480>> rawKeys480;

    private Number160 locationKey;

    private Number160 domainKey;

    private Map<PeerAddress, Map<Number160, Data>> rawData;

    private Map<PeerAddress, DigestResult> rawDigest;

    private Map<PeerAddress, Object> rawObjects;

    private Map<PeerAddress, ChannelBuffer> rawChannels;

    // Flag indicating if the minimum operations for put have been reached.
    private boolean minReached;

    // used for general purpose setDone()
    private Object attachement;

    private boolean releaseEarly = false;

    public FutureDHT() {
        this(0, new VotingSchemeDHT(), null);
    }

    /**
     * Creates a new DHT future object that keeps track of the status of the DHT
     * operations.
     * 
     * @param min
     *            The minimum of expected results
     * @param evaluationScheme
     *            The scheme to evaluate results from multiple peers
     * @param futureCreate
     *            The object to keep track of the futures created based on this
     *            future
     * @param futureRouting
     *            The futures from the routing process.
     */
    public FutureDHT(final int min, final EvaluatingSchemeDHT evaluationScheme, FutureCreate<FutureDHT> futureCreate) {
        this.min = min;
        this.evaluationScheme = evaluationScheme;
        this.futureCreate = futureCreate;
        self(this);
    }

    /**
     * Finish the future and set a general purpose attachement.
     * 
     * @param object
     *            General purpose attachement
     */
    public void setDone(Object attachement) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.attachement = attachement;
            this.type = FutureType.OK;
        }
        notifyListerenrs();
    }

    public Object getAttachement() {
        synchronized (lock) {
            return attachement;
        }
    }

    /**
     * Finish the future and set the keys that have been removed.
     * 
     * @param rawKeys
     *            The removed keys with information from which peer it has been
     *            removed
     */
    public void setRemovedKeys(final Map<PeerAddress, Collection<Number160>> rawKeys) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.rawKeys = rawKeys;
            final int size = rawKeys.size();
            this.minReached = size >= min;
            this.type = size > 0 ? FutureType.OK : FutureType.FAILED;
            this.reason = size > 0 ? "Minimun number of results reached" : "Expected > 0 result, but got " + size;
        }
        notifyListerenrs();
    }

    /**
     * Finish the future and set the keys that have been stored. Success or
     * failure is determined if the communication was successful. This means
     * that we need to further check if the other peers have denied the storage
     * (e.g., due to no storage space, no security permissions). Further
     * evaluation can be retrieved with {@link #getAvgStoredKeys()} or if the
     * evaluation should be done by the user, use {@link #getRawKeys()}.
     * 
     * @param domainKey
     * @param locationKey
     * 
     * @param rawKeys
     *            The keys that have been stored with information on which peer
     *            it has been stored
     * @param rawData480
     * @param ifAbsent
     *            Flag if the user requested putIfAbsent
     */
    public void setStoredKeys(Number160 locationKey, Number160 domainKey,
            final Map<PeerAddress, Collection<Number160>> rawKeys, Map<PeerAddress, Collection<Number480>> rawKeys480) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.rawKeys = rawKeys;
            this.rawKeys480 = rawKeys480;
            this.locationKey = locationKey;
            this.domainKey = domainKey;
            final int size = (rawKeys == null ? 0 : rawKeys.size()) + (rawKeys480 == null ? 0 : rawKeys480.size());
            this.minReached = size >= min;
            this.type = minReached ? FutureType.OK : FutureType.FAILED;
            this.reason = minReached ? "Minimun number of results reached" : "Expected " + min + " result, but got "
                    + size;
        }
        notifyListerenrs();
    }

    public double getAvgStoredKeys() {
        synchronized (lock) {
            final int size = rawKeys.size();
            int total = 0;
            for (Collection<Number160> collection : rawKeys.values()) {
                if (collection != null) {
                    total += collection.size();
                }
            }
            return total / (double) size;
        }
    }

    /**
     * Finish the future and set the keys and data that have been received.
     * 
     * @param rawData
     *            The keys and data that have been received with information
     *            from which peer it has been received.
     */
    public void setReceivedData(final Map<PeerAddress, Map<Number160, Data>> rawData) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.rawData = rawData;
            final int size = rawData.size();
            this.minReached = size >= min;
            this.type = size > 0 ? FutureType.OK : FutureType.FAILED;
            this.reason = size > 0 ? "Minimun number of results reached" : "Expected >0 result, but got " + size;
        }
        notifyListerenrs();
    }

    /**
     * Finishes the future and set the digest information that have been
     * received.
     * 
     * @param rawDigest
     *            The hashes of the content stored with information from which
     *            peer it has been received.
     */
    public void setReceivedDigest(final Map<PeerAddress, DigestResult> rawDigest) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.rawDigest = rawDigest;
            final int size = rawDigest.size();
            this.minReached = size >= min;
            this.type = size > 0 ? FutureType.OK : FutureType.FAILED;
            this.reason = size > 0 ? "Minimun number of results reached" : "Expected >0 result, but got " + size;
        }
        notifyListerenrs();
    }

    /**
     * Finish the future and set the keys and data that have send directly using
     * the Netty buffer.
     * 
     * @param rawChannels
     *            The raw data that have been sent directly with information on
     *            which peer it has been sent
     */
    public void setDirectData1(Map<PeerAddress, ChannelBuffer> rawChannels) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.rawChannels = rawChannels;
            final int size = rawChannels.size();
            this.minReached = size >= min;
            this.type = minReached ? FutureType.OK : FutureType.FAILED;
            this.reason = minReached ? "Minimun number of results reached" : "Expected " + min + " result, but got "
                    + size;
        }
        notifyListerenrs();
    }

    /**
     * Finish the future and set the keys and data that have send directly using
     * an object.
     * 
     * @param rawObjects
     *            The objects that have been sent directly with information on
     *            which peer it has been sent
     */
    public void setDirectData2(Map<PeerAddress, Object> rawObjects) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.rawObjects = rawObjects;
            final int size = rawObjects.size();
            this.minReached = size >= min;
            this.type = minReached ? FutureType.OK : FutureType.FAILED;
            this.reason = minReached ? "Minimun number of results reached" : "Expected " + min + " result, but got "
                    + size;
        }
        notifyListerenrs();
    }

    /**
     * Returns the raw keys from the storage or removal operation
     * 
     * @return The raw keys and the information which peer has been contacted
     */
    public Map<PeerAddress, Collection<Number160>> getRawKeys() {
        synchronized (lock) {
            return rawKeys;
        }
    }

    /**
     * Returns the raw data from the get operation
     * 
     * @return The raw data and the information which peer has been contacted
     */
    public Map<PeerAddress, Map<Number160, Data>> getRawData() {
        synchronized (lock) {
            return rawData;
        }
    }

    /**
     * @return The raw digest information with hashes of the content and the
     *         information which peer has been contacted
     */
    public Map<PeerAddress, DigestResult> getRawDigest() {
        synchronized (lock) {
            return rawDigest;
        }
    }

    /**
     * Return raw data from send_dircet (Netty buffer).
     * 
     * @return The raw data from send_dircet and the information which peer has
     *         been contacted
     */
    public Map<PeerAddress, ChannelBuffer> getRawDirectData1() {
        synchronized (lock) {
            return rawChannels;
        }
    }

    /**
     * Return raw data from send_dircet (Object).
     * 
     * @return The raw data from send_dircet and the information which peer has
     *         been contacted
     */
    public Map<PeerAddress, Object> getRawDirectData2() {
        synchronized (lock) {
            return rawObjects;
        }
    }

    /**
     * Checks if the minimum of expected results have been reached. This flag is
     * also used for determining the success or failure of this future for put
     * and send_direct.
     * 
     * @return True, if expected minimum results have been reached.
     */
    public boolean isMinReached() {
        synchronized (lock) {
            return minReached;
        }
    }

    /**
     * Returns the keys that have been stored or removed after evaluation. The
     * evaluation gets rid of the PeerAddress information, by either a majority
     * vote or cumulation. Use {@link FutureDHT#getEvalKeys()} instead of this
     * method.
     * 
     * @return The keys that have been stored or removed
     */
    @Deprecated
    public Collection<Number160> getKeys() {
        synchronized (lock) {
            Collection<Number480> result = evaluationScheme.evaluate1(locationKey, domainKey, rawKeys, rawKeys480);
            return Utils.extractContentKeys(result);
        }
    }

    /**
     * Returns the keys that have been stored or removed after evaluation. The
     * evaluation gets rid of the PeerAddress information, by either a majority
     * vote or cumulation. Use {@link FutureDHT#getEvalKeys()} instead of this
     * method.
     * 
     * @return The keys that have been stored or removed
     */
    public Collection<Number480> getEvalKeys() {
        synchronized (lock) {
            return evaluationScheme.evaluate1(locationKey, domainKey, rawKeys, rawKeys480);
        }
    }

    /**
     * Return the data from get() after evaluation. The evaluation gets rid of
     * the PeerAddress information, by either a majority vote or cumulation.
     * 
     * @return The evaluated data that have been received.
     */
    public Map<Number160, Data> getDataMap() {
        synchronized (lock) {
            return evaluationScheme.evaluate2(rawData);
        }
    }

    /**
     * @return The first data object from get() after evaluation.
     */
    public Data getData() {
        Map<Number160, Data> dataMap = getDataMap();
        if (dataMap.size() == 0) {
            return null;
        }
        return dataMap.values().iterator().next();
    }

    /**
     * Return the digest information from the get() after evaluation. The
     * evaluation gets rid of the PeerAddress information, by either a majority
     * vote or cumulation.
     * 
     * @return The evaluated digest information that have been received.
     */
    public DigestResult getDigest() {
        synchronized (lock) {
            return evaluationScheme.evaluate5(rawDigest);
        }
    }

    /**
     * Return the data from send_direct (Object) after evaluation. The
     * evaluation gets rid of the PeerAddress information, by either a majority
     * vote or cumulation.
     * 
     * @return The data that have been received.
     */
    public Object getObject() {
        synchronized (lock) {
            return this.evaluationScheme.evaluate3(rawObjects);
        }
    }

    /**
     * Return the data from send_direct (Netty buffer) after evaluation. The
     * evaluation gets rid of the PeerAddress information, by either a majority
     * vote or cumulation.
     * 
     * @return The data that have been received.
     */
    public Object getChannelBuffer() {
        synchronized (lock) {
            return this.evaluationScheme.evaluate4(rawChannels);
        }
    }

    public Map<PeerAddress, Collection<Number480>> getRawKeys480() {
        synchronized (lock) {
            return rawKeys480;
        }
    }

    public Number160 getLocationKey() {
        synchronized (lock) {
            return locationKey;
        }
    }

    public Number160 getDomainKey() {
        synchronized (lock) {
            return domainKey;
        }
    }

    /**
     * Returns the future object that keeps information about future object,
     * based on this object
     * 
     * @return FutureCreate object.
     */
    public FutureCreate<FutureDHT> getFutureCreate() {
        synchronized (lock) {
            return futureCreate;
        }
    }

    /**
     * Returns the future object that was used for the routing. Before the
     * FutureDHT is used, FutureRouting has to be completed successfully.
     * 
     * @return The future object during the previous routing, or null if routing
     *         failed completely.
     */
    public FutureRouting getFutureRouting() {
        synchronized (lock) {
            return futureRouting;
        }
    }

    /**
     * Sets the future object that was used for the routing. Before the
     * FutureDHT is used, FutureRouting has to be completed successfully.
     * 
     * @param futureRouting
     *            The future object to set
     */
    public void setFutureRouting(FutureRouting futureRouting) {
        synchronized (lock) {
            this.futureRouting = futureRouting;
        }
    }

    /**
     * Returns back those futures that are still running. If 6 storage futures
     * are started at the same time and 5 of them finish, and we specified that
     * we are fine if 5 finishes, then futureDHT returns success. However, the
     * future that may still be running is the one that stores the content to
     * the closest peer. For testing this is not acceptable, thus after waiting
     * for futureDHT, one needs to wait for the running futures as well.
     * 
     * @return A future that finishes if all running futures are finished.
     */
    public FutureForkJoin<FutureResponse> getFutureRequests() {
        synchronized (lock) {
            final int size = requests.size();
            final FutureResponse[] futureResponses = new FutureResponse[size];
            for (int i = 0; i < size; i++) {
                futureResponses[i] = requests.get(i);
            }
            return new FutureForkJoin<FutureResponse>(futureResponses);
        }
    }

    /**
     * Adds all requests that have been created for the DHT operations. Those
     * were created after the routing process.
     * 
     * @param futureResponse
     *            The futurRepsonse that has been created
     */
    public void addRequests(FutureResponse futureResponse) {
        synchronized (lock) {
            requests.add(futureResponse);
        }
    }

    /**
     * Called for futures created based on this future. This is used for
     * scheduled futures.
     * 
     * @param futureDHT
     *            The newly created future
     */
    public void repeated(FutureDHT futureDHT) {
        if (futureCreate != null) {
            futureCreate.repeated(futureDHT);
        }
    }

    public void addCleanup(Cancellable cancellable) {
        synchronized (lock) {
            cleanup.add(cancellable);
        }
    }

    public void shutdown() {
        // Even though, this future is completed, there may be tasks than can be
        // canceled due to scheduled futures attached to this event.
        synchronized (lock) {
            for (final Cancellable cancellable : cleanup) {
                cancellable.cancel();
            }
        }
    }

    public void releaseEarly() {
        synchronized (lock) {
            releaseEarly = true;
        }
    }

    public boolean isReleaseEarly() {
        synchronized (lock) {
            return releaseEarly;
        }
    }
}
