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

import java.util.Collection;
import java.util.Map;

import net.tomp2p.p2p.EvaluatingSchemeDHT;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.p2p.builder.DHTBuilder;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;

/**
 * The future object for put() operations including routing.
 * 
 * @author Thomas Bocek
 */
public class FuturePut extends FutureDHT<FuturePut> {
    // The minimum number of expected results. This is also used for put()
    // operations to decide if a future failed or not.
    private final int min;

    // Since we receive multiple results, we have an evaluation scheme to
    // simplify the result
    private final EvaluatingSchemeDHT evaluationScheme;

    // Storage of results
    private Map<PeerAddress, Map<Number640, Byte>> rawResult;

    // Flag indicating if the minimum operations for put have been reached.
    private boolean minReached;

    /**
     * Default constructor.
     */
    public FuturePut(final DHTBuilder<?> builder) {
        this(builder, 0, new VotingSchemeDHT());
    }

    /**
     * Creates a new DHT future object that keeps track of the status of the DHT operations.
     * 
     * @param min
     *            The minimum of expected results
     * @param evaluationScheme
     *            The scheme to evaluate results from multiple peers
     */
    public FuturePut(final DHTBuilder<?> builder, final int min, final EvaluatingSchemeDHT evaluationScheme) {
        super(builder);
        this.min = min;
        this.evaluationScheme = evaluationScheme;
        self(this);
    }

    /**
     * Finish the future and set the keys that have been stored. Success or failure is determined if the communication
     * was successful. This means that we need to further check if the other peers have denied the storage (e.g., due to
     * no storage space, no security permissions). Further evaluation can be retrieved with {@link #getAvgStoredKeys()}
     * or if the evaluation should be done by the user, use {@link #getRawKeys()}.
     * 
     * @param rawKeys
     *            The keys that have been stored with information on which peer it has been stored
     * @param rawKeys480
     *            The keys with locationKey and domainKey Flag if the user requested putIfAbsent
     */
    public void setStoredKeys(final Map<PeerAddress, Map<Number640, Byte>> rawResult) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.rawResult = rawResult;
            final int size = rawResult == null ? 0 : rawResult.size();
            this.minReached = size >= min;
            this.type = minReached ? FutureType.OK : FutureType.FAILED;
            this.reason = minReached ? "Minimun number of results reached" : "Expected " + min
                    + " result, but got " + size;
        }
        notifyListeners();
    }

    /**
     * @return The average keys received from the DHT. Only evaluates rawKeys.
     */
    public double getAvgStoredKeys() {
        synchronized (lock) {
            final int size = rawResult.size();
            int total = 0;
            for (Map<Number640, Byte> map : rawResult.values()) {
                Collection<Number640> collection = map.keySet();
                if (collection != null) {
                    total += collection.size();
                }
            }
            return total / (double) size;
        }
    }

    /**
     * Returns the raw result from the storage or removal operation.
     * 
     * @return The raw keys and the information which peer has been contacted
     */
    public Map<PeerAddress, Map<Number640, Byte>> getRawResult() {
        synchronized (lock) {
            return rawResult;
        }
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
     * Returns the keys that have been stored or removed after evaluation. The evaluation gets rid of the PeerAddress
     * information, by either a majority vote or cumulation. Use {@link FuturePut#getEvalKeys()} instead of this method.
     * 
     * @return The keys that have been stored or removed
     */
    public Collection<Number640> getResult() {
        synchronized (lock) {
            return evaluationScheme.evaluate7(rawResult);
        }
    }
}
