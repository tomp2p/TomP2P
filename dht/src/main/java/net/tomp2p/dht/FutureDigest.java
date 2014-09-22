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
package net.tomp2p.dht;

import java.util.Map;

import net.tomp2p.p2p.EvaluatingSchemeDHT;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestResult;

/**
 * The future object for put() operations including routing.
 * 
 * @author Thomas Bocek
 */
public class FutureDigest extends FutureDHT<FutureDigest> {
    // The minimum number of expected results. This is also used for put()
    // operations to decide if a future failed or not.
    private final int min;

    // Since we receive multiple results, we have an evaluation scheme to
    // simplify the result
    private final EvaluatingSchemeDHT evaluationScheme;

    // Digest results
    private Map<PeerAddress, DigestResult> rawDigest;

    // Flag indicating if the minimum operations for put have been reached.
    private boolean minReached;

    /**
     * Default constructor.
     */
    public FutureDigest(final DHTBuilder<?> builder) {
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
    public FutureDigest(final DHTBuilder<?> builder, final int min, final EvaluatingSchemeDHT evaluationScheme) {
        super(builder);
        this.min = min;
        this.evaluationScheme = evaluationScheme;
        self(this);
    }
    
    /**
     * Finishes the future and set the digest information that have been received.
     * 
     * @param rawDigest
     *            The hashes of the content stored with information from which peer it has been received.
     */
    public void receivedDigest(final Map<PeerAddress, DigestResult> rawDigest) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return;
            }
            this.rawDigest = rawDigest;
            final int size = rawDigest.size();
            this.minReached = size >= min;
            this.type = size > 0 ? FutureType.OK : FutureType.FAILED;
            this.reason = size > 0 ? "Minimum number of results reached" : "Expected >0 result, but got " + size;
        }
        notifyListeners();
    }
    
    /**
     * @return The raw digest information with hashes of the content and the information which peer has been contacted
     */
    public Map<PeerAddress, DigestResult> rawDigest() {
        synchronized (lock) {
            return rawDigest;
        }
    }
    
    /**
     * Return the digest information from the get() after evaluation. The evaluation gets rid of the PeerAddress
     * information, by either a majority vote or cumulation.
     * 
     * @return The evaluated digest information that have been received.
     */
    public DigestResult digest() {
        synchronized (lock) {
            return evaluationScheme.evaluate5(rawDigest);
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
}
