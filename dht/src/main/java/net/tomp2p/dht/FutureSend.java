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

import io.netty.buffer.ByteBuf;

import java.util.Map;

import net.tomp2p.p2p.EvaluatingSchemeDHT;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.peers.PeerAddress;

/**
 * The future object for put() operations including routing.
 * 
 * @author Thomas Bocek
 */
public class FutureSend extends FutureDHT<FutureSend> {
    // The minimum number of expected results. This is also used for put()
    // operations to decide if a future failed or not.
    private final int min;

    // Since we receive multiple results, we have an evaluation scheme to
    // simplify the result
    private final EvaluatingSchemeDHT evaluationScheme;

    // Storage of results
    private Map<PeerAddress, Object> rawObjects;
    private Map<PeerAddress, ByteBuf> rawChannels;

    // Flag indicating if the minimum operations for put have been reached.
    private boolean minReached;

    /**
     * Default constructor.
     */
    public FutureSend(final DHTBuilder<?> builder) {
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
    public FutureSend(final DHTBuilder<?> builder, final int min, final EvaluatingSchemeDHT evaluationScheme) {
        super(builder);
        this.min = min;
        this.evaluationScheme = evaluationScheme;
        self(this);
    }

    /**
     * Finish the future and set the keys and data that have send directly using the Netty buffer.
     * 
     * @param rawChannels
     *            The raw data that have been sent directly with information on which peer it has been sent
     */
    public void directData1(final Map<PeerAddress, ByteBuf> rawChannels) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return;
            }
            this.rawChannels = rawChannels;
            final int size = rawChannels.size();
            this.minReached = size >= min;
            this.type = minReached ? FutureType.OK : FutureType.FAILED;
            this.reason = minReached ? "Minimum number of results reached" : "Expected " + min + " result, but got "
                    + size;
        }
        notifyListeners();
    }

    /**
     * Finish the future and set the keys and data that have send directly using an object.
     * 
     * @param rawObjects
     *            The objects that have been sent directly with information on which peer it has been sent
     */
    public void directData2(final Map<PeerAddress, Object> rawObjects) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return;
            }
            this.rawObjects = rawObjects;
            final int size = rawObjects.size();
            this.minReached = size >= min;
            this.type = minReached ? FutureType.OK : FutureType.FAILED;
            this.reason = minReached ? "Minimum number of results reached" : "Expected " + min + " result, but got "
                    + size;
        }
        notifyListeners();
    }
    
    /**
     * Return raw data from send_dircet (Netty buffer).
     * 
     * @return The raw data from send_dircet and the information which peer has been contacted
     */
    public Map<PeerAddress, ByteBuf> rawDirectData1() {
        synchronized (lock) {
            return rawChannels;
        }
    }

    /**
     * Return raw data from send_dircet (Object).
     * 
     * @return The raw data from send_dircet and the information which peer has been contacted
     */
    public Map<PeerAddress, Object> rawDirectData2() {
        synchronized (lock) {
            return rawObjects;
        }
    }
    
    /**
     * Return the data from send_direct (Object) after evaluation. The evaluation gets rid of the PeerAddress
     * information, by either a majority vote or cumulation.
     * 
     * @return The data that have been received.
     */
    public Object object() {
        synchronized (lock) {
            return this.evaluationScheme.evaluate3(rawObjects);
        }
    }

    /**
     * Return the data from send_direct (Netty buffer) after evaluation. The evaluation gets rid of the PeerAddress
     * information, by either a majority vote or cumulation.
     * 
     * @return The data that have been received.
     */
    public ByteBuf channelBuffer() {
        synchronized (lock) {
            return this.evaluationScheme.evaluate4(rawChannels);
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
