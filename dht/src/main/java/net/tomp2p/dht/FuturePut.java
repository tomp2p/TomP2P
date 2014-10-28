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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.dht.StorageLayer.PutStatus;
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
    
    private final int dataSize;

    // Storage of results
    private Map<PeerAddress, Map<Number640, Byte>> rawResult;

    // Flag indicating if the minimum operations for put have been reached.
    private boolean minReached;
    
    private Map<Number640, Integer> result;

    /**
     * Creates a new DHT future object that keeps track of the status of the DHT operations.
     * 
     * @param min
     *            The minimum of expected results
     * @param evaluationScheme
     *            The scheme to evaluate results from multiple peers
     */
    public FuturePut(final DHTBuilder<?> builder, final int min, final int dataSize) {
        super(builder);
        this.min = min;
        this.dataSize = dataSize;
        self(this);
    }

    /**
     * Finish the future and set the keys that have been stored. Success or failure is determined if the communication
     * was successful. This means that we need to further check if the other peers have denied the storage (e.g., due to
     * no storage space, no security permissions). Further evaluation can be retrieved with {@link #avgStoredKeys()}
     * or if the evaluation should be done by the user, use {@link #rawKeys()}.
     * 
     * @param rawKeys
     *            The keys that have been stored with information on which peer it has been stored
     * @param rawKeys480
     *            The keys with locationKey and domainKey Flag if the user requested putIfAbsent
     */
    public void storedKeys(final Map<PeerAddress, Map<Number640, Byte>> rawResult) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return;
            }
            this.rawResult = rawResult;
            final int size = rawResult == null ? 0 : rawResult.size();
            this.minReached = size >= min;
            this.type = minReached ? FutureType.OK : FutureType.FAILED;
            this.reason = minReached ? "Minimum number of results reached" : "Expected " + min
                    + " result, but got " + size;
        }
        notifyListeners();
    }

    /**
     * @return The average keys received from the DHT. Only evaluates rawKeys.
     */
    public double avgStoredKeys() {
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
    public Map<PeerAddress, Map<Number640, Byte>> rawResult() {
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
     * information, by either a majority vote or cumulation. Use {@link FuturePut#evalKeys()} instead of this method.
     * 
     * @return The keys that have been stored or removed
     */
    public Map<Number640, Integer> result() {
        synchronized (lock) {
            if(result == null) {
                result = evaluate(rawResult); 
            }
            return result;
        }
    }
    
    private Map<Number640, Integer> evaluate(Map<PeerAddress, Map<Number640, Byte>> rawResult2) {
        Map<Number640, Integer> result = new HashMap<Number640, Integer>();
        for(Map<Number640, Byte> map:rawResult2.values()) {
            for(Map.Entry<Number640, Byte> entry: map.entrySet()) {
                if(entry.getValue().intValue() == PutStatus.OK.ordinal()
                		|| entry.getValue().intValue() == PutStatus.VERSION_FORK.ordinal()
                		|| entry.getValue().intValue() == PutStatus.DELETED.ordinal()) {
                    Integer integer = result.get(entry.getKey());
                    if(integer == null) {
                        result.put(entry.getKey(), 1);
                    } else {
                        result.put(entry.getKey(), integer + 1);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public boolean isSuccess() {
        if(!super.isSuccess()) {
            return false;
        }
        return checkResults(result(), rawResult.size(), dataSize);
    }
    
    private boolean checkResults(Map<Number640, Integer> result2, int peerReports, int dataSize) {
        for(Map.Entry<Number640, Integer> entry:result2.entrySet()) {
            if(entry.getValue() != peerReports) {
                return false;
            }
        }
        //we know exactly how much data we need to store
        return result2.size() == dataSize;
    }

    public boolean isSuccessPartially() {
        boolean networkSuccess = super.isSuccess();
        return networkSuccess && result().size() > 0;
    }
}
