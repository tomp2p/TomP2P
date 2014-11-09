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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.dht.StorageLayer.PutStatus;
import net.tomp2p.p2p.EvaluatingSchemeDHT;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

/**
 * The future object for put() operations including routing.
 * 
 * @author Thomas Bocek
 * @param <K>
 */
public class FutureRemove extends FutureDHT<FutureRemove> {
    // The minimum number of expected results. This is also used for put()
    // operations to decide if a future failed or not.
    private final boolean failIfNotFound;

    // Since we receive multiple results, we have an evaluation scheme to
    // simplify the result
    private final EvaluatingSchemeDHT evaluationScheme;

    // Storage of results
    private Map<PeerAddress, Map<Number640, Byte>> rawKeys640;
    private Map<PeerAddress, Map<Number640, Data>> rawData;
    
    private Map<Number640, Integer> result;
    
    /**
     * Default constructor.
     */
    public FutureRemove(final DHTBuilder<?> builder) {
        this(builder, new VotingSchemeDHT());
    }

    /**
     * Creates a new DHT future object that keeps track of the status of the DHT operations.
     * 
     * @param min
     *            The minimum of expected results
     * @param evaluationScheme
     *            The scheme to evaluate results from multiple peers
     */
    public FutureRemove(final DHTBuilder<?> builder, final EvaluatingSchemeDHT evaluationScheme) {
        super(builder);
        this.evaluationScheme = evaluationScheme;
        this.failIfNotFound = builder == null? false: ((RemoveBuilder)builder).isFailIfNotFound();
        self(this);
    }

    /**
     * Finish the future and set the keys that have been stored. Success or failure is determined if the communication
     * was successful. This means that we need to further check if the other peers have denied the storage (e.g., due to
     * no storage space, no security permissions). Further evaluation can be retrieved with {@link #avgStoredKeys()}
     * or if the evaluation should be done by the user, use {@link #rawKeys()}.
     * 
     * @param domainKey
     *            The domain key
     * @param locationKey
     *            The location key
     * @param rawKeys
     *            The keys that have been stored with information on which peer it has been stored
     * @param rawKeys480
     *            The keys with locationKey and domainKey Flag if the user requested putIfAbsent
     */
    public void storedKeys(final Map<PeerAddress, Map<Number640, Byte>> rawKeys640) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return;
            }
            this.rawKeys640 = rawKeys640;
            final int size = rawKeys640 == null ? 0 : rawKeys640.size();
            this.type = size > 0 ? FutureType.OK : FutureType.FAILED;
            this.reason = size > 0 ? "Minimum number of results reached" : "Expected > 0 result, but got " + size;
        }
        notifyListeners();
    }

    /**
     * @return The average keys received from the DHT. Only evaluates rawKeys.
     */
	public double avgStoredKeys() {
		synchronized (lock) {
			final int size = rawKeys640.size();
			int total = 0;
			for (Map<Number640, Byte> map : rawKeys640.values()) {
				if (map != null) {
					total += map.size();
				}
			}
			return total / (double) size;
		}
	}
    
    /**
     * Finish the future and set the keys and data that have been received.
     * 
     * @param rawData
     *            The keys and data that have been received with information from which peer it has been received.
     */
    public void receivedData(final Map<PeerAddress, Map<Number640, Data>> rawData) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return;
            }
            this.rawData = rawData;
            final int size = rawData.size();
            this.type = size > 0 ? FutureType.OK : FutureType.FAILED;
            this.reason = size > 0 ? "Minimum number of results reached" : "Expected >0 result, but got " + size;
        }
        notifyListeners();
    }

    /**
     * Returns the raw keys from the storage or removal operation.
     * 
     * @return The raw keys and the information which peer has been contacted
     */
    public Map<PeerAddress, Map<Number640, Byte>> rawKeys() {
        synchronized (lock) {
            return rawKeys640;
        }
    }

    /**
     * Returns the keys that have been stored or removed after evaluation. The evaluation gets rid of the PeerAddress
     * information, by either a majority vote or cumulation. Use {@link FutureRemove#evalKeys()} instead of this method.
     * 
     * @return The keys that have been stored or removed
     */
    public Collection<Number640> evalKeys() {
        synchronized (lock) {
            return evaluationScheme.evaluate6(rawKeys640);
        }
    }
    
    /**
     * Returns the raw data from the get operation.
     * 
     * @return The raw data and the information which peer has been contacted
     */
    public Map<PeerAddress, Map<Number640, Data>> rawData() {
        synchronized (lock) {
            return rawData;
        }
    }
    
    /**
     * Return the data from get() after evaluation. The evaluation gets rid of the PeerAddress information, by either a
     * majority vote or cumulation.
     * 
     * @return The evaluated data that have been received.
     */
    public Map<Number640, Data> dataMap() {
        synchronized (lock) {
            return evaluationScheme.evaluate2(rawData);
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
            	 if(rawKeys640!=null) {
            		 result = evaluate0(rawKeys640);
            	 } else if(rawData!=null) {
            		 result = evaluate1(rawData);
            	 } else {
            		 return Collections.<Number640, Integer>emptyMap();
            	 }
            }
            return result;
        }
    }
    
    private Map<Number640, Integer> evaluate0(Map<PeerAddress, Map<Number640, Byte>> rawResult2) {
        Map<Number640, Integer> result = new HashMap<Number640, Integer>();
        for(Map<Number640, Byte> map:rawResult2.values()) {
            for(Map.Entry<Number640, Byte> entry: map.entrySet()) {
                if(entry.getValue().intValue() == PutStatus.OK.ordinal()) {
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
    
	private Map<Number640, Integer> evaluate1(Map<PeerAddress, Map<Number640, Data>> rawData) {
		Map<Number640, Integer> result = new HashMap<Number640, Integer>();
		for (Map<Number640, Data> map : rawData.values()) {
			for (Map.Entry<Number640, Data> entry : map.entrySet()) {
				// data is never null
				Integer integer = result.get(entry.getKey());
				if (integer == null) {
					result.put(entry.getKey(), 1);
				} else {
					result.put(entry.getKey(), integer + 1);
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
        if(!failIfNotFound) {
        	return true; 
        }
        if(rawKeys640 != null) {
        	return checkAtLeastOneSuccess();
        } else if (rawData!=null) {
        	return checkAtLeastOneSuccessData();
        } 
        return false;
    }
    
    private boolean checkAtLeastOneSuccess() {
        for(Map.Entry<PeerAddress, Map<Number640, Byte>> entry:rawKeys640.entrySet()) {
        	for(Map.Entry<Number640, Byte> entry2:entry.getValue().entrySet()) {
                if(entry2.getValue() == PutStatus.OK.ordinal()) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private boolean checkAtLeastOneSuccessData() {
        for(Map.Entry<PeerAddress, Map<Number640, Data>> entry:rawData.entrySet()) {
        	for(Map.Entry<Number640, Data> entry2:entry.getValue().entrySet()) {
                if(entry2.getValue() != null && !entry2.getValue().isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }
}
