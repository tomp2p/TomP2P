/*
 * Copyright 2012 Thomas Bocek
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

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.rpc.SimpleBloomFilter;

public class DigestBuilder extends DHTBuilder<DigestBuilder> implements SearchableBuilder {
    
    private final static FutureDigest FUTURE_DIGEST_SHUTDOWN = new FutureDigest(null)
    .failed("Peer is shutting down.");
    private final static Collection<Number160> NUMBER_ZERO_CONTENT_KEYS = new ArrayList<Number160>(1);

    private Collection<Number160> contentKeys;
    private Collection<Number640> keys;
    private Number160 contentKey;

    private SimpleBloomFilter<Number160> keyBloomFilter;
    private SimpleBloomFilter<Number160> contentBloomFilter;

    private EvaluatingSchemeDHT evaluationScheme;
    private Number640 from;
    private Number640 to;

    private boolean all = false;
    private boolean returnBloomFilter = false;
    private boolean returnAllBloomFilter = false;
    private boolean ascending = true;
    private boolean bloomFilterAnd = true;
    private boolean returnMetaValues = false;
    private boolean fastGet = true;

    private int returnNr = -1;

    static {
        NUMBER_ZERO_CONTENT_KEYS.add(Number160.ZERO);
    }

    public DigestBuilder(PeerDHT peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public Collection<Number160> contentKeys() {
        return contentKeys;
    }

    /**
     * Set the content keys that should be found. Please note that if the content keys are too large, you may need to
     * switch to TCP during routing. The default routing is UDP. Currently, the header is 59bytes, and the length of the
     * content keys is as follows: 4 bytes for the length, 20bytes per content key. The user is warned if it will exceed
     * the UDP size of 1400 (ConnectionHandler.UDP_LIMIT)
     * 
     * @param contentKeys
     * @return
     */
    public DigestBuilder contentKeys(Collection<Number160> contentKeys) {
        this.contentKeys = contentKeys;
        return this;
    }

    public Collection<Number640> keys() {
        return keys;
    }

    public DigestBuilder keys(Collection<Number640> keys) {
        this.keys = keys;
        return this;
    }

    public Number160 contentKey() {
        return contentKey;
    }

    public DigestBuilder contentKey(Number160 contentKey) {
        this.contentKey = contentKey;
        return this;
    }

    public SimpleBloomFilter<Number160> keyBloomFilter() {
        return keyBloomFilter;
    }

    public DigestBuilder keyBloomFilter(SimpleBloomFilter<Number160> keyBloomFilter) {
        this.keyBloomFilter = keyBloomFilter;
        return this;
    }

    public SimpleBloomFilter<Number160> contentBloomFilter() {
        return contentBloomFilter;
    }

    public DigestBuilder contentBloomFilter(SimpleBloomFilter<Number160> contentBloomFilter) {
        this.contentBloomFilter = contentBloomFilter;
        return this;
    }

    public EvaluatingSchemeDHT evaluationScheme() {
        return evaluationScheme;
    }

    public DigestBuilder evaluationScheme(EvaluatingSchemeDHT evaluationScheme) {
        this.evaluationScheme = evaluationScheme;
        return this;
    }

    public Number640 from() {
        return from;
    }

    public DigestBuilder from(Number640 from) {
        this.from = from;
        return this;
    }

    public Number640 to() {
        return to;
    }

    public DigestBuilder to(Number640 to) {
        this.to = to;
        return this;
    }

    public boolean isRange() {
        return from != null && to != null;
    }
    
    public boolean isAll() {
        return all;
    }

    public DigestBuilder all() {
        return all(true);
    }
    
    public DigestBuilder all(boolean all) {
        this.all = all;
        return this;
    }

    public boolean isReturnBloomFilter() {
        return returnBloomFilter;
    }

    public DigestBuilder returnBloomFilter() {
        return returnBloomFilter(true);
    }
    
    public DigestBuilder returnBloomFilter(boolean returnBloomFilter) {
        this.returnBloomFilter = returnBloomFilter;
        return this;
    }
    
    public boolean isReturnAllBloomFilter() {
        return returnAllBloomFilter;
    }

    public DigestBuilder returnAllBloomFilter() {
    	return returnAllBloomFilter(true);
    }

    public DigestBuilder returnAllBloomFilter(boolean returnAllBloomFilter) {
        this.returnAllBloomFilter = returnAllBloomFilter;
        return this;
    }

    public boolean isAscending() {
        return ascending;
    }
    
    public boolean isDescending() {
        return !ascending;
    }

    public DigestBuilder ascending() {
        return ascending(true);
    }
    
    public DigestBuilder descending() {
        return ascending(false);
    }
    
    public DigestBuilder ascending(boolean ascending) {
        this.ascending = ascending;
        return this;
    }

    public boolean isBloomFilterAnd() {
        return bloomFilterAnd;
    }

    public DigestBuilder bloomFilterAnd() {
        return bloomFilterAnd(true);
    }
    
    public DigestBuilder bloomFilterAnd(boolean bloomFilterAnd) {
        this.bloomFilterAnd = bloomFilterAnd;
        return this;
    }
    
    public boolean isReturnMetaValues() {
        return returnMetaValues;
    }
    
    public DigestBuilder returnMetaValues() {
    	return returnMetaValues(true);
    }
    
    public DigestBuilder returnMetaValues(boolean returnMetaValues) {
        this.returnMetaValues = returnMetaValues;
        return this;
    }

    public boolean isFastGet() {
        return fastGet;
    }

    public DigestBuilder fastGet() {
        return fastGet(true);
    }
    
    public DigestBuilder fastGet(boolean fastGet) {
        this.fastGet = fastGet;
        return this;
    }

    public int returnNr() {
        return returnNr;
    }
    
    public DigestBuilder returnNr(int returnNr) {
        this.returnNr = returnNr;
        return this;
    }

    public FutureDigest start() {
        if (peerDht.peer().isShutdown()) {
            return FUTURE_DIGEST_SHUTDOWN;
        }
        preBuild();

        if (all) {
            contentKeys = null;
        } else if (contentKeys == null && !all) {
            // default is Number160.ZERO
            if (contentKey == null) {
                contentKeys = NUMBER_ZERO_CONTENT_KEYS;
            } else {
                contentKeys = new ArrayList<Number160>(1);
                contentKeys.add(contentKey);
            }
        }
        if (evaluationScheme == null) {
            evaluationScheme = new VotingSchemeDHT();
        }
        return peerDht.distributedHashTable().digest(this);
    }
}
