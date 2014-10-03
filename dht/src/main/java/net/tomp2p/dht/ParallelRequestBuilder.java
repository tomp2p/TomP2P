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

import java.util.NavigableSet;
import java.util.TreeSet;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class ParallelRequestBuilder<K extends FutureDHT<?>> extends
        DHTBuilder<ParallelRequestBuilder<K>> {
    private NavigableSet<PeerAddress> directHits;
    private NavigableSet<PeerAddress> potentialHits;

    private OperationMapper<K> operation;

    private boolean cancelOnFinish = false;
    
    private K futureDHT;

    public ParallelRequestBuilder(PeerDHT peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public NavigableSet<PeerAddress> directHits() {
        return directHits;
    }

    public ParallelRequestBuilder<K> directHits(NavigableSet<PeerAddress> directHits) {
        this.directHits = directHits;
        return this;
    }
    
    public NavigableSet<PeerAddress> potentialHits() {
        return potentialHits;
    }

    public ParallelRequestBuilder<K> potentialHits(NavigableSet<PeerAddress> potentialHits) {
        this.potentialHits = potentialHits;
        return this;
    }

    public ParallelRequestBuilder<K> add(PeerAddress peerAddress) {
        if (directHits == null) {
        	directHits = new TreeSet<PeerAddress>(peer.peer().peerBean().peerMap().createComparator());
        }
        if (potentialHits == null) {
        	potentialHits = new TreeSet<PeerAddress>(peer.peer().peerBean().peerMap().createComparator());
        }
        potentialHits.add(peerAddress);
        return this;
    }

    public OperationMapper<K> operation() {
        return operation;
    }

    public ParallelRequestBuilder<K> operation(OperationMapper<K> operation) {
        this.operation = operation;
        return this;
    }

    public boolean isCancelOnFinish() {
        return cancelOnFinish;
    }

    public ParallelRequestBuilder<K> cancelOnFinish() {
        this.cancelOnFinish = true;
        return this;
    }

    public ParallelRequestBuilder<K> cancelOnFinish(boolean cancelOnFinish) {
        this.cancelOnFinish = cancelOnFinish;
        return this;
    }
    
    public ParallelRequestBuilder<K> futureDHT(K futureDHT) {
    	this.futureDHT = futureDHT;
    	return this;
    }
    
    public K futureDHT() {
    	return futureDHT;
    }

    public K start() {

        preBuild("parallel-builder");
        if (directHits == null || potentialHits == null || potentialHits.size() == 0) {
            throw new IllegalArgumentException("queue cannot be empty");
        }

        return DistributedHashTable.<K> parallelRequests(requestP2PConfiguration, directHits, potentialHits, cancelOnFinish,
                futureChannelCreator, operation, futureDHT);
    }
}
