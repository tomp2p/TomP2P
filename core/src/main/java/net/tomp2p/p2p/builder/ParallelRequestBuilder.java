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

package net.tomp2p.p2p.builder;

import java.util.NavigableSet;
import java.util.TreeSet;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.DistributedHashTable;
import net.tomp2p.p2p.OperationMapper;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class ParallelRequestBuilder<K extends FutureDHT<?>> extends
        DHTBuilder<ParallelRequestBuilder<K>> {
    private NavigableSet<PeerAddress> queue;

    private OperationMapper<K> operation;

    private boolean cancelOnFinish = false;
    
    private K futureDHT;

    public ParallelRequestBuilder(Peer peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public NavigableSet<PeerAddress> getQueue() {
        return queue;
    }

    public ParallelRequestBuilder<K> setQueue(NavigableSet<PeerAddress> queue) {
        this.queue = queue;
        return this;
    }

    public ParallelRequestBuilder<K> add(PeerAddress peerAddress) {
        if (queue == null) {
            queue = new TreeSet<PeerAddress>(peer.getPeerBean().peerMap().createComparator());
        }
        queue.add(peerAddress);
        return this;
    }

    public OperationMapper<K> getOperation() {
        return operation;
    }

    public ParallelRequestBuilder<K> setOperation(OperationMapper<K> operation) {
        this.operation = operation;
        return this;
    }

    public boolean isCancelOnFinish() {
        return cancelOnFinish;
    }

    public ParallelRequestBuilder<K> setCancelOnFinish() {
        this.cancelOnFinish = true;
        return this;
    }

    public ParallelRequestBuilder<K> setCancelOnFinish(boolean cancelOnFinish) {
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
        if (queue == null || queue.size() == 0) {
            throw new IllegalArgumentException("queue cannot be empty");
        }

        return DistributedHashTable.<K> parallelRequests(requestP2PConfiguration, queue, cancelOnFinish,
                futureChannelCreator, operation, futureDHT);
    }
}
