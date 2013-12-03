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

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.futures.FutureRemove;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public class RemoveBuilder extends DHTBuilder<RemoveBuilder> {
    
    private final static FutureRemove FUTURE_SHUTDOWN = new FutureRemove(null)
            .setFailed("remove builder - peer is shutting down");
    
    private Collection<Number160> contentKeys;

    private Collection<Number640> keys;

    private Number160 contentKey;

    private boolean all = false;

    private boolean returnResults = false;

    public RemoveBuilder(Peer peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public Collection<Number160> getContentKeys() {
        return contentKeys;
    }

    public RemoveBuilder setContentKeys(Collection<Number160> contentKeys) {
        this.contentKeys = contentKeys;
        return this;
    }

    public Collection<Number640> getKeys() {
        return keys;
    }

    public RemoveBuilder setKeys(Collection<Number640> keys) {
        this.keys = keys;
        return this;
    }

    public Number160 getContentKey() {
        return contentKey;
    }

    public RemoveBuilder setContentKey(Number160 contentKey) {
        this.contentKey = contentKey;
        return this;
    }

    public boolean isAll() {
        return all;
    }

    public RemoveBuilder setAll(boolean all) {
        this.all = all;
        return this;
    }

    public RemoveBuilder setAll() {
        this.all = true;
        return this;
    }

    public boolean isReturnResults() {
        return returnResults;
    }

    public RemoveBuilder setReturnResults(boolean returnResults) {
        this.returnResults = returnResults;
        return this;
    }

    public RemoveBuilder setReturnResults() {
        this.returnResults = true;
        return this;
    }

    public FutureRemove start() {
        if (peer.isShutdown()) {
            return FUTURE_SHUTDOWN;
        }
        preBuild("remove-builder");
        if (all) {
            contentKeys = null;
        } else if (contentKeys == null && !all) {
            contentKeys = new ArrayList<Number160>(1);
            if (contentKey == null) {
                contentKey = Number160.ZERO;
            }
            contentKeys.add(contentKey);
        }

        return peer.getDistributedHashMap().remove(this);
    }
}
