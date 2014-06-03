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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class AddBuilder extends DHTBuilder<AddBuilder> {
    private final static FuturePut FUTURE_SHUTDOWN = new FuturePut(null, 0, 0)
            .failed("add builder - peer is shutting down");
    private Collection<Data> dataSet;

    private Data data;

    private boolean list = false;

    private Random rnd;

    public AddBuilder(PeerDHT peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public Collection<Data> dataSet() {
        return dataSet;
    }

    public AddBuilder dataSet(Collection<Data> dataSet) {
        this.dataSet = dataSet;
        return this;
    }

    public Data data() {
        return data;
    }

    public AddBuilder data(Data data) {
        this.data = data;
        return this;
    }

    public AddBuilder object(Object object) throws IOException {
        return data(new Data(object));
    }

    public boolean isList() {
        return list;
    }

    public AddBuilder list(boolean list) {
        this.list = list;
        return this;
    }

    public AddBuilder list() {
        this.list = true;
        return this;
    }

    public AddBuilder random(Random rnd) {
        this.rnd = rnd;
        return this;
    }

    public Random random() {
        return rnd;
    }

    public FuturePut start() {
        if (peer.peer().isShutdown()) {
            return FUTURE_SHUTDOWN;
        }
        preBuild("add-builder");
        if (dataSet == null) {
            dataSet = new ArrayList<Data>(1);
        }
        if (data != null) {
            dataSet.add(data);
        }
        if (dataSet.size() == 0) {
            throw new IllegalArgumentException(
                    "You must either set data via setDataMap() or setData(). Cannot add nothing.");
        }
        if (rnd == null) {
            rnd = new Random();
        }

        return peer.distributedHashTable().add(this);
    }
}
