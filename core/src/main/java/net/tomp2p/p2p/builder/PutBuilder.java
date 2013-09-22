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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.tomp2p.futures.FuturePut;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;

public class PutBuilder extends DHTBuilder<PutBuilder> {
    private Entry<Number480, Data> data;

    private Map<Number480, Data> dataMap;

    private Map<Number160, Data> dataMapConvert;

    //
    private boolean putIfAbsent = false;

    public PutBuilder(Peer peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public Entry<Number480, Data> getData() {
        return data;
    }

    public PutBuilder setData(final Data data) {
        return setData(locationKey, domainKey == null ? Number160.ZERO : domainKey, Number160.ZERO, data);
    }

    public PutBuilder setData(final Number160 contentKey, final Data data) {
        return setData(locationKey, domainKey == null ? Number160.ZERO : domainKey, contentKey, data);
    }

    public PutBuilder setData(final Number160 domainKey, final Number160 contentKey, final Data data) {
        return setData(locationKey, domainKey, contentKey, data);
    }

    public PutBuilder setData(final Number160 locationKey, final Number160 domainKey,
            final Number160 contentKey, final Data data) {
        this.data = new Entry<Number480, Data>() {
            @Override
            public Data setValue(Data value) {
                return null;
            }

            @Override
            public Data getValue() {
                return data;
            }

            @Override
            public Number480 getKey() {
                return new Number480(locationKey, domainKey, contentKey);
            }
        };
        return this;
    }

    @Override
    public PutBuilder setDomainKey(final Number160 domainKey) {
        // if we set data before we set domain key, we need to adapt the domain key of the data object
        if (data != null) {
            setData(data.getKey().getLocationKey(), domainKey, data.getKey().getContentKey(), data.getValue());
        }
        super.setDomainKey(domainKey);
        return this;
    }

    public PutBuilder setObject(Object object) throws IOException {
        return setData(new Data(object));
    }

    public PutBuilder setKeyObject(Number160 contentKey, Object object) throws IOException {
        return setData(contentKey, new Data(object));
    }

    public Map<Number480, Data> getDataMap() {
        return dataMap;
    }

    public PutBuilder setDataMap(Map<Number480, Data> dataMap) {
        this.dataMap = dataMap;
        return this;
    }

    public Map<Number160, Data> getDataMapContent() {
        return dataMapConvert;
    }

    public PutBuilder setDataMapContent(Map<Number160, Data> dataMapConvert) {
        this.dataMapConvert = dataMapConvert;
        return this;
    }

    public boolean isPutIfAbsent() {
        return putIfAbsent;
    }

    public PutBuilder setPutIfAbsent(boolean putIfAbsent) {
        this.putIfAbsent = putIfAbsent;
        return this;
    }

    public PutBuilder setPutIfAbsent() {
        this.putIfAbsent = true;
        return this;
    }

    public FuturePut start() {
        preBuild("put-builder");
        if (data != null) {
            if (dataMap == null) {
                setDataMap(new HashMap<Number480, Data>(1));
            }
            getDataMap().put(getData().getKey(), getData().getValue());
        }
        if (dataMap == null && dataMapConvert == null) {
            throw new IllegalArgumentException(
                    "You must either set data via setDataMap() or setData(). Cannot add nothing.");
        }

        return peer.getDistributedHashMap().put(this);
    }
}
