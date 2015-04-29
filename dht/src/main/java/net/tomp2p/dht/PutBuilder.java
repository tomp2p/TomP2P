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
import java.security.PublicKey;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class PutBuilder extends DHTBuilder<PutBuilder> {
    
	private final static FuturePut FUTURE_PUT_SHUTDOWN = new FuturePut(null, 0, 0)
            .failed("Peer is shutting down.");
    
	private Entry<Number640, Data> data;
    private NavigableMap<Number640, Data> dataMap;
    private NavigableMap<Number160, Data> dataMapConvert;

    private boolean putIfAbsent = false;
    private boolean putMeta = false;
    private boolean putConfim = false;

    private PublicKey changePublicKey = null;

    public PutBuilder(PeerDHT peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public Entry<Number640, Data> data() {
        return data;
    }

    public PutBuilder data(final Data data) {
        return data(locationKey, domainKey == null ? Number160.ZERO : domainKey, Number160.ZERO,
                versionKey == null ? Number160.ZERO : versionKey, data);
    }

    public PutBuilder data(final Number160 contentKey, final Data data) {
        return data(locationKey, domainKey == null ? Number160.ZERO : domainKey, contentKey,
                versionKey == null ? Number160.ZERO : versionKey, data);
    }

    public PutBuilder data(final Number160 domainKey, final Number160 contentKey, final Data data) {
        return data(locationKey, domainKey, contentKey, versionKey == null ? Number160.ZERO : versionKey,
                data);
    }

    public PutBuilder data(final Data data, final Number160 versionKey) {
        return data(locationKey, domainKey == null ? Number160.ZERO : domainKey, Number160.ZERO,
                versionKey, data);
    }

    public PutBuilder data(final Number160 contentKey, final Data data, final Number160 versionKey) {
        return data(locationKey, domainKey == null ? Number160.ZERO : domainKey, contentKey, versionKey,
                data);
    }

    public PutBuilder data(final Number160 locationKey, final Number160 domainKey,
            final Number160 contentKey, final Number160 versionKey, final Data data) {
        this.data = new Entry<Number640, Data>() {
            @Override
            public Data setValue(Data value) {
                return null;
            }

            @Override
            public Data getValue() {
                return data;
            }

            @Override
            public Number640 getKey() {
                return new Number640(locationKey, domainKey, contentKey, versionKey);
            }
        };
        return this;
    }

    @Override
    public PutBuilder domainKey(final Number160 domainKey) {
        // if we set data before we set domain key, we need to adapt the domain key of the data object
        if (data != null) {
            data(data.getKey().locationKey(), domainKey, data.getKey().contentKey(), data.getKey()
                    .versionKey(), data.getValue());
        }
        super.domainKey(domainKey);
        return this;
    }

    @Override
    public PutBuilder versionKey(final Number160 versionKey) {
        // if we set data before we set domain key, we need to adapt the domain key of the data object
        if (data != null) {
            data(data.getKey().locationKey(), data.getKey().domainKey(), data.getKey()
                    .contentKey(), versionKey, data.getValue());
        }
        super.versionKey(versionKey);
        return this;
    }

    public PutBuilder object(Object object) throws IOException {
        return data(new Data(object));
    }

    public PutBuilder keyObject(Number160 contentKey, Object object) throws IOException {
        return data(contentKey, new Data(object));
    }

    public NavigableMap<Number640, Data> dataMap() {
        return dataMap;
    }

    public PutBuilder dataMap(NavigableMap<Number640, Data> dataMap) {
        this.dataMap = dataMap;
        return this;
    }

    public NavigableMap<Number160, Data> dataMapContent() {
        return dataMapConvert;
    }

    public PutBuilder dataMapConvert(NavigableMap<Number160, Data> dataMapConvert) {
        this.dataMapConvert = dataMapConvert;
        return this;
    }

    public boolean isPutIfAbsent() {
        return putIfAbsent;
    }

    public PutBuilder putIfAbsent() {
        return putIfAbsent(true);
    }
    
    public PutBuilder putIfAbsent(boolean putIfAbsent) {
        this.putIfAbsent = putIfAbsent;
        return this;
    }
    
    public boolean isPutMeta() {
        return putMeta;
    }

    public PutBuilder putMeta() {
        return putMeta(true);
    }
    
    public PutBuilder putMeta(boolean putMeta) {
        this.putMeta = putMeta;
        return this;
    }

	public boolean isPutConfirm() {
		return putConfim;
	}

	public PutBuilder putConfirm() {
		return putConfirm(true);
	}
	
	public PutBuilder putConfirm(boolean putConfirm)
	{
		this.putConfim = putConfirm;
		return this;
	}

    public PublicKey changePublicKey() {
    	return changePublicKey;
    }
    
    public PutBuilder changePublicKey(PublicKey changePublicKey) {
    	this.changePublicKey = changePublicKey;
    	putMeta();
    	sign();
    	return this;
    }

    public FuturePut start() {
        if (peerDht.peer().isShutdown()) {
            return FUTURE_PUT_SHUTDOWN;
        }
        preBuild();
        if (data != null) {
            if (dataMap == null) {
                dataMap = new TreeMap<Number640, Data>();
            }
            dataMap().put(data().getKey(), data().getValue());
        }
        if (!putMeta && !putConfim && dataMap == null && dataMapConvert == null) {
            throw new IllegalArgumentException("No data set to be put.");
        }
        if (locationKey == null) {
            throw new IllegalArgumentException("No location key set.");
        }
        if (domainKey == null) {
            domainKey = Number160.ZERO;
        }
        if (versionKey == null) {
            versionKey = Number160.ZERO;
        }
        return peerDht.distributedHashTable().put(this);
    }
}
