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
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.tomp2p.futures.FuturePut;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class PutBuilder extends DHTBuilder<PutBuilder> {
    private final static FuturePut FUTURE_SHUTDOWN = new FuturePut(null, 0, 0)
            .setFailed("put builder - peer is shutting down");
    private Entry<Number640, Data> data;

    private Map<Number640, Data> dataMap;

    private Map<Number160, Data> dataMapConvert;

    private boolean putIfAbsent = false;
    
    private boolean putMeta = false;
    
    private boolean putConfim = false;
    private boolean putReject = false;
    
    private PublicKey changePublicKey = null;

    public PutBuilder(Peer peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public Entry<Number640, Data> getData() {
        return data;
    }

    public PutBuilder setData(final Data data) {
        return setData(locationKey, domainKey == null ? Number160.ZERO : domainKey, Number160.ZERO,
                versionKey == null ? Number160.ZERO : versionKey, data);
    }

    public PutBuilder setData(final Number160 contentKey, final Data data) {
        return setData(locationKey, domainKey == null ? Number160.ZERO : domainKey, contentKey,
                versionKey == null ? Number160.ZERO : versionKey, data);
    }

    public PutBuilder setData(final Number160 domainKey, final Number160 contentKey, final Data data) {
        return setData(locationKey, domainKey, contentKey, versionKey == null ? Number160.ZERO : versionKey,
                data);
    }

    public PutBuilder setData(final Data data, final Number160 versionKey) {
        return setData(locationKey, domainKey == null ? Number160.ZERO : domainKey, Number160.ZERO,
                versionKey, data);
    }

    public PutBuilder setData(final Number160 contentKey, final Data data, final Number160 versionKey) {
        return setData(locationKey, domainKey == null ? Number160.ZERO : domainKey, contentKey, versionKey,
                data);
    }

    public PutBuilder setData(final Number160 locationKey, final Number160 domainKey,
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
    public PutBuilder setDomainKey(final Number160 domainKey) {
        // if we set data before we set domain key, we need to adapt the domain key of the data object
        if (data != null) {
            setData(data.getKey().getLocationKey(), domainKey, data.getKey().getContentKey(), data.getKey()
                    .getVersionKey(), data.getValue());
        }
        super.setDomainKey(domainKey);
        return this;
    }

    @Override
    public PutBuilder setVersionKey(final Number160 versionKey) {
        // if we set data before we set domain key, we need to adapt the domain key of the data object
        if (data != null) {
            setData(data.getKey().getLocationKey(), data.getKey().getDomainKey(), data.getKey()
                    .getContentKey(), versionKey, data.getValue());
        }
        super.setVersionKey(versionKey);
        return this;
    }

    public PutBuilder setObject(Object object) throws IOException {
        return setData(new Data(object));
    }

    public PutBuilder setKeyObject(Number160 contentKey, Object object) throws IOException {
        return setData(contentKey, new Data(object));
    }

    public Map<Number640, Data> getDataMap() {
        return dataMap;
    }

    public PutBuilder setDataMap(Map<Number640, Data> dataMap) {
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
    
    public boolean isPutMeta() {
        return putMeta;
    }

    public PutBuilder putMeta(boolean putMeta) {
        this.putMeta = putMeta;
        return this;
    }

    public PutBuilder putMeta() {
        this.putMeta = true;
        return this;
    }
    
    public PutBuilder changePublicKey(PublicKey changePublicKey) {
    	this.changePublicKey = changePublicKey;
    	this.putMeta = true;
    	setSign();
    	return this;
    }

	public boolean isPutConfirm() {
		return putConfim;
	}

	public PutBuilder putConfirm() {
		this.putConfim = true;
		return this;
	}

	public boolean isPutReject() {
		return putReject;
	}

	public PutBuilder putReject() {
		this.putReject = true;
		return this;
	}

    public PublicKey changePublicKey() {
    	return changePublicKey;
    }

    public FuturePut start() {
        if (peer.isShutdown()) {
            return FUTURE_SHUTDOWN;
        }
        preBuild("put-builder");
        if (data != null) {
            if (dataMap == null) {
                setDataMap(new HashMap<Number640, Data>(1));
            }
            getDataMap().put(getData().getKey(), getData().getValue());
        }
        if (!putMeta && dataMap == null && dataMapConvert == null) {
            throw new IllegalArgumentException(
                    "You must either set data via setDataMap() or setData(). Cannot add nothing.");
        }
        if (!putConfim && dataMap == null && dataMapConvert == null) {
            throw new IllegalArgumentException(
                    "You must either set data via setDataMap() or setData(). Cannot add nothing.");
        }
        if (!putReject && dataMap == null && dataMapConvert == null) {
            throw new IllegalArgumentException(
                    "You must either set data via setDataMap() or setData(). Cannot add nothing.");
        }
        if (putConfim && putReject) {
            throw new IllegalArgumentException(
                    "You can't confirm and reject a put.");
        }
        if (locationKey == null) {
            throw new IllegalArgumentException("You must provide a location key.");
        }
        if (domainKey == null) {
            domainKey = Number160.ZERO;
        }
        if (versionKey == null) {
            versionKey = Number160.ZERO;
        }

        return peer.getDistributedHashMap().put(this);
    }
}
