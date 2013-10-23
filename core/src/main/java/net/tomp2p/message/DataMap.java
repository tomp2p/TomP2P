package net.tomp2p.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class DataMap {
    private final Map<Number480, Data> dataMap;
    private final Map<Number160, Data> dataMapConvert;
    private final Number160 locationKey;
    private final Number160 domainKey;

    public DataMap(final Map<Number480, Data> dataMap) {
        this.dataMap = dataMap;
        this.dataMapConvert = null;
        this.locationKey = null;
        this.domainKey = null;
    }

    public DataMap(final Number160 locationKey, final Number160 domainKey,
            final Map<Number160, Data> dataMapConvert) {
        this.dataMap = null;
        this.dataMapConvert = dataMapConvert;
        this.locationKey = locationKey;
        this.domainKey = domainKey;
    }

    public Map<Number480, Data> dataMap() {
        return dataMap;
    }

    public Map<Number160, Data> dataMapConvert() {
        return dataMapConvert;
    }

    public Number160 locationKey() {
        return locationKey;
    }

    public Number160 domainKey() {
        return domainKey;
    }

    /**
     * @return The size of either the datamap with the number480 as key, or datamap with the number160 as key
     */
    public int size() {
        if (dataMap != null) {
            return dataMap.size();
        } else if (dataMapConvert != null) {
            return dataMapConvert.size();
        }
        return 0;
    }

    /**
     * @return True if we have number160 stored and we need to add the location and domain key
     */
    public boolean isConvert() {
        return dataMapConvert != null;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof DataMap)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        DataMap d = (DataMap) obj;
        final Map<Number480, Data> dataMap2 = convert(this);
        final Map<Number480, Data> dataMap3 = convert(d);
        boolean test1 = Utils.isSameSets(dataMap2.keySet(), dataMap3.keySet());
        boolean test2 = Utils.isSameSets(dataMap2.values(), dataMap3.values());
        return test1 && test2;
    }
    
    public Map<Number480, Data> convertToMap480() {
        return convert(this);
    }
    
    public Map<Number480, Number160> convertToHash() {
        Map<Number480, Number160> retVal = new HashMap<Number480, Number160>();
        if (dataMap != null) {
            for (Map.Entry<Number480, Data> entry : dataMap.entrySet()) {
                retVal.put(entry.getKey(), entry.getValue().hash());
            }
            
        } else if (dataMapConvert != null) {
            for (Map.Entry<Number160, Data> entry : dataMapConvert.entrySet()) {
                retVal.put(new Number480(locationKey, domainKey, entry.getKey()), entry.getValue().hash());
            }
        }
        return retVal;
    }

    private static Map<Number480, Data> convert(final DataMap d) {
        final Map<Number480, Data> dataMap3;
        if (d.dataMapConvert != null) {
            dataMap3 = new HashMap<Number480, Data>(d.dataMapConvert.size());
            for (Map.Entry<Number160, Data> entry : d.dataMapConvert.entrySet()) {
                dataMap3.put(new Number480(d.locationKey, d.domainKey, entry.getKey()), entry.getValue());
            }
        } else {
            dataMap3 = d.dataMap;
        }
        return dataMap3;
    }
}
