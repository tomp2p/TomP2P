package net.tomp2p.message;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class DataMap {
    private final NavigableMap<Number640, Data> dataMap;
    private final NavigableMap<Number160, Data> dataMapConvert;
    private final Number160 locationKey;
    private final Number160 domainKey;
    private final Number160 versionKey;
    private final boolean convertMeta;
    
    public DataMap(final NavigableMap<Number640, Data> dataMap) {
    	this(dataMap, false);
    }

    public DataMap(final NavigableMap<Number640, Data> dataMap, boolean convertMeta) {
        this.dataMap = dataMap;
        this.dataMapConvert = null;
        this.locationKey = null;
        this.domainKey = null;
        this.versionKey = null;
        this.convertMeta = convertMeta;
    }
    
    public DataMap(final Number160 locationKey, final Number160 domainKey, final Number160 versionKey,
            final NavigableMap<Number160, Data> dataMapConvert) {
    	this(locationKey, domainKey, versionKey, dataMapConvert, false);
    }

    public DataMap(final Number160 locationKey, final Number160 domainKey, final Number160 versionKey,
            final NavigableMap<Number160, Data> dataMapConvert, boolean convertMeta) {
        this.dataMap = null;
        this.dataMapConvert = dataMapConvert;
        this.locationKey = locationKey;
        this.domainKey = domainKey;
        this.versionKey = versionKey;
        this.convertMeta = convertMeta;
    }
    
    public boolean isConvertMeta() {
    	return convertMeta;
    }

    public NavigableMap<Number640, Data> dataMap() {
        return convert(this);
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

    public Number160 versionKey() {
        return versionKey;
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

    public NavigableMap<Number640, Number160> convertToHash() {
    	NavigableMap<Number640, Number160> retVal = new TreeMap<Number640, Number160>();
        if (dataMap != null) {
            for (Map.Entry<Number640, Data> entry : dataMap.entrySet()) {
                retVal.put(entry.getKey(), entry.getValue().hash());
            }

        } else if (dataMapConvert != null) {
            for (Map.Entry<Number160, Data> entry : dataMapConvert.entrySet()) {
                retVal.put(new Number640(locationKey, domainKey, entry.getKey(), versionKey), entry
                        .getValue().hash());
            }
        }
        return retVal;
    }

    private static NavigableMap<Number640, Data> convert(final DataMap d) {
        final NavigableMap<Number640, Data> dataMap3;
        if (d.dataMapConvert != null) {
            dataMap3 = new TreeMap<Number640, Data>();
            for (Map.Entry<Number160, Data> entry : d.dataMapConvert.entrySet()) {
                dataMap3.put(new Number640(d.locationKey, d.domainKey, entry.getKey(), d.versionKey),
                        entry.getValue());
            }
        } else {
            dataMap3 = d.dataMap;
        }
        return dataMap3;
    }
    
    @Override
	public int hashCode() {
		int hashCode = 31;
		final Map<Number640, Data> dataMap = convert(this);
		for (Map.Entry<Number640, Data> entry : dataMap.entrySet()) {
			hashCode ^= entry.getKey().hashCode();
			if (entry.getValue() != null) {
				hashCode ^= entry.getValue().hashCode();
			}
		}
		return hashCode;
	}

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof DataMap)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        
        final Map<Number640, Data> dataMapThis = convert(this);
        final Map<Number640, Data> dataMapOther = convert((DataMap) obj);
        final boolean test1 = Utils.isSameSets(dataMapThis.keySet(), dataMapOther.keySet());
        final boolean test2 = Utils.isSameSets(dataMapThis.values(), dataMapOther.values());
        return test1 && test2;
    }
}
