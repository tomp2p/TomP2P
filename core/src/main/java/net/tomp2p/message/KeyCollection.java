package net.tomp2p.message;

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.peers.Number256;
import net.tomp2p.utils.Utils;

public class KeyCollection {
    private final Collection<Object> keys;
    private final Collection<Number256> keysConvert;
    private final Number256 locationKey;
    private final Number256 domainKey;
    private final Number256 versionKey;

    public KeyCollection(final Number256 locationKey, final Number256 domainKey, final Number256 versionKey,
                         final Collection<Number256> keysConvert) {
        this.keys = null;
        this.keysConvert = keysConvert;
        this.locationKey = locationKey;
        this.domainKey = domainKey;
        this.versionKey = versionKey;
    }

    public KeyCollection(final Collection<Object> keys) {
        this.keys = keys;
        this.keysConvert = null;
        this.locationKey = null;
        this.domainKey = null;
        this.versionKey = null;
    }

    public Collection<Object> keys() {
        return convert(this);
    }

    public Collection<Number256> keysConvert() {
        return keysConvert;
    }

    public Number256 locationKey() {
        return locationKey;
    }

    public Number256 domainKey() {
        return domainKey;
    }

    public Number256 versionKey() {
        return versionKey;
    }

    /**
     * @return The size of either the set with the number480 as key, or set with the number160 as key
     */
    public int size() {
        if (keys != null) {
            return keys.size();
        } else if (keysConvert != null) {
            return keysConvert.size();
        }
        return 0;
    }

    /**
     * @return True if we have number160 stored and we need to add the location and domain key
     */
    public boolean isConvert() {
        return keysConvert != null;
    }

    /**
     * @param number640
     *            Add this number to the number480 set
     * @return This class
     */
    public KeyCollection add(final Object number640) {
        keys.add(number640);
        return this;
    }

    private Collection<Object> convert(final KeyCollection k) {
        final Collection<Object> keys3;
        if (k.keysConvert != null) {
            keys3 = new ArrayList<Object>(k.keysConvert.size());
            for (Number256 n160 : k.keysConvert) {
                //keys3.add(new Object(k.locationKey, k.domainKey, n160, k.versionKey));
            }
        } else {
            keys3 = k.keys;
        }
        return keys3;
    }
    
    @Override
    public int hashCode() {
        final Collection<Object> keys = convert(this);
        return keys.hashCode();
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof KeyCollection)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        final KeyCollection k = (KeyCollection) obj;
        final Collection<Object> keys2 = convert(this);
        final Collection<Object> keys3 = convert(k);
        return Utils.isSameSets(keys2, keys3);
    }
}
