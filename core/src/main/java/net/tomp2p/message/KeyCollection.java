package net.tomp2p.message;

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.utils.Utils;

public class KeyCollection {
    private final Collection<Number640> keys;
    private final Collection<Number160> keysConvert;
    private final Number160 locationKey;
    private final Number160 domainKey;
    private final Number160 versionKey;

    public KeyCollection(final Number160 locationKey, final Number160 domainKey, final Number160 versionKey,
            final Collection<Number160> keysConvert) {
        this.keys = null;
        this.keysConvert = keysConvert;
        this.locationKey = locationKey;
        this.domainKey = domainKey;
        this.versionKey = versionKey;
    }

    public KeyCollection(final Collection<Number640> keys) {
        this.keys = keys;
        this.keysConvert = null;
        this.locationKey = null;
        this.domainKey = null;
        this.versionKey = null;
    }

    public Collection<Number640> keys() {
        return keys;
    }

    public Collection<Number160> keysConvert() {
        return keysConvert;
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
    public KeyCollection add(final Number640 number640) {
        keys.add(number640);
        return this;
    }

    private Collection<Number640> convert(final KeyCollection k) {
        final Collection<Number640> keys3;
        if (k.keysConvert != null) {
            keys3 = new ArrayList<Number640>(k.keysConvert.size());
            for (Number160 n160 : k.keysConvert) {
                keys3.add(new Number640(k.locationKey, k.domainKey, k.versionKey, n160));
            }
        } else {
            keys3 = k.keys;
        }
        return keys3;
    }
    
    @Override
    public int hashCode() {
        final Collection<Number640> keys = convert(this);
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
        final Collection<Number640> keys2 = convert(this);
        final Collection<Number640> keys3 = convert(k);
        return Utils.isSameSets(keys2, keys3);
    }
}
