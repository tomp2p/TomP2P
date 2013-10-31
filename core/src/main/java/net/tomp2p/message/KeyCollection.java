package net.tomp2p.message;

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.utils.Utils;

public class KeyCollection {
    private final Collection<Number480> keys;
    private final Collection<Number160> keysConvert;
    private final Number160 locationKey;
    private final Number160 domainKey;

    public KeyCollection(final Number160 locationKey, final Number160 domainKey,
            final Collection<Number160> keysConvert) {
        this.keys = null;
        this.keysConvert = keysConvert;
        this.locationKey = locationKey;
        this.domainKey = domainKey;
    }

    public KeyCollection(final Collection<Number480> keys) {
        this.keys = keys;
        this.keysConvert = null;
        this.locationKey = null;
        this.domainKey = null;
    }

    public Collection<Number480> keys() {
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
     * @param number480
     *            Add this number to the number480 set
     * @return This class
     */
    public KeyCollection add(final Number480 number480) {
        keys.add(number480);
        return this;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof KeyCollection)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        KeyCollection k = (KeyCollection) obj;
        final Collection<Number480> keys2 = convert(this);
        final Collection<Number480> keys3 = convert(k);
        return Utils.isSameSets(keys2, keys3);
    }

    private Collection<Number480> convert(final KeyCollection k) {
        final Collection<Number480> keys3;
        if (k.keysConvert != null) {
            keys3 = new ArrayList<Number480>(k.keysConvert.size());
            for (Number160 n160 : k.keysConvert) {
                keys3.add(new Number480(k.locationKey, k.domainKey, n160));
            }
        } else {
            keys3 = k.keys;
        }
        return keys3;
    }
}
