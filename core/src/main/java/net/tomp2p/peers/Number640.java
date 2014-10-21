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

package net.tomp2p.peers;

import java.util.Random;

/**
 * This class stores the location, domain, content and version keys.
 * 
 * @author Thomas Bocek
 * 
 */
public final class Number640 extends Number implements Comparable<Number640> {
    private static final long serialVersionUID = 1L;

	public static final Number640 ZERO = new Number640(Number480.ZERO, Number160.ZERO);

    private final Number160 locationKey;

    private final Number160 domainKey;

    private final Number160 contentKey;
    
    private final Number160 versionKey;

    /**
     * Creates a new Number640 key from given location, domain, content and version keys.
     * 
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param contentKey
     *            The content key
     * @param versionKey
     * 			  The version key
     */
    public Number640(final Number160 locationKey, final Number160 domainKey, final Number160 contentKey, final Number160 versionKey) {
        if (locationKey == null) {
            throw new RuntimeException("locationKey cannot be null");
        }
        this.locationKey = locationKey;
        if (domainKey == null) {
            throw new RuntimeException("domainKey cannot be null");
        }
        this.domainKey = domainKey;
        if (contentKey == null) {
            throw new RuntimeException("contentKey cannot be null");
        }
        this.contentKey = contentKey;
        if (versionKey == null) {
            throw new RuntimeException("versionKey cannot be null");
        }
        this.versionKey = versionKey;
    }

    /**
     * Creates a new Number640 key from given location, domain, content and version keys.
     * 
     * @param key
     *            The location, domain and content key
     * @param versionKey
     *            The version key
     */
    public Number640(final Number480 key, final Number160 versionKey) {
        this(key.locationKey(), key.domainKey(), key.contentKey(), versionKey);
    }
    
    /**
     * Creates a new Number640 key from given location, domain, content and version keys.
     * 
     * @param key
     *            The location and domain key
     * @param contentKey
     * 			  The content key
     * @param versionKey
     *            The version key
     */
    public Number640(final Number320 key, final Number160 contentKey, final Number160 versionKey) {
        this(key.locationKey(), key.domainKey(), contentKey, versionKey);
    }

    /**
     * Creates a new random Number640 key.
     * 
     * @param rnd
     *            The random class
     */
    public Number640(final Random rnd) {
        this(new Number160(rnd), new Number160(rnd), new Number160(rnd), new Number160(rnd));
    }

    /**
     * @return The location key
     */
    public Number160 locationKey() {
        return locationKey;
    }

    /**
     * @return The domain key
     */
    public Number160 domainKey() {
        return domainKey;
    }

    /**
     * @return The content key
     */
    public Number160 contentKey() {
        return contentKey;
    }
    
    /**
     * @return The version key
     */
    public Number160 versionKey() {
        return versionKey;
    }

    @Override
    public int hashCode() {
        return locationKey.hashCode() ^ domainKey.hashCode() ^ contentKey.hashCode() ^ versionKey.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Number640)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        Number640 cmp = (Number640) obj;
        
        boolean t1 = locationKey.equals(cmp.locationKey);
        boolean t2 = domainKey.equals(cmp.domainKey);
        boolean t3 = contentKey.equals(cmp.contentKey);
        boolean t4 = versionKey.equals(cmp.versionKey);
        
        return t1 && t2 && t3 && t4;
    }

    @Override
    public int compareTo(final Number640 o) {
        int diff = locationKey.compareTo(o.locationKey);
        if (diff != 0) {
            return diff;
        }
        diff = domainKey.compareTo(o.domainKey);
        if (diff != 0) {
            return diff;
        }
        diff = contentKey.compareTo(o.contentKey);
        if (diff != 0) {
            return diff;
        }
        return versionKey.compareTo(o.versionKey);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        sb.append(locationKey.toString()).append(",");
        sb.append(domainKey.toString()).append(",");
        sb.append(contentKey.toString()).append(",");
        sb.append(versionKey.toString()).append("]");
        return sb.toString();
    }

    @Override
    public int intValue() {
        return contentKey.intValue();
    }

    @Override
    public long longValue() {
        return contentKey.longValue();
    }

    @Override
    public float floatValue() {
        return (float) doubleValue();
    }

    @Override
    public double doubleValue() {
        return (locationKey.doubleValue() * Math.pow(2, Number160.BITS * 3))
                + (domainKey.doubleValue() * Math.pow(2, Number160.BITS * 2)) 
                + (contentKey.doubleValue() * Math.pow(2, Number160.BITS * 1))
                + versionKey.doubleValue();
    }
    
    public Number640 minVersionKey() {
        return new Number640(locationKey, domainKey, contentKey, Number160.ZERO);
    }
    
    public Number640 minContentKey() {
        return new Number640(locationKey, domainKey, Number160.ZERO, Number160.ZERO);
    }
    
    public Number640 maxVersionKey() {
        return new Number640(locationKey, domainKey, contentKey, Number160.MAX_VALUE);
    }
    
    public Number640 maxContentKey() {
        return new Number640(locationKey, domainKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
    }
    
    public Number320 locationAndDomainKey() {
        return new Number320(locationKey, domainKey);
    }
    
    public Number480 locationAndDomainAndContentKey() {
        return new Number480(locationKey, domainKey, contentKey);
    }
}
