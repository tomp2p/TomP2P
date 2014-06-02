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
 * This class stores the location, domain, and content key.
 * 
 * @author Thomas Bocek
 * 
 */
public final class Number480 extends Number implements Comparable<Number480> {
    private static final long serialVersionUID = 1L;

	public static final Number480 ZERO = new Number480(Number320.ZERO, Number160.ZERO);

    private final Number160 locationKey;

    private final Number160 domainKey;

    private final Number160 contentKey;

    /**
     * Constructor with a given location key, domain, and content key.
     * 
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param contentKey
     *            The content key
     */
    public Number480(final Number160 locationKey, final Number160 domainKey, final Number160 contentKey) {
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
    }

    /**
     * Constructor with a given location key, domain, and content key.
     * 
     * @param key
     *            The location and domain key
     * @param contentKey
     *            The content key
     */
    public Number480(final Number320 key, final Number160 contentKey) {
        this(key.locationKey(), key.domainKey(), contentKey);
    }

    /**
     * Constructor that creates a random 480bit number.
     * 
     * @param rnd
     *            The random class
     */
    public Number480(final Random rnd) {
        this(new Number160(rnd), new Number160(rnd), new Number160(rnd));
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

    @Override
    public int hashCode() {
        return locationKey.hashCode() ^ domainKey.hashCode() ^ contentKey.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Number480)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        Number480 cmp = (Number480) obj;
        return locationKey.equals(cmp.locationKey) && domainKey.equals(cmp.domainKey)
                && contentKey.equals(cmp.contentKey);
    }

    @Override
    public int compareTo(final Number480 o) {
        int diff = locationKey.compareTo(o.locationKey);
        if (diff != 0) {
            return diff;
        }
        diff = domainKey.compareTo(o.domainKey);
        if (diff != 0) {
            return diff;
        }
        return contentKey.compareTo(o.contentKey);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        sb.append(locationKey.toString()).append(",");
        sb.append(domainKey.toString()).append(",");
        sb.append(contentKey.toString()).append("]");
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
        return (locationKey.doubleValue() * Math.pow(2, Number160.BITS * 2))
                + (domainKey.doubleValue() * Math.pow(2, Number160.BITS)) + contentKey.doubleValue();
    }
}
