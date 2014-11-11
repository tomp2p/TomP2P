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

/**
 * This class stores the location and domain key.
 * 
 * @author Thomas Bocek
 * 
 */
public final class Number320 extends Number implements Comparable<Number320> {
    private static final long serialVersionUID = -7200924461230885512L;

	public static final Number320 ZERO = new Number320(Number160.ZERO, Number160.ZERO);

    private final Number160 locationKey;

    private final Number160 domainKey;

    /**
     * Creates a new Number320 key from given location and domain keys.
     * 
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     */
    public Number320(final Number160 locationKey, final Number160 domainKey) {
        if (locationKey == null) {
            throw new RuntimeException("locationKey cannot be null");
        }
        this.locationKey = locationKey;
        if (domainKey == null) {
            throw new RuntimeException("domainKey cannot be null");
        }
        this.domainKey = domainKey;
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

    @Override
    public int hashCode() {
        return locationKey.hashCode() ^ domainKey.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Number320)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        Number320 cmp = (Number320) obj;
        return locationKey.equals(cmp.locationKey) && domainKey.equals(cmp.domainKey);
    }

    @Override
    public int compareTo(final Number320 o) {
        int diff = locationKey.compareTo(o.locationKey);
        if (diff != 0) {
            return diff;
        }
        return domainKey.compareTo(o.domainKey);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        sb.append(locationKey.toString()).append(",");
        sb.append(domainKey.toString()).append("]");
        return sb.toString();
    }

    /**
     * @return The minimum value of a content key
     */
    public Number480 minContentKey() {
        return new Number480(locationKey, domainKey, Number160.ZERO);
    }

    /**
     * @return The maxium value of a content key
     */
    public Number480 maxContentKey() {
        return new Number480(locationKey, domainKey, Number160.MAX_VALUE);
    }

    @Override
    public int intValue() {
        return domainKey.intValue();
    }

    @Override
    public long longValue() {
        return domainKey.longValue();
    }

    @Override
    public float floatValue() {
        return (float) doubleValue();
    }

    @Override
    public double doubleValue() {
        return (locationKey.doubleValue() * Math.pow(2, Number160.BITS)) + domainKey.doubleValue();
    }
}
