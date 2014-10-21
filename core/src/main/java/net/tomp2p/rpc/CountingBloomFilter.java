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

package net.tomp2p.rpc;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

/**
 * A counting Bloom Filter (see http://en.wikipedia.org/wiki/Bloom_filter) that uses java.util.Random as a primitive
 * hash function, and which implements Java's Set interface for convenience. Only the add(), addAll(), contains(), and
 * containsAll() methods are implemented. Calling any other method will yield an UnsupportedOperationException. This
 * code may be used, modified, and redistributed provided that the author tag below remains intact.
 * 
 * @author Ian Clarke <ian@uprizer.com>
 * @author Thomas Bocek <tom@tomp2p.net> Made a counting bloomfliter based on the simple bloom filter.
 * @param <E>
 *            The type of object the BloomFilter should contain
 */
public class CountingBloomFilter<E> implements Set<E>, Serializable {
    private static final long serialVersionUID = 3527833617516722215L;

    private final int k;

    private final int[] intSet;

    private final int intArraySize, expectedElements;

    /**
     * Constructs a CountingBloomFilter out of existing data. You must specify the number of bits in the Bloom Filter,
     * and also you should specify the number of items you expect to add. The latter is used to choose some optimal
     * internal values to minimize the false-positive rate (which can be estimated with expectedFalsePositiveRate()).
     * 
     * @param expectedElements
     *            he typical number of items you expect to be added to the CountingBloomFilter (often called 'n').
     * @param intSet
     *            The data that will be used in the backing BitSet
     */
    public CountingBloomFilter(int expectedElements, int[] intSet) {
        this.intArraySize = intSet.length;
        this.expectedElements = expectedElements;
        this.k = (int) Math.ceil((intArraySize / (double) expectedElements) * Math.log(2.0));
        this.intSet = intSet;
    }

    /**
     * Calculates the approximate probability of the contains() method returning true for an object that had not
     * previously been inserted into the bloom filter. This is known as the "false positive probability".
     * 
     * @return The estimated false positive rate
     */
    public double expectedFalsePositiveProbability() {
        return Math.pow((1 - Math.exp(-k * (double) expectedElements / intArraySize)), k);
    }

    /**
     * Returns the expected elements that was provided by the user.
     * 
     * @return The expected elements that was provided by the user
     */
    public int expectedElements() {
        return expectedElements;
    }

    /*
     * @return This method will always return false
     * 
     * @see java.util.Set#add(java.lang.Object)
     */
    @Override
    public boolean add(E o) {
        Random r = new Random(o.hashCode());
        for (int x = 0; x < k; x++) {
            int index = r.nextInt(intArraySize);
            int old = intSet[index];
            if (old != Integer.MAX_VALUE) {
                intSet[index] = old + 1;
            }

        }
        return false;
    }

    /**
     * @param c
     *            The collection to add
     * @return This method will always return false
     */
    public boolean addAll(Collection<? extends E> c) {
        for (E o : c) {
            add(o);
        }
        return false;
    }

    /**
     * Clear the Bloom Filter
     */
    public void clear() {
        for (int x = 0; x < intSet.length; x++) {
            intSet[x] = 0;
        }
    }

    /**
     * @param o
     *            The object to compare
     * @return False indicates that o was definitely not added to this Bloom Filter, true indicates that it probably
     *         was. The probability can be estimated using the expectedFalsePositiveProbability() method.
     */
    public boolean contains(Object o) {
        Random r = new Random(o.hashCode());
        for (int x = 0; x < k; x++) {
            if (intSet[r.nextInt(intArraySize)] == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param c
     *            The collection to check if its inside this bloom filter
     * @return true if the collection contains all the values
     */
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Not implemented
     */
    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not implemented
     */
    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not implemented
     */
    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not implemented
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not implemented
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not implemented
     */
    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not implemented
     */
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not implemented
     */
    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the bitset that backs the bloom filter
     * 
     * @return bloom filter as a bitset
     */
    public int[] intSet() {
        return intSet;
    }

    /**
     * Returns the number of times that an element has been added. This is an approximate value and can be larger but
     * never smaller than the actual number of additions.
     * 
     * @param key
     *            The key to count
     * @return The number of approximate additions of this object
     */
    public int approximateCount(final E key) {
        int retVal = Integer.MAX_VALUE;
        Random r = new Random(key.hashCode());
        for (int x = 0; x < k; x++) {
            retVal = Math.min(retVal, intSet[r.nextInt(intArraySize)]);
        }
        return retVal;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof CountingBloomFilter)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        @SuppressWarnings("unchecked")
        CountingBloomFilter<E> o = (CountingBloomFilter<E>) obj;
        return o.k == k && o.intArraySize == intArraySize && expectedElements == o.expectedElements
                && Arrays.equals(intSet, o.intSet);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        final int magic = 31;
        hash = magic * hash + Arrays.hashCode(intSet);
        hash = magic * hash + k;
        hash = magic * hash + expectedElements;
        hash = magic * hash + intArraySize;
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int length = intSet.length;
        for (int i = 0; i < length; i++) {
            sb.append(intSet[i]);
        }
        return sb.toString();
    }
}
