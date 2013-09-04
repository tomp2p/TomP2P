/*
 * Copyright 2013 Thomas Bocek
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

import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test the counting bloom filter and the regular bloom filter.
 * 
 * @author Thomas Bocek
 * 
 */
public class TestBloomFilter {

    private final int bfSize = 40;
    private final int bfSizeLarge = 200;

    /**
     * Test the serialization and if the bloomfilter works as expected.
     */
    @Test
    public void testBloomfilter() {
        Random rnd = new Random(0);
        final int filterSize = 100;
        final int expected = 1000;
        SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(filterSize, expected);
        for (int i = 0; i < expected; i++) {
            bloomFilter.add(new Number160(rnd));
        }
        bloomFilter.add(Number160.MAX_VALUE);

        // convert and back
        ByteBuf buf = Unpooled.buffer(filterSize + SimpleBloomFilter.SIZE_HEADER);
        bloomFilter.toByteBuf(buf);
        SimpleBloomFilter<Number160> bloomFilter2 = new SimpleBloomFilter<Number160>(buf);
        Assert.assertEquals(true, bloomFilter2.contains(Number160.MAX_VALUE));
        Assert.assertEquals(false, bloomFilter2.contains(Number160.ONE));
        Assert.assertEquals(bloomFilter, bloomFilter2);
    }

    /**
     * Test with a small set of additions.
     */
    @Test
    public void testCountingBloomFilter() {
        final int countingSize = 10;
        int[] counting = new int[countingSize];
        CountingBloomFilter<String> cbs = new CountingBloomFilter<String>(bfSize, counting);

        cbs.add("abc");
        cbs.add("abc");
        cbs.add("abc");
        cbs.add("abd");
        cbs.add("abe");

        // CHECKSTYLE:OFF
        Assert.assertEquals(3, cbs.approximateCount("abc"));
        // CHECKSTYLE:ON
        Assert.assertEquals(1, cbs.approximateCount("abd"));
        Assert.assertEquals(0, cbs.approximateCount("abg"));

    }

    /**
     * Test with a large set of additions.
     */
    @Test
    public void testCountingBloomFilter2() {
        final int countingSize = 20;
        final int items = 100;
        final int error = 8;
        int[] counting = new int[countingSize];
        CountingBloomFilter<String> cbs = new CountingBloomFilter<String>(bfSizeLarge, counting);

        System.out.println(cbs.expectedFalsePositiveProbability());

        for (int i = 0; i < items; i++) {
            cbs.add("abc");
        }
        for (int i = 0; i < items; i++) {
            cbs.add("abc" + i);
        }

        // here we show the false negatives. Actually, we should get 100, but
        // since we inserted also other stuff, we get
        // more.
        Assert.assertEquals(items + error, cbs.approximateCount("abc"));

    }

    /**
     * Test with a small set of additions.
     */
    @Test
    public void testCountingBloomFilter3() {
        final int countingSize = 10;
        int[] counting = new int[countingSize];
        CountingBloomFilter<String> cbs = new CountingBloomFilter<String>(bfSize, counting);

        cbs.add("abc");
        cbs.add("abc");
        cbs.add("abc");
        cbs.add("abd");
        cbs.add("abe");

        Assert.assertEquals(true, cbs.contains("abc"));
    }
}
