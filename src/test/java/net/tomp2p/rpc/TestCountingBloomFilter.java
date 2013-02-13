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

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author Thomas Bocek
 */
public class TestCountingBloomFilter {
    private final int bfSize = 40;
    private final int bfSizeLarge = 200;

    /**
     * Test with a small set of additions.
     */
    @Test
    public void testCountingBloomFilter() {
        int[] counting = new int[10];
        CountingBloomFilter<String> cbs = new CountingBloomFilter<String>(bfSize, counting);

        cbs.add("abc");
        cbs.add("abc");
        cbs.add("abc");
        cbs.add("abd");
        cbs.add("abe");

        Assert.assertEquals(3, cbs.approximateCount("abc"));
        Assert.assertEquals(1, cbs.approximateCount("abd"));
        Assert.assertEquals(0, cbs.approximateCount("abg"));

    }

    /**
     * Test with a large set of additions.
     */
    @Test
    public void testCountingBloomFilter2() {
        int[] counting = new int[2 * 10];
        CountingBloomFilter<String> cbs = new CountingBloomFilter<String>(bfSizeLarge, counting);

        System.out.println(cbs.expectedFalsePositiveProbability());

        for (int i = 0; i < 100; i++) {
            cbs.add("abc");
        }
        for (int i = 0; i < 100; i++) {
            cbs.add("abc" + i);
        }

        // here we show the false negatives. Actually, we should get 100, but
        // since we inserted also other stuff, we get
        // more.
        Assert.assertEquals(100 + 8, cbs.approximateCount("abc"));

    }

    /**
     * Test with a small set of additions.
     */
    @Test
    public void testCountingBloomFilter3() {
        int[] counting = new int[10];
        CountingBloomFilter<String> cbs = new CountingBloomFilter<String>(bfSize, counting);

        cbs.add("abc");
        cbs.add("abc");
        cbs.add("abc");
        cbs.add("abd");
        cbs.add("abe");

        Assert.assertEquals(true, cbs.contains("abc"));
    }
}
