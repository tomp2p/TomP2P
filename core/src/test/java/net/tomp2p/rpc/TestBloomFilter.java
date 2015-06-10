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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Random;

import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Test the counting bloom filter and the regular bloom filter.
 * 
 * @author Thomas Bocek
 * 
 */
public class TestBloomFilter {

	private final int bfSize = 40;
    private final int bfSizeLarge = 200;
    
    @Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };
    
    @Test
    public void testEmptyBloomfilter() {
    	SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(0, 0);
    	Number160 hash = new Number160("0x41844ffc9aeea30aefd1e0aff687dacdf1c6f36e");
    	Assert.assertEquals(true, bloomFilter.isVoid());
    	Assert.assertEquals(false, bloomFilter.contains(hash));
    	// convert and back
    	ByteBuf buf = Unpooled.buffer(SimpleBloomFilter.SIZE_HEADER);
        bloomFilter.toByteBuf(buf);
        SimpleBloomFilter<Number160> bloomFilter2 = new SimpleBloomFilter<Number160>(buf);
        Assert.assertEquals(true, bloomFilter2.isVoid());
    }
    
    @Test
    public void testFullBloomfilter() {
    	SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(8, Integer.MAX_VALUE).setAll();
    	Number160 hash = new Number160("0x41844ffc9aeea30aefd1e0aff687dacdf1c6f36e");
    	Assert.assertEquals(true, bloomFilter.isFull());
    	Assert.assertEquals(true, bloomFilter.contains(hash));
    	// convert and back, minimun size is a long
    	ByteBuf buf = Unpooled.buffer(8 + SimpleBloomFilter.SIZE_HEADER);
        bloomFilter.toByteBuf(buf);
        SimpleBloomFilter<Number160> bloomFilter2 = new SimpleBloomFilter<Number160>(buf);
        Assert.assertEquals(true, bloomFilter2.isFull());
    }
    
    @Test
    public void testOneBloomfilter1() {
    	SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(1024, 1000);
    	Number160 hash = new Number160("0x41844ffc9aeea30aefd1e0aff687dacdf1c6f36e");
    	bloomFilter.add(hash);
    	// convert and back, minimun size is a long
    	ByteBuf buf = Unpooled.buffer();
        bloomFilter.toByteBuf(buf);
        SimpleBloomFilter<Number160> bloomFilter2 = new SimpleBloomFilter<Number160>(buf);
        Assert.assertEquals(true, bloomFilter2.contains(hash));
    }
    
    @Test
    public void testOneBloomfilter2() {
    	SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(0.01d, 1000);
    	Number160 hash = new Number160("0x41844ffc9aeea30aefd1e0aff687dacdf1c6f36e");
    	bloomFilter.add(hash);
    	// convert and back, minimun size is a long
    	ByteBuf buf = Unpooled.buffer();
        bloomFilter.toByteBuf(buf);
        SimpleBloomFilter<Number160> bloomFilter2 = new SimpleBloomFilter<Number160>(buf);
        Assert.assertEquals(true, bloomFilter2.contains(hash));
    }
    

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

        Assert.assertEquals(3, cbs.approximateCount("abc"));
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
    
    @Test 
    public void intersect() {
    	for(int i=1;i<100;i++) {
    		intersect((0.001 * i));
    	}
    }
    
    @Test 
    public void intersect1() {
    	intersect(0.01);
    }
    
    private void intersect(double falsePositive) {
    	final int nrPeers = 1000;
        final int range1 = 800;
        final int range2 = 1800;
        
        SimpleBloomFilter<Number160> sbf = new SimpleBloomFilter<Number160>(falsePositive, nrPeers);
        
        for(int i=0;i<nrPeers;i++) {
        	sbf.add(new Number160(i));
        }
        
        int counter = 0;
        for(int i=range1;i<range2;i++) {
        	Number160 test = new Number160(i);
        	if(sbf.contains(test)) {
        		counter ++;
        	}
        	
        }
        //System.err.println("counter "+counter);
        Assert.assertEquals(true, counter>=nrPeers-range1);
    }
}
