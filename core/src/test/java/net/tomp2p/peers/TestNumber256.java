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

package net.tomp2p.peers;

import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestNumber256 {
    private final Random rnd = new Random();
    
    @Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };

    @Test
    public void testLength() {
        Number256 bi1 = new Number256("0x7f");
        Number256 bi2 = new Number256("0x80");
        Number256 bi3 = new Number256("0xff");
        Assert.assertEquals(false, bi1.compareTo(bi2) == 0);
        Assert.assertEquals(bi3, PeerMap.distance(bi1, bi2));
        Assert.assertEquals(7, PeerMap.classMember(bi1, bi2));
        Assert.assertEquals(6, PeerMap.classMember(bi2, bi3));
    }

    @Test
    public void testXor() {
        BigInteger bi1 = new BigInteger("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", 16);
        Number256 ki1 = new Number256("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        BigInteger bi2 = new BigInteger("357116889007843534245232322114545905234a688432435674546621211677", 16);
        Number256 ki2 = new Number256("0x357116889007843534245232322114545905234a688432435674546621211677");
        Assert.assertEquals("0x" + bi1.toString(16), ki1.toString());
        Assert.assertEquals("0x" + bi2.toString(16), ki2.toString());
        Number256 ki3 = ki1.xor(ki2);
        BigInteger bi3 = bi1.xor(bi2);
        Assert.assertEquals("0x" + bi3.toString(16), ki3.toString());
    }

    @Test
    public void testRandomXor() {
        for (int i = 0; i < 1000; i++) {
            BigInteger bi1 = new BigInteger(256, rnd);
            BigInteger bi2 = new BigInteger(256, rnd);
            if (bi1.toString(16).length() == 64 && bi2.toString(16).length() == 64) {
                Number256 ki1 = new Number256("0x" + bi1.toString(16));
                Number256 ki2 = new Number256("0x" + bi2.toString(16));
                //
                Number256 ki3 = ki1.xor(ki2);
                BigInteger bi3 = bi1.xor(bi2);
                String s1 = bi3.toString(16);
                while(s1.length() < 64) s1 = "0" + s1;
                Assert.assertEquals(ki3.toString(), "0x" + s1);
            }
        }
    }

    @Test
    public void testFromIntArray() {
        long[] tmp = new long[] { 1, 2, 3, 4};
        Number256 ki2 = new Number256(tmp);
        Assert.assertEquals("0x0000000000000001000000000000000200000000000000030000000000000004", ki2.toString());
        tmp = new long[] { -1, -1, -1, -1};
        ki2 = new Number256(tmp);
        Assert.assertEquals("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", ki2.toString());
        ki2 = new Number256(new long[] { 1, 2 });
        Assert.assertEquals("0x0000000000000000000000000000000000000000000000010000000000000002", ki2.toString());
        ki2 = new Number256(new long[] { 1 });
        Assert.assertEquals("0x0000000000000000000000000000000000000000000000000000000000000001", ki2.toString());
        ki2 = new Number256(new long[] { 1, 1, 1, 1});
        Assert.assertEquals("0x0000000000000001000000000000000100000000000000010000000000000001", ki2.toString());
    }

    @Test
    public void testFromString() {
        Number256 ki2 = new Number256("0x9");
        Assert.assertEquals("0x0000000000000000000000000000000000000000000000000000000000000009", ki2.toString());
        ki2 = new Number256("0x22");
        Assert.assertEquals("0x0000000000000000000000000000000000000000000000000000000000000022", ki2.toString());
        ki2 = new Number256("0x0000000000000002000000030000000400000005");
        Assert.assertEquals("0x0000000000000000000000000000000000000002000000030000000400000005", ki2.toString());
        ki2 = new Number256("0x0000000000000002000000000000000000000000");
        Assert.assertEquals("0x0000000000000000000000000000000000000002000000000000000000000000", ki2.toString());
        ki2 = new Number256("0x1000000000000000000000000000000000000000");
        Assert.assertEquals("0x0000000000000000000000001000000000000000000000000000000000000000", ki2.toString());
    }

    @Test
    public void testFromInt() {
        Number256 ki2 = new Number256(0x2c);
        Assert.assertEquals("0x000000000000000000000000000000000000000000000000000000000000002c", ki2.toString());
    }

    @Test
    public void testFromByteArray() {
        Number256 ki2 = new Number256(new byte[] { 1 });
        Assert.assertEquals("0x0000000000000000000000000000000000000000000000000000000000000001", ki2.toString());
        ki2 = new Number256(1, 2);
        Assert.assertEquals("0x0000000000000000000000000000000000000000000000010000000000000002", ki2.toString());
        ki2 = new Number256(new byte[] { 1, 2 });
        Assert.assertEquals("0x0000000000000000000000000000000000000000000000000000000000000102", ki2.toString());
        //
        ki2 = new Number256(new byte[] { 1 });
        Number256 ki3 = new Number256(new byte[] { 2 });
        Assert.assertEquals(true, ki2.compareTo(ki3) < 0);
        //
        ki2 = new Number256(new byte[] { 3 });
        ki3 = new Number256(new byte[] { 2 });
        Assert.assertEquals(true, ki2.compareTo(ki3) > 0);
        //
        ki2 = new Number256(new byte[] { 3 });
        ki3 = new Number256(new byte[] { 3 });
        Assert.assertEquals(true, ki2.compareTo(ki3) == 0);
        //
        ki2 = new Number256(new byte[] { 2, 3 });
        ki3 = new Number256(new byte[] { 3, 2 });
        Assert.assertEquals(true, ki2.compareTo(ki3) < 0);
        //
        ki2 = new Number256(new byte[] { 1, 2, 3 });
        ki3 = new Number256(new byte[] { 3, 2 });
        Assert.assertEquals(true, ki2.compareTo(ki3) > 0);
    }

    @Test
    public void testSerialization() {
        Number256 ki2 = new Number256("0x357116889007843534245232322114545905234a");
        byte[] me = ki2.toByteArray();
        Number256 ki3 = new Number256(me);
        Assert.assertEquals(ki2, ki3);
        //
        ki2 = Number256.MAX_VALUE;
        me = ki2.toByteArray();
        ki3 = new Number256(me);
        Assert.assertEquals(ki2, ki3);
        //
        ki2 = new Number256("0x1234567890abcdef1234567890abcdef12345678");
        me = ki2.toByteArray();
        ki3 = new Number256(me);
        Assert.assertEquals(ki2, ki3);
    }

    @Test
    public void testIsZero() {
        Number256 ki2 = new Number256(new byte[] { 0 });
        Number256 ki3 = new Number256(new byte[] { 2 });
        Assert.assertEquals(true, ki2.isZero());
        Assert.assertEquals(false, ki3.isZero());
    }

    @Test
    public void testBitLength() {
        Number256 ki2 = Number256.ONE;
        Assert.assertEquals(1, ki2.bitLength());
        ki2 = new Number256("0x2");
        Assert.assertEquals(2, ki2.bitLength());
        ki2 = new Number256("0x3");
        Assert.assertEquals(2, ki2.bitLength());
        ki2 = new Number256("0xfa");
        Assert.assertEquals(8, ki2.bitLength());
        ki2 = new Number256("0x100000002");
        Assert.assertEquals(33, ki2.bitLength());
        ki2 = new Number256("0x1100000002");
        Assert.assertEquals(37, ki2.bitLength());
        try {
            ki2 = new Number256("0x-1");
            Assert.fail();
        } catch (RuntimeException e) {
        }
        ki2 = Number256.MAX_VALUE;
        Assert.assertEquals(256, ki2.bitLength());
    }

    @Test
    public void testDouble() {
        BigInteger bi1 = new BigInteger("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", 16);
        Number256 ki1 = new Number256("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        BigInteger bi2 = new BigInteger("357116889007843534245232322114545905234a688432435674546621211677", 16);
        Number256 ki2 = new Number256("0x357116889007843534245232322114545905234a688432435674546621211677");
        Assert.assertEquals(0, Double.compare(bi1.doubleValue(), ki1.doubleValue()));
        Assert.assertEquals(0, Double.compare(bi2.doubleValue(), ki2.doubleValue()));
    }

    @Test
    public void testFloat() {
        BigInteger bi1 = new BigInteger("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", 16);
        Number256 ki1 = new Number256("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        BigInteger bi2 = new BigInteger("357116889007843534245232322114545905234a688432435674546621211677", 16);
        Number256 ki2 = new Number256("0x357116889007843534245232322114545905234a688432435674546621211677");
        Assert.assertEquals(0, Float.compare(bi1.floatValue(), ki1.floatValue()));
        Assert.assertEquals(0, Float.compare(bi2.floatValue(), ki2.floatValue()));
    }

    @Test
    public void testLong() {
        Number256 n1 = new Number256(1);
        Number256 n2 = new Number256(1L);
        Assert.assertEquals(n1, n2);
        //
        n2 = new Number256(Long.MAX_VALUE);
        Assert.assertEquals("0x0000000000000000000000000000000000000000000000007FFFFFFFFFFFFFFF".toLowerCase(), n2.toString());
    }

    @Test
    public void testPerformance() {
        int runs = 1000;
        BigInteger bi1[] = new BigInteger[runs];
        BigInteger bi2[] = new BigInteger[runs];
        for (int i = 0; i < runs; i++) {
            bi1[i] = new BigInteger(256, rnd);
            bi2[i] = new BigInteger(256, rnd);
            if (bi1[i].toString(16).length() != 64 || bi2[i].toString(16).length() != 64)
                i--;
        }
        Number256 ki1[] = new Number256[runs];
        Number256 ki2[] = new Number256[runs];
        for (int i = 0; i < runs; i++) {
            ki1[i] = new Number256("0x" + bi1[i].toString(16));
            ki2[i] = new Number256("0x" + bi2[i].toString(16));
        }
        long start = System.currentTimeMillis();
        BigInteger bi3 = BigInteger.TEN;
        for (int j = 1; j < 1000; j++) {
            for (int i = 0; i < runs; i++)
                // int i = 0;
                bi3 = bi1[(i * j) % runs].xor(bi2[i]).xor(bi3);
        }
        System.err.println("BigInteger time " + (System.currentTimeMillis() - start) + ", " + bi3.toString(16));
        start = System.currentTimeMillis();
        Number256 ki3 = new Number256(10);
        for (int j = 1; j < 1000; j++) {
            for (int i = 0; i < runs; i++)
                ki3 = ki1[(i * j) % runs].xor(ki2[i]).xor(ki3);
        }
        System.err.println("Key time " + (System.currentTimeMillis() - start) + ", " + ki3);

        String s1 = bi3.toString(16);
        while(s1.length() < 64) s1 = "0" + s1;
        Assert.assertEquals("0x" + s1, ki3.toString());
    }


}
