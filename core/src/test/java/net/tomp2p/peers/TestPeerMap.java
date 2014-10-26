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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.Utils2;
import net.tomp2p.connection.PeerException;
import net.tomp2p.connection.PeerException.AbortCause;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestPeerMap {

    private static final Number160 ID = new Number160("0x1");
    
    @Test
    public void testDifference() throws UnknownHostException {
        // setup
        Collection<PeerAddress> newC = new ArrayList<PeerAddress>();
        newC.add(Utils2.createAddress(12));
        newC.add(Utils2.createAddress(15));
        newC.add(Utils2.createAddress(88));
        newC.add(Utils2.createAddress(90));
        newC.add(Utils2.createAddress(91));
        SortedSet<PeerAddress> result = new TreeSet<PeerAddress>(PeerMap.createComparator(new Number160(88)));
        SortedSet<PeerAddress> already = new TreeSet<PeerAddress>(PeerMap.createComparator(new Number160(88)));
        already.add(Utils2.createAddress(90));
        already.add(Utils2.createAddress(15));
        // do testing
        Utils.difference(newC, result, already);
        // verification
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(Utils2.createAddress(88), result.first());
    }

    @Test
    public void testAdd1() {
        PeerMapConfiguration conf = new PeerMapConfiguration(ID);
        conf.bagSizeVerified(4).bagSizeOverflow(4);
        conf.offlineCount(1000).offlineTimeout(60);
        conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
        PeerMap peerMap = new PeerMap(conf);

        Number160 id1 = new Number160("0x2");
        Number160 id2 = new Number160("0x3");
        Number160 id3 = new Number160("0x4");
        Number160 id4 = new Number160("0x5");
        Number160 id5 = new Number160("0x6");
        Number160 id6 = new Number160("0x7");
        Number160 id11 = id1.xor(ID);
        Number160 id22 = id2.xor(ID);
        Number160 id33 = id3.xor(ID);
        Assert.assertEquals(2, id11.bitLength());
        Assert.assertEquals(2, id22.bitLength());
        Assert.assertEquals(3, id33.bitLength());
        Assert.assertEquals(0, PeerMap.classMember(Number160.ZERO, ID));
        Assert.assertEquals(1, PeerMap.classMember(Number160.ZERO, id1));
        Assert.assertEquals(159, PeerMap.classMember(Number160.ZERO, Number160.MAX_VALUE));
        //
        PeerAddress pa1 = new PeerAddress(id1);
        PeerAddress pa2 = new PeerAddress(id2);
        PeerAddress pa3 = new PeerAddress(id3);
        PeerAddress pa4 = new PeerAddress(id4);
        PeerAddress pa5 = new PeerAddress(id5);
        PeerAddress pa6 = new PeerAddress(id6);
        peerMap.peerFound(pa1, null, null);
        peerMap.peerFound(pa2, null, null);
        peerMap.peerFound(pa3, null, null);
        peerMap.peerFound(pa4, null, null);
        peerMap.peerFound(pa5, null, null);
        peerMap.peerFound(pa6, null, null);
        SortedSet<PeerAddress> pa = peerMap.closePeers(ID, 2);
        Assert.assertEquals(2, pa.size());
        Iterator<PeerAddress> iterator = pa.iterator();
        Assert.assertEquals("0x3", iterator.next().peerId().toString());
        Assert.assertEquals("0x2", iterator.next().peerId().toString());
        pa = peerMap.closePeers(id3, 3);
        Assert.assertEquals(4, pa.size());
        iterator = pa.iterator();
        Assert.assertEquals("0x4", iterator.next().peerId().toString());
        Assert.assertEquals("0x5", iterator.next().peerId().toString());
        Assert.assertEquals("0x6", iterator.next().peerId().toString());
        Assert.assertEquals("0x7", iterator.next().peerId().toString());
    }

    @Test
    public void testAdd2() {
        PeerMapConfiguration conf = new PeerMapConfiguration(ID);
        conf.bagSizeVerified(3).bagSizeOverflow(3);
        conf.offlineCount(1000).offlineTimeout(60);
        conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
        PeerMap peerMap = new PeerMap(conf);

        Number160 id1 = new Number160("0x2");
        Number160 id2 = new Number160("0x3");
        Number160 id3 = new Number160("0x4");
        Number160 id4 = new Number160("0x5");
        Number160 id5 = new Number160("0x6");
        Number160 id6 = new Number160("0x7");
        //
        PeerAddress pa1 = new PeerAddress(id1);
        PeerAddress pa2 = new PeerAddress(id2);
        PeerAddress pa3 = new PeerAddress(id3);
        PeerAddress pa4 = new PeerAddress(id4);
        PeerAddress pa5 = new PeerAddress(id5);
        PeerAddress pa6 = new PeerAddress(id6);
        peerMap.peerFound(pa1, null, null);
        peerMap.peerFound(pa2, null, null);
        peerMap.peerFound(pa3, null, null);
        peerMap.peerFound(pa4, null, null);
        peerMap.peerFound(pa5, null, null);
        peerMap.peerFound(pa6, null, null);
        SortedSet<PeerAddress> pa = peerMap.closePeers(ID, 2);
        Assert.assertEquals(2, pa.size());
        Iterator<PeerAddress> iterator = pa.iterator();
        Assert.assertEquals("0x3", iterator.next().peerId().toString());
        Assert.assertEquals("0x2", iterator.next().peerId().toString());
        pa = peerMap.closePeers(id3, 3);
        Assert.assertEquals(3, pa.size());
        iterator = pa.iterator();
        Assert.assertEquals("0x4", iterator.next().peerId().toString());
        Assert.assertEquals("0x5", iterator.next().peerId().toString());
        Assert.assertEquals("0x6", iterator.next().peerId().toString());
        // 0x7 is in the non-verified / overflow map
        List<PeerAddress> list = peerMap.allOverflow();
        Assert.assertEquals("0x7", list.iterator().next().peerId().toString());
    }

    @Test
    public void testLength() {
        Number160 bi1 = new Number160("0x127");
        Number160 bi2 = new Number160("0x128");
        Number160 bi3 = new Number160("0x255");
        Number160 rr = PeerMap.distance(bi1, bi2);
        Assert.assertFalse(bi3.equals(rr));
        bi1 = new Number160("0x7f");
        bi2 = new Number160("0x80");
        bi3 = new Number160("0xff");
        rr = PeerMap.distance(bi1, bi2);
        Assert.assertTrue(bi3.equals(rr));
        Assert.assertEquals(7, PeerMap.classMember(bi1, bi2));
        Assert.assertEquals(6, PeerMap.classMember(bi2, bi3));
    }

    @Test
    public void testCloser() throws UnknownHostException {
        PeerAddress rn1 = new PeerAddress(new Number160("0x7f"));
        PeerAddress rn2 = new PeerAddress(new Number160("0x40"));
        Number160 key = new Number160("0xff");
        Assert.assertEquals(-1, PeerMap.isCloser(key, rn1, rn2));
        //
        rn1 = new PeerAddress(new Number160("0x10"));
        rn2 = new PeerAddress(new Number160("0x11"));
        key = new Number160("0xff");
        System.err.println("0x7f xor 0xff " + rn1.peerId().xor(key));
        System.err.println("0x40 xor 0xff " + rn2.peerId().xor(key));
        Assert.assertEquals(1, PeerMap.isCloser(key, rn1, rn2));
    }

    @Test
    public void testCloser2() throws UnknownHostException {
        PeerAddress rn1 = new PeerAddress(new Number160(98));
        PeerAddress rn2 = new PeerAddress(new Number160(66));
        PeerAddress rn3 = new PeerAddress(new Number160(67));
        PeerMapConfiguration conf = new PeerMapConfiguration(ID);
        conf.bagSizeVerified(3).bagSizeOverflow(3);
        conf.offlineCount(1000).offlineTimeout(60);
        conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
        PeerMap peerMap = new PeerMap(conf);
        SortedSet<PeerAddress> rc = peerMap.closePeers(new Number160(98), 3);
        rc.add(rn2);
        rc.add(rn1);
        rc.add(rn3);
        Assert.assertTrue(rc.first().equals(rn1));
    }

    @Test
    public void testAddNode() throws UnknownHostException {
        PeerMapConfiguration conf = new PeerMapConfiguration(ID);
        conf.bagSizeVerified(4).bagSizeOverflow(4);
        conf.offlineCount(1000).offlineTimeout(60);
        conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
        PeerMap peerMap = new PeerMap(conf);
        for (int i = 1; i < 12; i++) {
            PeerAddress r1 = new PeerAddress(new Number160(i));
            peerMap.peerFound(r1, null, null);
        }
        SortedSet<PeerAddress> close = peerMap.closePeers(new Number160(2), 2);
        Assert.assertEquals(2, close.size());
        close = peerMap.closePeers(new Number160(6), 4);
        Assert.assertEquals(4, close.size());
    }

    @Test
    public void testAddNode2() throws UnknownHostException {
        PeerMapConfiguration conf = new PeerMapConfiguration(ID);
        conf.bagSizeVerified(3).bagSizeOverflow(3);
        conf.offlineCount(1000).offlineTimeout(60);
        conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
        PeerMap peerMap = new PeerMap(conf);
        for (int i = 1; i < 12; i++) {
            PeerAddress r1 = new PeerAddress(new Number160((i % 6) + 1));
            peerMap.peerFound(r1, null, null);
        }
        SortedSet<PeerAddress> close = peerMap.closePeers(new Number160(2), 2);
        Assert.assertEquals(2, close.size());
        close = peerMap.closePeers(new Number160(6), 1);
        Assert.assertEquals(3, close.size());
    }

    @Test
    public void testRemove() throws UnknownHostException, InterruptedException {
        PeerMapConfiguration conf = new PeerMapConfiguration(ID);
        conf.bagSizeVerified(3).bagSizeOverflow(3);
        conf.offlineCount(1000).offlineTimeout(1);
        conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
        PeerMap peerMap = new PeerMap(conf);
        for (int i = 1; i <= 200; i++) {
            PeerAddress r1 = new PeerAddress(new Number160(i + 1));
            peerMap.peerFound(r1, null, null);
        }
        Assert.assertEquals(20, peerMap.size());
        peerMap.peerFailed(new PeerAddress(new Number160(100)), new PeerException(AbortCause.PROBABLY_OFFLINE, "probably offline"));
        Assert.assertTrue(peerMap.isPeerRemovedTemporarly(new PeerAddress(new Number160(100))));
        Assert.assertEquals(20, peerMap.size());
        peerMap.peerFailed(new PeerAddress(new Number160(2)), new PeerException(AbortCause.PROBABLY_OFFLINE, "probably offline"));
        Assert.assertEquals(19, peerMap.size());
        Assert.assertTrue(peerMap.isPeerRemovedTemporarly(new PeerAddress(new Number160(2))));
        Thread.sleep(1000);
        Assert.assertFalse(peerMap.isPeerRemovedTemporarly(new PeerAddress(new Number160(2))));
        Assert.assertFalse(peerMap.isPeerRemovedTemporarly(new PeerAddress(new Number160(100))));

    }

    @Test
    public void testRemoveConcurrent() throws UnknownHostException, InterruptedException {
        PeerMapConfiguration conf = new PeerMapConfiguration(ID);
        conf.bagSizeVerified(3).bagSizeOverflow(3);
        conf.offlineCount(1000).offlineTimeout(1);
        conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
        final PeerMap peerMap = new PeerMap(conf);
        for (int i = 1; i <= 200; i++) {
            PeerAddress r1 = new PeerAddress(new Number160(i + 1));
            peerMap.peerFound(r1, null, null);
        }
        Assert.assertEquals(20, peerMap.size());
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 1; i <= 50; i++) {
                    peerMap.peerFailed(new PeerAddress(new Number160(i + 1)), new PeerException(AbortCause.SHUTDOWN, "shutdown"));
                }
            }
        });
        t1.start();
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 1; i <= 100; i++) {
                    peerMap.peerFailed(new PeerAddress(new Number160(i + 1)), new PeerException(AbortCause.SHUTDOWN, "shutdown"));
                }
            }
        });
        t2.start();

        t1.join();
        t2.join();

        Assert.assertTrue(peerMap.isPeerRemovedTemporarly(new PeerAddress(new Number160(100))));
        Assert.assertEquals(3, peerMap.size());
    }

    @Test
    public void testAddConcurrent() throws UnknownHostException, InterruptedException {
        PeerMapConfiguration conf = new PeerMapConfiguration(ID);
        conf.bagSizeVerified(3).bagSizeOverflow(3);
        conf.offlineCount(1000).offlineTimeout(1);
        conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
        final PeerMap peerMap = new PeerMap(conf);
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 1; i <= 50; i++) {
                    peerMap.peerFound(new PeerAddress(new Number160(i + 1)), null, null);
                }
            }
        });
        t1.start();
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 1; i <= 100; i++) {
                    peerMap.peerFound(new PeerAddress(new Number160(i + 1)), null, null);
                }
            }
        });
        t2.start();

        t1.join();
        t2.join();

        Assert.assertEquals(17, peerMap.size());
    }

    /**
     * Repeating test of testRandomAddRemove() to test for concurrency issues.
     * 
     * @throws InterruptedException .
     */
    @Test
    public void testMultiRandomAddRemove() throws InterruptedException {
        final int rounds = 100;
        for (int i = 0; i < rounds; i++) {
            testRandomAddRemove();
        }
    }

    /**
     * Tests the peermap and concurrent adds and puts. There will be two times 5000 inserts and 4989 removes. That means
     * the resulting peer count needs to be 11.
     * 
     * @throws InterruptedException .
     */
    @Test
    public void testRandomAddRemove() throws InterruptedException {
        for (int j = 0; j < 50; j++) {
            PeerMapConfiguration conf = new PeerMapConfiguration(ID);
            conf.bagSizeVerified(j + 1).bagSizeOverflow(j + 1);
            conf.offlineCount(1000).offlineTimeout(1);
            conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
            final PeerMap peerMap = new PeerMap(conf);

            final AtomicInteger add = new AtomicInteger();
            final AtomicInteger del = new AtomicInteger();
            final int rounds = 500;
            final int diff = 10;

            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = 1; i <= rounds + diff; i++) {
                        if (i + diff < rounds) {
                            boolean retVal = peerMap.peerFound(new PeerAddress(new Number160(i + 1)), null, null);
                            if (retVal) {
                                add.incrementAndGet();
                            }
                        }
                        if (i - diff > 1) {
                            boolean retVal = peerMap.peerFailed(new PeerAddress(new Number160(i - diff)), new PeerException(AbortCause.SHUTDOWN, "shutdown"));
                            if (retVal) {
                                del.incrementAndGet();
                            }
                        }

                    }
                }
            };

            Thread t1 = new Thread(runnable);
            Thread t2 = new Thread(runnable);
            t1.start();
            t2.start();
            t1.join();
            t2.join();
            System.err.println("inserted: " + add.get() + ", removed: " + del.get());
            Assert.assertEquals(0, peerMap.size());
        }
    }

    @Test
    public void testPerformance() throws IOException {
        PeerMapConfiguration conf = new PeerMapConfiguration(ID);
        conf.bagSizeVerified(3).bagSizeOverflow(3);
        conf.offlineCount(1000).offlineTimeout(100);
        conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
        final PeerMap peerMap = new PeerMap(conf);
        final Random random = new Random(42L);
        final List<PeerAddress> listAdded = new ArrayList<PeerAddress>();
        final List<PeerAddress> listRemoved = new ArrayList<PeerAddress>();
        long start = System.currentTimeMillis();
        int size = 500000;
        for (int i = 1; i <= size; i++) {
            PeerAddress r1 = new PeerAddress(new Number160(random));
            listAdded.add(r1);
        }
        for (PeerAddress r1 : listAdded) {
            peerMap.peerFound(r1, null, null);
        }
        for (PeerAddress r1 : listAdded) {
            peerMap.peerFound(r1, null, null);
        }
        for (int i = 0; i < 100; i++) {
            PeerAddress removed = listAdded.get(random.nextInt(i + 1));
            if (peerMap.peerFailed(removed, new PeerException(AbortCause.SHUTDOWN, "shutdown"))) {
                listRemoved.add(removed);
            }
        }
        for (PeerAddress r1 : listAdded) {
            peerMap.peerFound(r1, r1, null);
        }
        Assert.assertEquals(47, peerMap.size());
        for (PeerAddress r1 : listRemoved) {
            Assert.assertTrue(peerMap.isPeerRemovedTemporarly(r1));
        }
        //
        for (PeerAddress r1 : listRemoved) {
            listAdded.remove(r1);
        }
        for (int i = 0; i < 300; i++) {
            PeerAddress removed = listAdded.get(random.nextInt(i + 1));
            peerMap.peerFailed(removed, new PeerException(AbortCause.SHUTDOWN, "shutdown"));
        }
        for (PeerAddress r1 : listRemoved) {
            Assert.assertTrue(peerMap.isPeerRemovedTemporarly(r1));
        }
        System.err.println("Time used: " + (System.currentTimeMillis() - start)
                + " ms. (time to beat ~5100ms, now ~1800ms)");
    }
    
    @Test
    public void testOrder() {
        Number160 b1 = new Number160("0x5");
        Number160 b2 = new Number160("0x32");
        Number160 b3 = new Number160("0x1F4");
        Number160 b4 = new Number160("0x1388");
        PeerAddress n1 = new PeerAddress(b1);
        PeerAddress n2 = new PeerAddress(b2);
        PeerAddress n3 = new PeerAddress(b3);
        PeerAddress n4 = new PeerAddress(b4);
        final NavigableSet<PeerAddress> queue = new TreeSet<PeerAddress>(PeerMap.createComparator(b3));
        queue.add(n1);
        queue.add(n2);
        queue.add(n3);
        queue.add(n4);
        Assert.assertEquals(queue.pollFirst(), n3);
        Assert.assertEquals(queue.pollFirst(), n2);
        Assert.assertEquals(queue.pollLast(), n4);
    }
    
    @Test
    public void testMaintenance() throws UnknownHostException, InterruptedException {
        PeerMapConfiguration conf = new PeerMapConfiguration(ID);
        conf.bagSizeVerified(10).bagSizeOverflow(10);
        conf.offlineCount(1000).offlineTimeout(100);
        conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
        
        conf.maintenance(new DefaultMaintenance(4, new int[] { 1, 1 }));
        
        final PeerMap peerMap = new PeerMap(conf);
        
        PeerAddress pa1 = Utils2.createAddress(Number160.createHash("peer 1"));
        PeerAddress pa2 = Utils2.createAddress(Number160.createHash("peer 2"));
        
        peerMap.peerFound(pa2, pa1, null);
        List<PeerAddress> notInterested = new ArrayList<PeerAddress>();
        PeerStatistic peerStatatistic = peerMap.nextForMaintenance(notInterested);
        notInterested.add(peerStatatistic.peerAddress());
        Assert.assertEquals(peerStatatistic.peerAddress(), pa2);
        peerStatatistic = peerMap.nextForMaintenance(notInterested);
        Assert.assertEquals(true, peerStatatistic == null);
        
        PeerAddress pa3 = Utils2.createAddress(Number160.createHash("peer 3"));
        peerMap.peerFound(pa3, null, null);
        peerStatatistic = peerMap.nextForMaintenance(notInterested);
        Assert.assertEquals(true, peerStatatistic == null);
        Thread.sleep(1000);
        peerStatatistic = peerMap.nextForMaintenance(notInterested);
        Assert.assertEquals(peerStatatistic.peerAddress(), pa3);
    }

    @Test
    public void testClose() throws UnknownHostException {
        for (int i = 1; i < 30; i++) {
            testClose(i);
        }
    }

    private void testClose(int round) throws UnknownHostException {
        Random rnd = new Random(round);
        for (int j = 0; j < 1000; j++) {

            Number160 key = new Number160(rnd);
            PeerMapConfiguration conf = new PeerMapConfiguration(ID);
            conf.bagSizeVerified(10).bagSizeOverflow(10);
            conf.offlineCount(1000).offlineTimeout(100);
            conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
            final PeerMap peerMap = new PeerMap(conf);
            List<PeerAddress> peers = new ArrayList<PeerAddress>();
            for (int i = 0; i < round; i++) {
                PeerAddress r1 = new PeerAddress(new Number160(rnd));
                peers.add(r1);
                peerMap.peerFound(r1, null, null);
            }

            TreeSet<PeerAddress> set = new TreeSet<PeerAddress>(PeerMap.createComparator(key));
            set.addAll(peers);
            PeerAddress closest1 = set.iterator().next();
            PeerAddress closest2 = peerMap.closePeers(key, 1).iterator().next();
            if (peerMap.allOverflow().size() == 0) {
                Assert.assertEquals(closest1, closest2);
            }
        }
    }
}
