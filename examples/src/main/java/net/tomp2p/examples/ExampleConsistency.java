/*
 * Copyright 2009 Thomas Bocek
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

package net.tomp2p.examples;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.Statistics;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.storage.Data;

/**
 * Example of consistency and DHT attacks.
 * 
 * @author Thomas Bocek
 * 
 */
public final class ExampleConsistency {

    /**
     * Empty constructor.
     */
    private ExampleConsistency() {
    }

    private static final Random RND = new Random(5467656537115L);

    private static final RequestP2PConfiguration REQUEST_3 = new RequestP2PConfiguration(3, 10, 0);
    private static final RequestP2PConfiguration REQUEST_6 = new RequestP2PConfiguration(6, 10, 0);

    /**
     * Start the examples.
     * 
     * @param args
     *            Empty
     * @throws Exception .
     */
    public static void main(final String[] args) throws Exception {
        PeerDHT master = null;
        try {
            final int peerNr = 100;
            final int port = 4001;
            PeerDHT[] peers = ExampleUtils.createAndAttachPeersDHT(peerNr, port);
            master = peers[0];
            ExampleUtils.bootstrap(peers);
            Number160 key1 = new Number160(RND);
            exampleConsistency(key1, peers);
            exampleAttack(key1, peers);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (master != null) {
                master.shutdown();
            }
        }
    }

    /**
     * Starts the consistency examples. First we find which peers (0x480137e1846dca42894ca653b60f1446f7ca9cd4) are
     * responsible for a particular key (0x4bca44fd09461db1981e387e99e41e7d22d06894):
     * 
     * @param peers
     *            All the peers
     * @param key1
     *            The key to store the data
     * @throws IOException .
     * @throws ClassNotFoundException .
     */
    private static void exampleConsistency(final Number160 key1, final PeerDHT[] peers) throws IOException,
            ClassNotFoundException {
        System.out.println("key is " + key1);
        // find close peers
        NavigableSet<PeerAddress> set = new TreeSet<PeerAddress>(PeerMap.createComparator(key1));
        for (PeerDHT peer : peers) {
            set.add(peer.peerAddress());
        }
        System.out.println("closest peer " + set.first());
        
        final int peerStore1 = 22;
        peers[peerStore1].put(key1).requestP2PConfiguration(REQUEST_3).data(new Data("Test 1")).start()
                .awaitUninterruptibly();
        // close peers go offline
        System.out.println("the following peers go offline");
        final int peerOffline1 = 67;
        final int peerOffline2 = 40;
        final int peerOffline3 = 39;
        System.out.println(peers[peerOffline1].peerAddress());
        System.out.println(peers[peerOffline2].peerAddress());
        System.out.println(peers[peerOffline3].peerAddress());
        peers[peerOffline1].shutdown().awaitListenersUninterruptibly();
        peers[peerOffline2].shutdown().awaitListenersUninterruptibly();
        peers[peerOffline3].shutdown().awaitListenersUninterruptibly();
        // now lets store something else with the same key
        final int peerGet = 33;
        FuturePut futurePut = peers[peerStore1].put(key1).requestP2PConfiguration(REQUEST_3)
                .data(new Data("Test 2")).start();
        futurePut.awaitUninterruptibly();
        System.out.println("stored [Test 2] on " + futurePut.rawResult().keySet());

        FutureGet futureGet = peers[peerGet].get(key1).all().start();
        futureGet.awaitUninterruptibly();
        System.out.println("peer[" + peerGet + "] got [" + futureGet.data().object() + "] should be [Test 2]");
        // peer 11 and 8 joins again
        peers[peerOffline1] = new PeerBuilderDHT(new PeerBuilder(peers[peerOffline1].peerID()).masterPeer(peers[0].peer()).start()).start();
        peers[peerOffline2] = new PeerBuilderDHT(new PeerBuilder(peers[peerOffline2].peerID()).masterPeer(peers[0].peer()).start()).start();
        peers[peerOffline3] = new PeerBuilderDHT(new PeerBuilder(peers[peerOffline3].peerID()).masterPeer(peers[0].peer()).start()).start();
        peers[peerOffline1].peer().bootstrap().peerAddress(peers[0].peerAddress()).start().awaitUninterruptibly();
        peers[peerOffline2].peer().bootstrap().peerAddress(peers[0].peerAddress()).start().awaitUninterruptibly();
        peers[peerOffline3].peer().bootstrap().peerAddress(peers[0].peerAddress()).start().awaitUninterruptibly();
        // load old data
        System.out.println("The 3 peers are now onlyne again, with the old data");
        Number640 key = new Number640(key1, Number160.ZERO, Number160.ZERO, Number160.ZERO);
        peers[peerOffline1].storageLayer()
                .put(key, new Data("Test 1"), null, false, false, false);
        peers[peerOffline2].storageLayer()
                .put(key, new Data("Test 1"), null, false, false, false);
        peers[peerOffline3].storageLayer()
                .put(key, new Data("Test 1"), null, false, false, false);
        // we got Test 1
        FutureGet futureGet2 = peers[0].get(key1).requestP2PConfiguration(REQUEST_3).all().start();
        futureGet2.awaitUninterruptibly();
        System.out.println("peer[0] got [" + futureGet2.data().object() + "] should be [Test 2]");
        // we got Test 1!
        FutureGet futureGet3 = peers[peerGet].get(key1).requestP2PConfiguration(REQUEST_3).all().start();
        futureGet3.awaitUninterruptibly();
        System.out.println("peer[" + peerGet + "] got [" + futureGet3.data().object() + "] should be [Test 2]");
    }

    /**
     * This example shows how to attack an DHT.
     * 
     * @param key1
     *            The key to attack
     * @param peers
     *            All the peers
     * @throws InterruptedException 
     * @throws IOException .
     * @throws ClassNotFoundException .
     */
    private static void exampleAttack(final Number160 key1, final PeerDHT[] peers) throws IOException,
            ClassNotFoundException, InterruptedException {
        // lets attack!
        System.out.println("Lets ATTACK!");
        PeerDHT mpeer1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x4bca44fd09461db1981e387e99e41e7d22d06893"))
                .masterPeer(peers[0].peer()).start()).start();
        PeerDHT mpeer2 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x4bca44fd09461db1981e387e99e41e7d22d06892"))
                .masterPeer(peers[0].peer()).start()).start();
        PeerDHT mpeer3 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x4bca44fd09461db1981e387e99e41e7d22d06895"))
                .masterPeer(peers[0].peer()).start()).start();
        mpeer1.peer().bootstrap().peerAddress(peers[0].peerAddress()).start().awaitUninterruptibly();
        mpeer2.peer().bootstrap().peerAddress(peers[0].peerAddress()).start().awaitUninterruptibly();
        mpeer3.peer().bootstrap().peerAddress(peers[0].peerAddress()).start().awaitUninterruptibly();
        // load old data
        Number640 key = new Number640(key1, Number160.ZERO, Number160.ZERO, Number160.ZERO);
        mpeer1.storageLayer()
                .put(key, new Data("attack, attack, attack!"), null, false, false, false);
        mpeer2.storageLayer()
                .put(key, new Data("attack, attack, attack!"), null, false, false, false);
        //uncomment below if you want to attack with 3 close peers
        //mpeer3.getPeerBean().storage()
        //        .put(key, new Data("attack, attack, attack!"), null, false, false);
        
        //wait for mainenance pings
        Thread.sleep(3000);
        
        // we got attack!
        FutureGet futureGet = peers[0].get(key1).all().requestP2PConfiguration(REQUEST_3).start();
        futureGet.awaitUninterruptibly();
        System.out.println("peer[0] got " + futureGet.data().object());
        for (Entry<PeerAddress, Map<Number640, Data>> entry : futureGet.rawData().entrySet()) {
            System.out.print("got from (3)" + entry.getKey());
            System.out.println(entry.getValue());
        }
        // increase the replicas we fetch
        FutureGet futureGet1 = peers[0].get(key1).all().requestP2PConfiguration(REQUEST_6).start();
        futureGet1.awaitUninterruptibly();
        System.out.println("peer[0] got " + futureGet1.data().object());

        // countermeasure - statistics, pick not closest, but random peer that has the data - freshness vs. load
        // also, check distances! 
        Statistics statistics = new Statistics(peers[0].peerBean().peerMap());
        System.out.println("average distance: "+statistics.avgGap());
        for (Entry<PeerAddress, Map<Number640, Data>> entry : futureGet1.rawData().entrySet()) {
            System.out.print("got from (6)" + entry.getKey());
            System.out.print(" distance: "+key1.xor(entry.getKey().peerId()).doubleValue());
            System.out.println(" "+entry.getValue());
        }
    }
}
