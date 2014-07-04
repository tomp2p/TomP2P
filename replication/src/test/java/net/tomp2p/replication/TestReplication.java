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

package net.tomp2p.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import net.tomp2p.Utils2;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.AutomaticFuture;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;


/**
 * 
 * @author Thomas Bocek
 * 
 */
public class TestReplication {
    private static final Random RND2 = new Random(42L);
    private static final int PORT = 4001;
    private static final int NR_PEERS = 100;
    
    @Test
    public void testDataloss() throws IOException, InterruptedException {
        PeerDHT p1 = null;
        PeerDHT p2 = null;
        PeerDHT p3 = null;
        try {
            
            p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash("111")).ports(PORT)
                    .start()).start();
            p2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash("22")).ports(PORT+1)
                    .start()).start();
            p3 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash("33")).ports(PORT+2)
                    .start()).start();
            
            IndirectReplication i1 = new IndirectReplication(p1);
            i1.start();
            IndirectReplication i2 = new IndirectReplication(p2);
            i2.start();
            IndirectReplication i3 = new IndirectReplication(p3);
            i3.start();
            
            Utils2.perfectRouting(p1, p2, p3);
            Number160 locationKey = Number160.createHash("test1");
            FuturePut fp = p2.put(locationKey).data(new Data("hallo")).requestP2PConfiguration(new RequestP2PConfiguration(2, 10, 0)).start();
            fp.awaitUninterruptibly();
            getReplicasCount(locationKey, p1, p2, p3);
            //
            p3.peer().announceShutdown().start().awaitUninterruptibly();
            p3.shutdown().awaitUninterruptibly();
            Thread.sleep(500);
            p3 = new PeerBuilderDHT(new PeerBuilder(locationKey).ports(PORT+3).start()).start();
            i3 = new IndirectReplication(p3);
            i3.start();
            
            System.out.println("now we add a peer that matches perfectly the key " + locationKey+ ". This will now become the responsible peer");
            p3.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
            getReplicasCount(locationKey, p1, p2, p3);
            Thread.sleep(500);
            p1.peer().announceShutdown().start().awaitUninterruptibly();
            p1.shutdown().awaitUninterruptibly();
            p2.peer().announceShutdown().start().awaitUninterruptibly();
            p2.shutdown().awaitUninterruptibly();
            Thread.sleep(500);
            int count = getReplicasCount(locationKey, p1, p2, p3);
            Assert.assertEquals(1, count);
            
        } finally {
            if (p1 != null && !p1.peer().isShutdown()) {
                p1.shutdown().awaitUninterruptibly();
            }
            if (p2 != null && !p2.peer().isShutdown()) {
                p2.shutdown().awaitUninterruptibly();
            }
            if (p3 != null && !p3.peer().isShutdown()) {
                p3.shutdown().awaitUninterruptibly();
            }
        }
    }
    
    @Test
    public void testDataloss2() throws IOException, InterruptedException {
    	PeerDHT p1 = null;
    	PeerDHT p2 = null;
    	PeerDHT p3 = null;
        try {
            
            p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash("111")).ports(PORT)
                    .start()).start();
            p2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash("22")).ports(PORT+1)
                    .start()).start();
            p3 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash("33")).ports(PORT+2)
                    .start()).start();
            
            IndirectReplication i1 = new IndirectReplication(p1);
            i1.start();
            IndirectReplication i2 = new IndirectReplication(p2);
            i2.start();
            IndirectReplication i3 = new IndirectReplication(p3);
            i3.start();
            
            
            
            Utils2.perfectRouting(p1, p2, p3);
            Number160 locationKey = Number160.createHash("test1");
            FuturePut fp = p2.put(locationKey).data(new Data("hallo")).requestP2PConfiguration(new RequestP2PConfiguration(2, 10, 0)).start();
            fp.awaitUninterruptibly();
            getReplicasCount(locationKey, p1, p2, p3);
            
            p3.peer().announceShutdown().start().awaitUninterruptibly();
            p3.shutdown().awaitUninterruptibly();
            p1.peer().announceShutdown().start().awaitUninterruptibly();
            p1.shutdown().awaitUninterruptibly();
            Thread.sleep(500);
            getReplicasCount(locationKey, p1, p2, p3);
            
            //
            
            p3 = new PeerBuilderDHT(new PeerBuilder(locationKey).ports(PORT+3).start()).start();
            i3 = new IndirectReplication(p3);
            i3.start();
            
            System.out.println("now we add a peer that matches perfectly the key " + locationKey+ ". This will now become the responsible peer");
            p3.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
            
            p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash("1111")).ports(PORT+4)
                    .start()).start();
            i1 = new IndirectReplication(p1);
            i1.start();
            
            p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
            
            getReplicasCount(locationKey, p1, p2, p3);
            Thread.sleep(500);
            
            int count = getReplicasCount(locationKey, p1, p2, p3);
            Assert.assertEquals(2, count);
            
        } finally {
        	if (p1 != null && !p1.peer().isShutdown()) {
                p1.shutdown().awaitUninterruptibly();
            }
            if (p2 != null && !p2.peer().isShutdown()) {
                p2.shutdown().awaitUninterruptibly();
            }
            if (p3 != null && !p3.peer().isShutdown()) {
                p3.shutdown().awaitUninterruptibly();
            }
        }
    }
    
    private static int getReplicasCount(Number160 key, PeerDHT... peers) {
        int count = 0;
        Number160 locationKey = null;
        ArrayList<Number160> peerIds = new ArrayList<Number160>();
        for(int i=0; i<peers.length; i++)
            if(!peers[i].peer().isShutdown())
                for(Map.Entry<Number640, Data> entry: peers[i].storageLayer().get().entrySet()){
                    locationKey = entry.getKey().locationKey();
                    if(locationKey.equals(key)){
                        count++;
                        peerIds.add(peers[i].peerID());
                        System.out.println("key " + key + " FoundOn peers["+i+"]=" + peers[i].peerID());
                        //break;
                    }
                }
        System.out.println("key " + key + " FoundOn "+count+" peers");
        return count;
    }
    
    @Test
    public void testSimpleIndirectReplicationForward() throws Exception {
        final Random rnd = new Random(42L);
        PeerDHT master = null;
        try {
            // setup
        	PeerDHT[] peers = Utils2.createNodes(2, rnd, PORT, new AutomaticFuture() {
                @Override
                public void futureCreated(BaseFuture future) {
                    System.err.println("future created "+ future);
                }
            }, true);
            master = peers[0];
            //print out info
            System.err.println("looking for "+searchPeer(Number160.createHash("2"), peers).peerAddress()+" in ");
            for(PeerDHT peer:peers) {
                System.err.println(peer.peerAddress());
            }
            //store data, the two peers do not know each other
            Data data = new Data("Test");
            FuturePut futureDHT = master.put(Number160.createHash("2")).data(data).start();
            futureDHT.awaitUninterruptibly();
            futureDHT.futureRequests().awaitUninterruptibly();
            //now, do the routing, so that each peers know each other. The content should be moved
            Assert.assertEquals(false, peers[1].storageLayer().contains(new Number640(Number160.createHash("2"), Number160.ZERO, Number160.ZERO, Number160.ZERO)));
            Utils2.perfectRouting(peers);
            //we should see now the forward replication
            Thread.sleep(1000);
            //test it
            Assert.assertEquals(true, peers[1].storageLayer().contains(new Number640(Number160.createHash("2"), Number160.ZERO, Number160.ZERO, Number160.ZERO)));
            
        } finally {
            if (master != null) {
                master.shutdown().awaitUninterruptibly();
            }
        }
    }

    /**
     * Test the indirect replication where the closest peer is responsible for a location key.
     * 
     * @throws Exception .
     */
    @Test
    public void testIndirectReplicationForward() throws Exception {
    	PeerDHT master = null;
        try {
            // setup
        	PeerDHT[] peers = Utils2.createNodes(NR_PEERS, RND2, PORT, new AutomaticFuture() {
                @Override
                public void futureCreated(BaseFuture future) {
                    System.err.println("future created "+ future);
                }
            }, true);
            master = peers[0];
            Number160 locationKey = new Number160(RND2);
            master.peerBean().peerMap();
            // closest
            TreeSet<PeerAddress> tmp = new TreeSet<PeerAddress>(PeerMap.createComparator(locationKey));
            tmp.add(master.peerAddress());
            for (int i = 0; i < peers.length; i++) {
                tmp.add(peers[i].peerAddress());
            }
            PeerAddress closest = tmp.iterator().next();
            System.err.println("closest to " + locationKey + " is " + closest);
            // store
            Data data = new Data("Test");
            FuturePut futureDHT = master.put(locationKey).data(data).start();
            futureDHT.awaitUninterruptibly();
            futureDHT.futureRequests().awaitUninterruptibly();
            Assert.assertEquals(true, futureDHT.isSuccess());
            List<FutureBootstrap> tmp2 = new ArrayList<FutureBootstrap>();
            Utils2.perfectRouting(peers);
            //for (int i = 0; i < peers.length; i++) {
            //    if (peers[i] != master) {
            //        tmp2.add(peers[i].bootstrap().setPeerAddress(master.getPeerAddress()).start());
            //    }
            //}
            for (FutureBootstrap fm : tmp2) {
                fm.awaitUninterruptibly();
                Assert.assertEquals(true, fm.isSuccess());
            }
            
            // wait for the replication
            PeerDHT peerClose = searchPeer(closest, peers);
            int i = 0;
            // wait for 2.5 sec
            final int tests = 10;
            final int wait = 2500;
            while (!peerClose.storageLayer()
                    .contains(new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO))) {
                Thread.sleep(wait / tests);
                i++;
                if (i > tests) {
                    break;
                }
            }
            Assert.assertEquals(true, peerClose.storageLayer()
                    .contains(new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO)));
        } finally {
            if (master != null) {
                master.shutdown().awaitUninterruptibly();
            }
        }
    }

    /**
     * Search a Peer in a list.
     * 
     * @param peerAddress
     *            The peer to search for
     * @param peers
     *            The list of peers
     * @return The found peer or null if not found
     */
    private static PeerDHT searchPeer(final PeerAddress peerAddress, final PeerDHT[] peers) {
        for (PeerDHT peer : peers) {
            if (peer.peerAddress().equals(peerAddress)) {
                return peer;
            }
        }
        return null;
    }
    
    private static PeerDHT searchPeer(final Number160 locationKey, final PeerDHT[] peers) {
        TreeSet<PeerAddress> tmp = new TreeSet<PeerAddress>(PeerMap.createComparator(locationKey));
        for (int i = 0; i < peers.length; i++) {
            tmp.add(peers[i].peerAddress());
        }
        PeerAddress peerAddress = tmp.iterator().next();
        return searchPeer(peerAddress, peers);
    }
    
    
    
    
    
    
   
}
