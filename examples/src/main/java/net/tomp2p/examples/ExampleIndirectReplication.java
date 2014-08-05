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

package net.tomp2p.examples;

import java.io.IOException;

import net.tomp2p.dht.FutureDigest;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.replication.IndirectReplication;
import net.tomp2p.storage.Data;

/**
 * Example of indirect replication with put.
 * 
 * @author Thomas Bocek
 * 
 */
public final class ExampleIndirectReplication {

    private static final int ONE_SECOND = 1000;

    /**
     * Empty constructor.
     */
    private ExampleIndirectReplication() {
    }

    /**
     * Create 3 peers and start the example.
     * 
     * @param args
     *            Empty
     * @throws Exception .
     */
    public static void main(final String[] args) throws Exception {
        exmpleIndirectReplication();
    }

    /**
     * Example of indirect replication with put. We store data in the DHT, then peers join that are closer to this data.
     * The indirect replication moves the content to the close peers.
     * 
     * @throws IOException .
     * @throws InterruptedException .
     */
    private static void exmpleIndirectReplication() throws IOException, InterruptedException {
        final int port1 = 4001;
        final int nr1 = 1;
        final int port2 = 4002;
        final int nr2 = 2;
        final int port3 = 4003;
        final int nr3 = 4;
        PeerDHT peer1 = new PeerBuilderDHT(new PeerBuilder(new Number160(nr1)).ports(port1).start()).start();
        PeerDHT peer2 = new PeerBuilderDHT(new PeerBuilder(new Number160(nr2)).ports(port2).start()).start();
        PeerDHT peer3 = new PeerBuilderDHT(new PeerBuilder(new Number160(nr3)).ports(port3).start()).start();
        
        new IndirectReplication(peer1).start();
        new IndirectReplication(peer2).start();
        new IndirectReplication(peer3).start();
        
        
        PeerDHT[] peers = new PeerDHT[] {peer1, peer2, peer3};
        //
        FuturePut futurePut = peer1.put(new Number160(nr3)).data(new Data("store on peer1")).start();
        futurePut.awaitUninterruptibly();
        FutureDigest futureDigest = peer1.digest(new Number160(nr3)).start();
        futureDigest.awaitUninterruptibly();
        System.out.println("we found the data on " + futureDigest.rawDigest().size() + " peers");
        // now peer1 gets to know peer2, transfer the data
        peer1.peer().bootstrap().peerAddress(peer2.peerAddress()).start();
        peer1.peer().bootstrap().peerAddress(peer3.peerAddress()).start();
        Thread.sleep(ONE_SECOND);
        futureDigest = peer1.digest(new Number160(nr3)).start();
        futureDigest.awaitUninterruptibly();
        System.out.println("we found the data on " + futureDigest.rawDigest().size() + " peers");
        shutdown(peers);
    }

    /**
     * Shutdown all the peers.
     * 
     * @param peers
     *            The peers in this P2P network
     */
    private static void shutdown(final PeerDHT[] peers) {
        for (PeerDHT peer : peers) {
            peer.shutdown();
        }
    }
}
