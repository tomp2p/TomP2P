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
import java.util.Map;

import net.tomp2p.dht.FutureDigest;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.replication.IndirectReplication;
import net.tomp2p.rpc.DigestResult;
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
        System.out.println("we found the data on:");
        for(Map.Entry<PeerAddress, DigestResult> entry: futureDigest.rawDigest().entrySet()) {
        	System.out.println("peer " + entry.getKey()+" reported " +entry.getValue().keyDigest().size());
        }
        // now peer1 gets to know peer2 and peer3, transfer the data
        peer1.peer().bootstrap().peerAddress(peer2.peerAddress()).start().awaitUninterruptibly();
        peer1.peer().bootstrap().peerAddress(peer3.peerAddress()).start().awaitUninterruptibly();
        
        Thread.sleep(ONE_SECOND);
        futureDigest = peer1.digest(new Number160(nr3)).start();
        futureDigest.awaitUninterruptibly();
        System.out.println("we found the data on:");
        for(Map.Entry<PeerAddress, DigestResult> entry: futureDigest.rawDigest().entrySet()) {
        	System.out.println("peer " + entry.getKey()+" reported " +entry.getValue().keyDigest().size());
        }
        //the result shows in the command line 1, 1, 1. This behavior is explaind as follows:
        //
        //The bootstrap from peer1 to peer2 causes peer 1 to replicate the data object to peer2
        //The bootstrap from peer1 to peer3 causes the peer3 to become the new resposible peer. Thus, peer1 
        //transfers the data to peer3 and all three peers have the data.
        //
        //Now consider the following scenario, if we change the order of the bootstrap, so that peer1 first
        //bootstraps to peer3 and then to peer2, we will see 1, 0, 1 in the command line. This behavior is explained
        //as follows:
        //
        //First peer1 will figure out that peer3 is repsonsible and transfer the data to this peer. Then peer1 bootstraps to 
        //peer2. However, as peer1 is not responsible anymore and peer3 does not know yet peer2, peer 2 won't see that data
        //until peer3 does the periodical replication check.
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
