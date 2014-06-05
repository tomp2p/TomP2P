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
import java.util.Iterator;
import java.util.Random;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

/**
 * This simple example creates 10 nodes, bootstraps to the first and put and get data from those 10 nodes.
 * 
 * @author draft
 */
public final class ExamplePutGet {
    private static final Random RND = new Random(42L);
    private static final int PEER_NR_1 = 30;
    private static final int PEER_NR_2 = 77;
    
    /**
     * Empty constructor.
     */
    private ExamplePutGet() { }

    /**
     * Starts to run the examples.
     * @param args No arguments necessary
     * @throws Exception .
     */
    public static void main(final String[] args) throws Exception {
    	PeerDHT master = null;
        final int nrPeers = 100;
        final int port = 4001;
        final int waitingTime = 250;
        try {
        	PeerDHT[] peers = ExampleUtils.createAndAttachPeersDHT(nrPeers, port);
            ExampleUtils.bootstrap(peers);
            master = peers[0];
            Number160 nr = new Number160(RND);
            examplePutGet(peers, nr);
            examplePutGetConfig(peers, nr);
            exampleGetBlocking(peers, nr);
            exampleGetNonBlocking(peers, nr);
            Thread.sleep(waitingTime);
            exampleAddGet(peers);
        } finally {
            if (master != null) {
                master.shutdown();
            }
        }
    }

    /**
     * Basic example for storing and retrieving content.
     * 
     * @param peers The peers in this P2P network
     * @param nr The number where the data is stored
     * @throws IOException e.
     * @throws ClassNotFoundException .
     */
    private static void examplePutGet(final PeerDHT[] peers, final Number160 nr) 
            throws IOException, ClassNotFoundException {
        FuturePut futurePut = peers[PEER_NR_1].put(nr).data(new Data("hallo")).start();
        futurePut.awaitUninterruptibly();
        System.out.println("peer " + PEER_NR_1 + " stored [key: " + nr + ", value: \"hallo\"]");
        FutureGet futureGet = peers[PEER_NR_2].get(nr).start();
        futureGet.awaitUninterruptibly();
        System.out.println("peer " + PEER_NR_2 + " got: \"" + futureGet.data().object() + "\" for the key " + nr);
        // the output should look like this:
        // peer 30 stored [key: 0xba419d350dfe8af7aee7bbe10c45c0284f083ce4, value: "hallo"]
        // peer 77 got: "hallo" for the key 0xba419d350dfe8af7aee7bbe10c45c0284f083ce4
    }

    private static void examplePutGetConfig( PeerDHT[] peers, Number160 nr2 )
        throws IOException, ClassNotFoundException
    {
        Number160 nr = new Number160( RND );
        FuturePut futurePut =
            peers[30].put( nr ).data( new Number160( 11 ), new Data( "hallo" ) ).domainKey( Number160.createHash( "my_domain" ) ).start();
        futurePut.awaitUninterruptibly();
        System.out.println( "peer 30 stored [key: " + nr + ", value: \"hallo\"]" );
        // this will fail, since we did not specify the domain
        FutureGet futureGet = peers[77].get( nr ).all().start();
        futureGet.awaitUninterruptibly();
        System.out.println( "peer 77 got: \"" + futureGet.data() + "\" for the key " + nr );
        // this will succeed, since we specify the domain
        futureGet =
            peers[77].get( nr ).all().domainKey( Number160.createHash( "my_domain" ) ).start().awaitUninterruptibly();
        System.out.println( "peer 77 got: \"" + futureGet.data().object() + "\" for the key " + nr );
        // the output should look like this:
        // peer 30 stored [key: 0x8992a603029824e810fd7416d729ef2eb9ad3cfc, value: "hallo"]
        // peer 77 got: "hallo" for the key 0x8992a603029824e810fd7416d729ef2eb9ad3cfc
    }

    private static void exampleAddGet( PeerDHT[] peers )
        throws IOException, ClassNotFoundException
    {
        Number160 nr = new Number160( RND );
        String toStore1 = "hallo1";
        String toStore2 = "hallo2";
        Data data1 = new Data( toStore1 );
        Data data2 = new Data( toStore2 );
        FuturePut futurePut = peers[30].add( nr ).data( data1 ).start();
        futurePut.awaitUninterruptibly();
        System.out.println( "added: " + toStore1 + " (" + futurePut.isSuccess() + ")" );
        futurePut = peers[50].add( nr ).data( data2 ).start();
        futurePut.awaitUninterruptibly();
        System.out.println( "added: " + toStore2 + " (" + futurePut.isSuccess() + ")" );
        FutureGet futureGet = peers[77].get( nr ).all().start();
        futureGet.awaitUninterruptibly();
        System.out.println( "size" + futureGet.dataMap().size() );
        Iterator<Data> iterator = futureGet.dataMap().values().iterator();
        System.out.println( "got: " + iterator.next().object() + " (" + futureGet.isSuccess() + ")" );
        System.out.println( "got: " + iterator.next().object() + " (" + futureGet.isSuccess() + ")" );
    }

    /**
     * Example of a blocking operation and what happens after.
     * @param peers The peers in this P2P network
     * @param nr The number where the data is stored
     * @throws ClassNotFoundException .
     * @throws IOException .
     */
    private static void exampleGetBlocking(final PeerDHT[] peers, final Number160 nr)
        throws ClassNotFoundException, IOException {
        FutureGet futureGet = peers[PEER_NR_2].get(nr).start();
        // blocking operation
        futureGet.awaitUninterruptibly();
        System.out.println("result blocking: " + futureGet.data().object());
        System.out.println("this may *not* happen before printing the result");
    }

    /**
     * Example of a non-blocking operation and what happens after.
     * @param peers The peers in this P2P network
     * @param nr The number where the data is stored
     */
    private static void exampleGetNonBlocking(final PeerDHT[] peers, final Number160 nr) {
        FutureGet futureGet = peers[PEER_NR_2].get(nr).start();
        // non-blocking operation
        futureGet.addListener(new BaseFutureAdapter<FutureGet>() {
        	@Override
			public void operationComplete(FutureGet future) throws Exception {
        		System.out.println("result non-blocking: " + future.data().object());
            }
            
        });
        System.out.println("this may happen before printing the result");
    }
}
