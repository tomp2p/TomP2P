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

import net.tomp2p.bitcoin.MessageFilterRegistered;
import net.tomp2p.bitcoin.Registration;
import net.tomp2p.bitcoin.RegistrationBuilder;
import net.tomp2p.bitcoin.RegistrationService;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.message.MessageFilter;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.RegTestParams;
import org.bitcoinj.params.TestNet3Params;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * This simple example creates 10 nodes, bootstraps to the first and put and get data from those 10 nodes.
 * 
 * @author draft
 */
public final class ExampleBitcoin {

    private static final Random RND = new Random(42L);
    private static final int PEER_NR_1 = 30;
    private static final int PEER_NR_2 = 77;

    /**
     * Empty constructor.
     */
    private ExampleBitcoin() { }

    /**
     * Starts to run the examples.
     * @param args No arguments necessary
     * @throws Exception.
     */
    public static void main(final String[] args) throws Exception {
        final NetworkParameters networkParameters = TestNet3Params.get();
        final File dir = new java.io.File(".");
        final String walletFileName = "tomP2P-bitcoin-example";
        final int port = 4001;
        RegistrationService rs =  new RegistrationService(networkParameters, dir, walletFileName).start();

        PeerDHT[] peers = registerAndAttachPeersDHT(rs, 3, port);

        for (PeerDHT peer : peers) {
            System.out.println("Successfully registered and connected peer " + peer.peerID());
        }
        examplePutGet(peers, peers[2].peerID());
    }


    public static PeerDHT[] registerAndAttachPeersDHT(RegistrationService rs, int nr, int port ) throws Exception {
        PeerDHT[] peers = new PeerDHT[nr];
        for ( int i = 0; i < nr; i++ ) {
            KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
            KeyPair keyPair = gen.generateKeyPair();
            // start registration builder to get peerId
            RegistrationBuilder registrationBuilder = new RegistrationBuilder(rs, keyPair);
            Registration registration = registrationBuilder.start();
            // setup filter to only accept messages from registered peers
            MessageFilter messageFilter = new MessageFilterRegistered(rs, null);
            // start dht peer builder with registered peerId
            if ( i == 0 ) {
                peers[0] = new PeerBuilderDHT(new PeerBuilder( registration ).messageFilter( messageFilter ).ports(port).start()).start();
            } else {
                peers[i] = new PeerBuilderDHT(new PeerBuilder( registration ).messageFilter( messageFilter ).masterPeer(peers[0].peer()).start()).start();
            }
        }
        return peers;
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
        FuturePut futurePut = peers[1].put(nr).data(new Data("hallo")).start();
        futurePut.awaitUninterruptibly();
        System.out.println("peer " + peers[1].peerID() + " stored [key: " + nr + ", value: \"hallo\"]");
//        FutureGet futureGet = peers[PEER_NR_2].get(nr).start();
//        futureGet.awaitUninterruptibly();
//        System.out.println("peer " + PEER_NR_2 + " got: \"" + futureGet.data().object() + "\" for the key " + nr);
        // the output should look like this:
        // peer 30 stored [key: 0xba419d350dfe8af7aee7bbe10c45c0284f083ce4, value: "hallo"]
        // peer 77 got: "hallo" for the key 0xba419d350dfe8af7aee7bbe10c45c0284f083ce4
    }
}
