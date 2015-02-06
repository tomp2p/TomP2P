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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.Data;

/**
 * Example of storing friends in a DHT.
 * 
 * @author Thomas Bocek
 * 
 */
public final class ExampleSimpleRecommondation {

    /**
     * Empty constructor.
     */
    private ExampleSimpleRecommondation() {
    }

    /**
     * Start the examples.
     * 
     * @param args
     *            Empty
     * @throws Exception .
     */
    public static void main(final String[] args) throws Exception {
    	PeerDHT[] peers = null;
        try {
            final int peerNr = 100;
            final int port = 4001;
            peers = ExampleUtils.createAndAttachPeersDHT(peerNr, port);
            ExampleUtils.bootstrap(peers);
            MyPeer[] myPeers = wrap(peers);
            example(myPeers);
        } finally {
            // 0 is the master
            if (peers != null && peers[0] != null) {
                peers[0].shutdown();
            }
        }
    }

    /**
     * Create MyPeer based on Peer.
     * 
     * @param peers
     *            All the peers
     * @return The converted MyPeers
     */
    private static MyPeer[] wrap(final PeerDHT[] peers) {
        MyPeer[] retVal = new MyPeer[peers.length];
        for (int i = 0; i < peers.length; i++) {
            retVal[i] = new MyPeer(peers[i]);
        }
        return retVal;
    }

    /**
     * Starts the example.
     * 
     * @param peers
     *            All the peers
     * @throws IOException .
     * @throws ClassNotFoundException .
     */
    private static void example(final MyPeer[] peers) throws IOException, ClassNotFoundException {
        // 3 peers have files
        System.out.println("Setup: we have " + peers.length
                + " peers; peers[12] (Leo) knows Jan, peers[24] (Tom) knows Urs and Pat, peers[42] (Urs) knows Pat and Tim");
        final int peer12 = 12;
        final int peer24 = 24;
        final int peer42 = 42;

        peers[peer12].announce("Leo", "Jan");
        peers[peer24].announce("Tom", "Urs");
        peers[peer24].announce("Tom", "Pat");
        peers[peer42].announce("Urs", "Pat");
        peers[peer42].announce("Urs", "Tim");
        // peer 12 now searches for Urs
        System.out.println("peers[24] (Tom) wants to know the friends of Urs");
        peers[peer24].list("Urs");
        System.out.println("peers[24] (Tom) does not know Tim yet, but Urs does");
    }

    /**
     * Peer class that deals with friends.
     * 
     * @author Thomas Bocek
     * 
     */
    private static class MyPeer {
        private final PeerDHT peer;

        private final Map<Number160, String> friends = new HashMap<Number160, String>();

        /**
         * @param peer
         *            The peer that backs this class
         */
        public MyPeer(final PeerDHT peer) {
            this.peer = peer;
            setReplyHandler(peer);
        }

        /**
         * Announce my friend list on the DHT and store it in my local map.
         * 
         * @param nickName
         *            My nickname
         * @param friendName
         *            The name of the friend
         * @throws IOException .
         */
        public void announce(final String nickName, final String friendName) throws IOException {
            friends.put(Number160.createHash(nickName), friendName);
            announce();
        }

        /**
         * Announce my friend list on the DHT.
         * 
         * @throws IOException .
         */
        public void announce() throws IOException {
            for (Map.Entry<Number160, String> entry : friends.entrySet()) {
                // announce it on DHT
                Collection<String> tmp = new ArrayList<String>(friends.values());
                NavigableMap<Number160, Data> dataMap = new TreeMap<Number160, Data>();
                for (String friend : tmp) {
                    dataMap.put(peer.peerID().xor(Number160.createHash(friend)), new Data(friend));
                }
                peer.put(entry.getKey()).dataMapContent(dataMap).start().awaitUninterruptibly();
            }
        }

        /**
         * Lists friends that are stored in the DHT.
         * 
         * @param nickName
         *            The nickname as the location key, where the data is stored
         * @throws IOException .
         * @throws ClassNotFoundException .
         */
        public void list(final String nickName) throws IOException, ClassNotFoundException {
            Number160 key = Number160.createHash(nickName);
            FutureGet futureGet = peer.get(key).all().start();
            futureGet.awaitUninterruptibly();
            for (Map.Entry<Number640, Data> entry : futureGet.dataMap().entrySet()) {
                System.out.println("this peers' (" + nickName + ") friend:" + entry.getValue().object());
            }
            System.out.println("DHT reports that " + futureGet.dataMap().size() + " peer(s) are his friends");
        }

        /**
         * @param peer
         *            Set reply handler for peer.
         */
        private void setReplyHandler(final PeerDHT peer) {
            peer.peer().objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(final PeerAddress sender, final Object request) throws Exception {
                    if (request != null && request instanceof Number160) {
                        return friends.get((Number160) request);
                    } else {
                        return null;
                    }
                }
            });
        }
    }
}
