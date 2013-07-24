/*
 * Copyright 2011 Thomas Bocek
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

import java.util.Random;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

/**
 * Example how to use persistent connection with Peer.sendDirect().
 * 
 * @author Thomas Bocek
 * 
 */
public final class ExamplePersistentConnection {

    /**
     * Empty constructor.
     */
    private ExamplePersistentConnection() {
    }

    private static final Random RND = new Random(42L);

    /**
     * Start the example with persistent connections.
     * 
     * @param args
     *            Empty
     * @throws Exception .
     */
    public static void main(final String[] args) throws Exception {
        examplePersistentConnection();
    }

    /**
     * Sends a message to a peer directly using peer connection. The peer remains the connection in an open state and
     * sends a second request that uses the same connection.
     * 
     * @throws Exception .
     */
    private static void examplePersistentConnection() throws Exception {
        Peer peer1 = null;
        Peer peer2 = null;
        try {
            final int port1 = 4001;
            final int port2 = 4002;
            final int timeout = 20;
            peer1 = new PeerMaker(new Number160(RND)).setPorts(port1).makeAndListen();
            peer2 = new PeerMaker(new Number160(RND)).setPorts(port2).makeAndListen();
            //
            peer2.setObjectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(final PeerAddress sender, final Object request) throws Exception {
                    return "world!";
                }
            });
            // keep the connection for 20s alive. Setting -1 means to keep it open as long as possible
            PeerConnection peerConnection = peer1.createPeerConnection(peer2.getPeerAddress(), timeout);
            String sentObject = "Hello";
            FutureResponse fd = peer1.sendDirect(peerConnection).setObject(sentObject).start();
            System.out.println("send " + sentObject);
            fd.awaitUninterruptibly();
            System.out.println("received " + fd.getObject() + " connections: "
                    + peer1.getPeerBean().getStatistics().getTCPChannelCreationCount());
            // we reuse the connection
            fd = peer1.sendDirect(peerConnection).setObject(sentObject).start();
            System.out.println("send " + sentObject);
            fd.awaitUninterruptibly();
            System.out.println("received " + fd.getObject() + " connections: "
                    + peer1.getPeerBean().getStatistics().getTCPChannelCreationCount());
            // now we don't want to keep the connection open anymore:
            peerConnection.close();
        } finally {
            if (peer1 != null) {
                peer1.halt();
            }
            if (peer2 != null) {
                peer2.halt();
            }
        }
    }

}
