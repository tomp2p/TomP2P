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

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.tracker.FutureTracker;
import net.tomp2p.tracker.PeerBuilderTracker;
import net.tomp2p.tracker.PeerTracker;

/**
 * Example of storing friends in a Tracker.
 * 
 * @author Thomas Bocek
 * 
 */
public final class ExampleTracker {
    
    /**
     * Empty constructor.
     */
    private ExampleTracker() {
    }
    
    /**
     * Start the examples.
     * 
     * @param args
     *            Empty
     * @throws Exception .
     */
    public static void main(final String[] args) throws Exception {
        Peer[] peers = null;
        try {
            final int peerNr = 100;
            final int port = 4001;
            peers = ExampleUtils.createAndAttachNodes(peerNr, port);
            ExampleUtils.bootstrap(peers);
            PeerTracker[] myPeers = wrap(peers);
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
    private static PeerTracker[] wrap(final Peer[] peers) {
    	PeerTracker[] retVal = new PeerTracker[peers.length];
        for (int i = 0; i < peers.length; i++) {
            retVal[i] = new PeerBuilderTracker(peers[i]).verifyPeersOnTracker(false).start();
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
    private static void example(final PeerTracker[] peers) throws IOException, ClassNotFoundException {
               
        FutureTracker futureTracker = peers[12].addTracker(Number160.createHash("song1")).start().awaitUninterruptibly();
        System.out.println("added myself to the tracker with location [song1]: "+futureTracker.isSuccess()+" I'm: "+peers[12].peerAddress());
        
        FutureTracker futureTracker2 = peers[24].getTracker(Number160.createHash("song1")).start().awaitUninterruptibly();
        
        System.out.println("peer24 got this: "+futureTracker2.trackers());
        System.out.println("currently stored on: "+futureTracker2.trackerPeers());
    }
}
