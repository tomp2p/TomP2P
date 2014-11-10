/*
 * This file is part of Bitsquare.
 *
 * Bitsquare is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at
 * your option) any later version.
 *
 * Bitsquare is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Bitsquare. If not, see <http://www.gnu.org/licenses/>.
 */

package net.tomp2p.examples;

import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.rpc.ObjectDataReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for testing with {@link TomP2PTests}
 */
public class SeedNodeForTesting {
    private static Peer peer = null;
    private static boolean running = true;
    private static final Logger log = LoggerFactory.getLogger(SeedNodeForTesting.class);

    public static void main(String[] args) throws Exception {
        try {
            Number160 peerId = Number160.createHash(TomP2PTests.BOOTSTRAP_NODE_ID);
            PeerMapConfiguration pmc = new PeerMapConfiguration(peerId).peerNoVerification();
            PeerMap pm = new PeerMap(pmc);
            peer = new PeerBuilder(peerId).ports(TomP2PTests.BOOTSTRAP_NODE_PORT).peerMap(pm).start();
            peer.objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    log.trace("received request: ", request.toString());
                    return "pong";
                }
            });

            new PeerBuilderDHT(peer).start();
            new PeerBuilderNAT(peer).start();

            log.debug("SeedNode started.");
            new Thread(new Runnable() {

                @Override
                public void run() {
                    while (running) {
                        for (PeerAddress pa : peer.peerBean().peerMap().all()) {
                            log.debug("peer online:" + pa);
                        }
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }

                }
            }).start();

        } catch (Exception e) {
            if (peer != null)
                peer.shutdown().awaitUninterruptibly();
        }
    }

    public static void stop() {
        running = false;
        if(peer!=null ) {
            peer.shutdown().awaitUninterruptibly();
        }
        peer = null;
    }
}
