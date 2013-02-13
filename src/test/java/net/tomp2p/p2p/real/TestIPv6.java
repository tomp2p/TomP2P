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

package net.tomp2p.p2p.real;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

import net.tomp2p.Utils2;
import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.Bindings.Protocol;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

/**
 * This class is not suitable for automated integration testing, since it
 * requires a setup with a IPv6, which has to be set up manually.
 * 
 * @author draft
 */
public class TestIPv6 {
    private final int port = 4000;
    private final String ipSuperPeer = "2001:620:10:10c1:201:6cff:feca:426d";

    /**
     * Starts the server (super peer).
     * 
     * @throws IOException
     *             PeerMaker may throw and IOException
     */
    @Test
    @Ignore
    public void startServer() throws IOException {
        Random r = new Random(Utils2.THE_ANSWER);
        Bindings b = new Bindings(Protocol.IPv6);
        Peer peer = new PeerMaker(new Number160(r)).setBindings(b).setPorts(port).makeAndListen();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            for (PeerAddress pa : peer.getPeerBean().getPeerMap().getAll()) {
                FutureChannelCreator fcc = peer.getConnectionBean().getConnectionReservation().reserve(1);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.getChannelCreator();
                FutureResponse fr1 = peer.getHandshakeRPC().pingTCP(pa, cc);
                fr1.awaitUninterruptibly();

                if (fr1.isSuccess()) {
                    System.out.println("peer online TCP:" + pa);
                } else {
                    System.out.println("offline " + pa);
                }
                FutureResponse fr2 = peer.getHandshakeRPC().pingUDP(pa, cc);
                fr2.awaitUninterruptibly();
                peer.getConnectionBean().getConnectionReservation().release(cc);
                if (fr2.isSuccess()) {
                    System.out.println("peer online UDP:" + pa);
                } else {
                    System.out.println("offline " + pa);
                }
            }
        }
    }

    /**
     * Start the client that bootstraps to the super peer. Make sure you set the
     * right IP address for the super peer.
     * 
     * @throws IOException
     *             PeerMaker may throw and IOException
     */
    @Test
    @Ignore
    public void startClient() throws IOException {
        Random r = new Random(Utils2.THE_ANSWER2);
        Bindings b = new Bindings(Protocol.IPv6);
        Peer peer = new PeerMaker(new Number160(r)).setBindings(b).setPorts(port).makeAndListen();
        FutureBootstrap fb = peer.bootstrap().setInetAddress(InetAddress.getByName(ipSuperPeer)).setPorts(port).start();
        fb.awaitUninterruptibly();
        System.out.println("Got it: " + fb.isSuccess());
    }
}
