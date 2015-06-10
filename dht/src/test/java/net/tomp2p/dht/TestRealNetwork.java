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
package net.tomp2p.dht;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Tests over a real network. Since this requires a real network, the tests are
 * not based on JUnit.
 * 
 * @author Thomas Bocek
 */
public class TestRealNetwork {
    private final int port = 4000;

    private final String ipSuperPeer = "192.168.1.187";
    
    @Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };

    /**
     * Starts the super peer.
     * 
     * @throws IOException
     *             PeerMaker may throw and IOException
     * @throws InterruptedException .
     */
    @Test
    @Ignore
    public void startSuperPeer() throws IOException, InterruptedException {
        new PeerBuilder(Number160.createHash("super peer")).ports(port).start();
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * Tests multiple connect and disconnects.
     * 
     * @throws ClassNotFoundException .
     * @throws IOException . 
     * @throws InterruptedException .
     */
    @Test
    @Ignore
    public void startClient2() throws ClassNotFoundException, IOException, InterruptedException {
        final int nrClients = 1000;
        for (int i = 0; i < nrClients; i++) {
            startClient();
        }
    }

    /**
     * Starts the client peer.
     * 
     * @throws IOException
     *             PeerMaker may throw and IOException
     * @throws ClassNotFoundException
     *             If the data object contained a class we did not expect
     * @throws InterruptedException .
     */
    @Test
    @Ignore
    public void startClient() throws IOException, ClassNotFoundException, InterruptedException {
        Peer myPeer = new PeerBuilder(Number160.createHash("client peer")).behindFirewall(true).
                ports(port).enableMaintenance(false).start();
        PeerAddress bootstrapServerPeerAddress = new PeerAddress(Number160.ZERO, new InetSocketAddress(
                InetAddress.getByName(ipSuperPeer), port));

        FutureDiscover discovery = myPeer.discover().peerAddress(bootstrapServerPeerAddress).start();
        discovery.awaitUninterruptibly();
        if (!discovery.isSuccess()) {
            System.err.println("no success!");
        }
        System.err.println("Peer: " + discovery.reporter() + " told us about our address.");
        InetSocketAddress myInetSocketAddress = new InetSocketAddress(myPeer.peerAddress().inetAddress(), port);

        bootstrapServerPeerAddress = discovery.reporter();
        FutureBootstrap bootstrap = myPeer.bootstrap().peerAddress(bootstrapServerPeerAddress).start();
        bootstrap.awaitUninterruptibly();

        if (!bootstrap.isSuccess()) {
            System.err.println("no success!");
        }
        
        PeerDHT myPeerDHT = new PeerBuilderDHT(myPeer).start();

        FuturePut putFuture = myPeerDHT.put(Number160.createHash("key")).data(new Data(myInetSocketAddress)).start();
        putFuture.awaitUninterruptibly();
        FutureGet futureDHT = myPeerDHT.get(Number160.createHash("key")).start();
        futureDHT.awaitUninterruptibly();
        futureDHT.futureRequests().awaitUninterruptibly();
        Data data = futureDHT.data();
        if (data == null) {
            throw new RuntimeException("Address not available in DHT.");
        }
        InetSocketAddress inetSocketAddress = (InetSocketAddress) data.object();
        System.err.println("returned " + inetSocketAddress);
        myPeer.shutdown();
        // Thread.sleep( Long.MAX_VALUE );
    }
}
