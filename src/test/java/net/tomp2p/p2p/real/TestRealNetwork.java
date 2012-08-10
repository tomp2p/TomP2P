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
import java.net.InetSocketAddress;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests over a real network. Since this requires a real network, the tests are not based on JUnit.
 * 
 * @author Thomas Bocek
 */
public class TestRealNetwork
{
    private final int port = 4000;

    private final String ipSuperPeer = "192.168.1.x";

    /**
     * Starts the super peer.
     * 
     * @throws IOException PeerMaker may throw and IOException
     */
    @Test
    @Ignore
    public void startSuperPeer()
        throws IOException
    {
        new PeerMaker( Number160.createHash( "super peer" ) ).setPorts( port ).makeAndListen();
    }

    /**
     * Starts the client peer.
     * 
     * @throws IOException PeerMaker may throw and IOException
     * @throws ClassNotFoundException If the data object contained a class we did not expect
     */
    @Test
    @Ignore
    public void startClient()
        throws IOException, ClassNotFoundException
    {
        Peer myPeer = new PeerMaker( Number160.createHash( "client peer" ) ).setPorts( port ).makeAndListen();
        PeerAddress bootstrapServerPeerAddress =
            new PeerAddress( Number160.ZERO, new InetSocketAddress( InetAddress.getByName( ipSuperPeer ), port ) );
        myPeer.getConfiguration().setBehindFirewall( true );

        FutureDiscover discovery = myPeer.discover().setPeerAddress( bootstrapServerPeerAddress ).start();
        discovery.awaitUninterruptibly();
        if ( !discovery.isSuccess() )
        {
            System.err.println( "no success!" );
        }
        System.err.println( "Peer: " + discovery.getReporter() + " told us about our address." );
        InetSocketAddress myInetSocketAddress = new InetSocketAddress( myPeer.getPeerAddress().getInetAddress(), port );

        bootstrapServerPeerAddress = discovery.getReporter();
        FutureBootstrap bootstrap = myPeer.bootstrap().setPeerAddress( bootstrapServerPeerAddress ).start();
        bootstrap.awaitUninterruptibly();

        if ( !bootstrap.isSuccess() )
        {
            System.err.println( "no success!" );
        }

        FutureDHT putFuture =
            myPeer.put( Number160.createHash( "key" ) ).setData( new Data( myInetSocketAddress ) ).start();
        putFuture.awaitUninterruptibly();
        FutureDHT futureDHT = myPeer.get( Number160.createHash( "key" ) ).start();
        futureDHT.awaitUninterruptibly();
        Data data = futureDHT.getData();
        if ( data == null )
        {
            throw new RuntimeException( "Address not available in DHT." );
        }
        InetSocketAddress inetSocketAddress = (InetSocketAddress) data.getObject();
        System.err.println( "returned " + inetSocketAddress );
        myPeer.shutdown();
    }
}
