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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;

/**
 * This simple example creates 10 nodes, bootstraps to the first and put and get data from those 10 nodes.
 * 
 * @author draft
 */
public class ExampleUtils
{
    final private static Random rnd = new Random( 42L );

    public static void bootstrap( Peer[] peers )
    {
        List<FutureBootstrap> futures1 = new ArrayList<FutureBootstrap>();
        List<FutureDiscover> futures2 = new ArrayList<FutureDiscover>();
        for ( int i = 1; i < peers.length; i++ )
        {
            FutureDiscover tmp = peers[i].discover().setPeerAddress( peers[0].getPeerAddress() ).start();
            futures2.add( tmp );
        }
        for ( FutureDiscover future : futures2 )
        {
            future.awaitUninterruptibly();
        }
        for ( int i = 1; i < peers.length; i++ )
        {
            FutureBootstrap tmp = peers[i].bootstrap().setPeerAddress( peers[0].getPeerAddress() ).start();
            futures1.add( tmp );
        }
        for ( int i = 1; i < peers.length; i++ )
        {
            FutureBootstrap tmp = peers[0].bootstrap().setPeerAddress( peers[i].getPeerAddress() ).start();
            futures1.add( tmp );
        }
        for ( FutureBootstrap future : futures1 )
            future.awaitUninterruptibly();
    }

    public static Peer[] createAndAttachNodes( int nr, int port )
        throws Exception
    {
        Peer[] peers = new Peer[nr];
        for ( int i = 0; i < nr; i++ )
        {
            if ( i == 0 )
            {
                peers[0] = new PeerMaker( new Number160( rnd ) ).setPorts( port ).makeAndListen();
            }
            else
            {
                peers[i] = new PeerMaker( new Number160( rnd ) ).setMasterPeer( peers[0] ).makeAndListen();
            }
        }
        return peers;
    }
}
