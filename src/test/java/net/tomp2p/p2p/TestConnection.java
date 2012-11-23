package net.tomp2p.p2p;

import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.Test;

public class TestConnection
{
    Random rnd1 = new Random();

    Random rnd2 = new Random();

    @Test
    public void test()
        throws Exception
    {
        Peer peer1 = null;
        Peer peer2 = null;
        try
        {
            Bindings b1 = new Bindings( Bindings.Protocol.IPv4, InetAddress.getByName( "127.0.0.1" ), 4005, 4005 );
            Bindings b2 = new Bindings( Bindings.Protocol.IPv4, InetAddress.getByName( "127.0.0.1" ), 4006, 4006 );
            peer1 = new PeerMaker( new Number160( rnd1 ) ).setPorts( 4005 ).setBindings( b1 ).makeAndListen();
            peer2 = new PeerMaker( new Number160( rnd2 ) ).setPorts( 4006 ).setBindings( b2 ).makeAndListen();
            // peer1 = new PeerMaker( new Number160( rnd1 ) ).setPorts(4001).makeAndListen();
            // peer2 = new PeerMaker( new Number160( rnd2 ) ).setPorts(4002).makeAndListen();
            //
            peer2.setObjectDataReply( new ObjectDataReply()
            {
                @Override
                public Object reply( PeerAddress sender, Object request )
                    throws Exception
                {
                    return "world!";
                }
            } );
            // keep the connection for 20s alive. Setting -1 means to keep it open as long as possible
            FutureBootstrap masterAnother = peer1.bootstrap().setPeerAddress( peer2.getPeerAddress() ).start();
            FutureBootstrap anotherMaster = peer2.bootstrap().setPeerAddress( peer1.getPeerAddress() ).start();
            masterAnother.awaitUninterruptibly();
            anotherMaster.awaitUninterruptibly();
            PeerConnection peerConnection = peer1.createPeerConnection( peer2.getPeerAddress(), 20 );
            String sentObject = "Hello";
            FutureResponse fd = peer1.sendDirect( peerConnection ).setObject( sentObject ).start();
            System.out.println( "send " + sentObject );
            fd.awaitUninterruptibly();
            System.out.println( "received " + fd.getObject() + " connections: "
                + peer1.getPeerBean().getStatistics().getTCPChannelCreationCount() );
            // we reuse the connection
            long start = System.currentTimeMillis();
            fd = peer1.sendDirect( peerConnection ).setObject( sentObject ).start();
            System.out.println( "send " + sentObject );
            fd.awaitUninterruptibly();
            System.out.println( "received " + fd.getObject() + " connections: "
                + peer1.getPeerBean().getStatistics().getTCPChannelCreationCount() );
            // now we don't want to keep the connection open anymore:
            double duration = ( System.currentTimeMillis() - start ) / 1000d;
            System.out.println( "Send and get in s:" + duration );
            peerConnection.close();
        }
        finally
        {
            peer1.shutdown();
            peer2.shutdown();
        }
    }
}
