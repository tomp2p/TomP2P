package net.tomp2p.p2p;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import net.tomp2p.Utils2;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RawDataReply;

/**
 * Testing with -Djava.util.logging.config.file=src/main/config/tomp2plog.properties -server -Xms2048m -Xmx2048m
 * -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods
 * 
 * @author draft
 */
public class TestPerformance
{
    final private static Random rnd = new Random( 42L );

    public static void main( String[] args )
        throws Exception
    {
        test1();
        test2();
        test3();
        test4();
    }

    private static void test1()
        throws Exception
    {
        int nrOfRequests = 300000;
        Peer[] peers = null;
        try
        {
            // setup
            peers = Utils2.createRealNodes( 50, rnd, 4000 );
            List<FutureResponse> result = new ArrayList<FutureResponse>( nrOfRequests );
            // prepare channel reservation
            PeerConnection[] peerConnections = new PeerConnection[50];
            for ( int i = 0; i < 50; i++ )
            {
                int indexCur = i % 50;
                int indexNext = ( i + 1 ) % 50;
                PeerConnection peerConnection =
                    peers[indexCur].createPeerConnection( peers[indexNext].getPeerAddress(), 1000 );
                peerConnections[indexCur] = peerConnection;
                peers[indexCur].setRawDataReply( new RawDataReply()
                {
                    @Override
                    public ChannelBuffer reply( PeerAddress sender, ChannelBuffer requestBuffer )
                        throws Exception
                    {
                        return null;
                    }
                } );
            }
            long start = System.currentTimeMillis();
            ChannelBuffer buffer = ChannelBuffers.buffer( 0 );
            for ( int i = 0; i < nrOfRequests; i++ )
            {
                int indexCur = i % 50;
                FutureResponse response =
                    peers[indexCur].sendDirect().setConnection( peerConnections[indexCur] ).setObject( buffer ).start();
                result.add( response );
            }
            for ( FutureResponse futureResponse : result )
            {
                futureResponse.awaitUninterruptibly();
            }

            double duration = ( System.currentTimeMillis() - start ) / 1000d;
            System.out.println( nrOfRequests + " requests completed in " + duration + " seconds" );
            System.out.println( "50 parallel clients" );
            double rps = nrOfRequests / duration;
            System.out.println( rps + " requests per second" );
            for ( int i = 0; i < 50; i++ )
            {
                peerConnections[i].close();
            }
        }
        finally
        {
            for ( Peer peer : peers )
            {
                peer.shutdown();
            }
        }
    }

    private static void test2()
        throws Exception
    {
        int nrOfRequests = 200000;
        Peer[] peers = null;
        try
        {
            // setup
            peers = Utils2.createRealNodes( 50, rnd, 4000 );
            List<FutureResponse> result = new ArrayList<FutureResponse>( nrOfRequests );
            // prepare channel reservation
            ChannelCreator[] channelCreators = new ChannelCreator[50];
            for ( int i = 0; i < 50; i++ )
            {
                FutureChannelCreator futureChannelCreator =
                    peers[i].getConnectionBean().getConnectionReservation().reserve( 10 );
                futureChannelCreator.awaitUninterruptibly();
                channelCreators[i] = futureChannelCreator.getChannelCreator();
            }
            long start = System.currentTimeMillis();
            for ( int i = 0; i < nrOfRequests; i++ )
            {
                int index = i % 50;
                FutureResponse response =
                    peers[index].getHandshakeRPC().pingTCP( peers[( i + 1 ) % 50].getPeerAddress(),
                                                            channelCreators[index] );
                result.add( response );
            }
            for ( FutureResponse futureResponse : result )
            {
                futureResponse.awaitUninterruptibly();
            }

            double duration = ( System.currentTimeMillis() - start ) / 1000d;
            System.out.println( nrOfRequests + " requests completed in " + duration + " seconds" );
            System.out.println( "50 parallel clients" );
            double rps = nrOfRequests / duration;
            System.out.println( rps + " requests per second" );
            for ( int i = 0; i < 50; i++ )
            {
                peers[i].getConnectionBean().getConnectionReservation().release( channelCreators[i] );
            }
        }
        finally
        {
            for ( Peer peer : peers )
            {
                peer.shutdown();
            }
        }
    }

    private static void test3()
        throws Exception
    {
        int nrOfRequests = 100000;
        Peer[] peers = null;
        try
        {
            // setup
            peers = Utils2.createRealNodes( 50, rnd, 4000 );
            List<FutureResponse> result = new ArrayList<FutureResponse>( nrOfRequests );
            // prepare channel reservation
            ChannelCreator[] channelCreators = new ChannelCreator[50];
            for ( int i = 0; i < 50; i++ )
            {
                FutureChannelCreator futureChannelCreator =
                    peers[i].getConnectionBean().getConnectionReservation().reserve( 1 );
                futureChannelCreator.awaitUninterruptibly();
                channelCreators[i] = futureChannelCreator.getChannelCreator();
            }
            long start = System.currentTimeMillis();
            for ( int i = 0; i < nrOfRequests; i++ )
            {
                int index = i % 50;
                FutureResponse response =
                    peers[index].getHandshakeRPC().pingUDP( peers[( i + 1 ) % 50].getPeerAddress(),
                                                            channelCreators[index] );
                result.add( response );
            }
            for ( FutureResponse futureResponse : result )
            {
                futureResponse.awaitUninterruptibly();
            }

            double duration = ( System.currentTimeMillis() - start ) / 1000d;
            System.out.println( nrOfRequests + " requests completed in " + duration + " seconds" );
            System.out.println( "50 parallel clients" );
            double rps = nrOfRequests / duration;
            System.out.println( rps + " requests per second" );
            for ( int i = 0; i < 50; i++ )
            {
                peers[i].getConnectionBean().getConnectionReservation().release( channelCreators[i] );
            }
        }
        finally
        {
            for ( Peer peer : peers )
            {
                peer.shutdown();
            }
        }
    }

    private static void test4()
        throws Exception
    {
        int nrOfRequests = 200000;
        int nrOfPeers = 2;
        Peer[] peers = null;
        try
        {
            // setup
            peers = Utils2.createRealNodes( nrOfPeers, rnd, 4000 );
            List<FutureResponse> result = new ArrayList<FutureResponse>( nrOfRequests );
            // prepare channel reservation
            ChannelCreator[] channelCreators = new ChannelCreator[nrOfPeers];
            for ( int i = 0; i < nrOfPeers; i++ )
            {
                FutureChannelCreator futureChannelCreator =
                    peers[i].getConnectionBean().getConnectionReservation().reserve( 50 );
                futureChannelCreator.awaitUninterruptibly();
                channelCreators[i] = futureChannelCreator.getChannelCreator();
            }
            long start = System.currentTimeMillis();
            for ( int i = 0; i < nrOfRequests; i++ )
            {
                FutureResponse response =
                    peers[0].getHandshakeRPC().pingTCP( peers[1].getPeerAddress(), channelCreators[0] );
                result.add( response );
            }
            for ( FutureResponse futureResponse : result )
            {
                futureResponse.awaitUninterruptibly();
            }

            double duration = ( System.currentTimeMillis() - start ) / 1000d;
            System.out.println( nrOfRequests + " requests completed in " + duration + " seconds" );
            System.out.println( "50 parallel clients" );
            double rps = nrOfRequests / duration;
            System.out.println( rps + " requests per second" );
            for ( int i = 0; i < nrOfPeers; i++ )
            {
                peers[i].getConnectionBean().getConnectionReservation().release( channelCreators[i] );
            }
        }
        finally
        {
            for ( Peer peer : peers )
            {
                peer.shutdown();
            }
        }
    }
}
