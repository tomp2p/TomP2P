package net.tomp2p.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.TrackerData;

public class ExampleDHT
{
    public static void main( String[] args )
        throws Exception
    {
        Peer[] peers = null;
        try
        {
            peers = ExampleUtils.createAndAttachNodes( 100, 4001 );
            ExampleUtils.bootstrap( peers );
            MyPeer[] myPeers = wrap( peers );
            example( myPeers );
        }
        finally
        {
            // 0 is the master
            peers[0].shutdown();
        }
    }

    private static MyPeer[] wrap( Peer[] peers )
    {
        MyPeer[] retVal = new MyPeer[peers.length];
        for ( int i = 0; i < peers.length; i++ )
        {
            retVal[i] = new MyPeer( peers[i] );
        }
        return retVal;
    }

    private static void example( MyPeer[] peers )
        throws IOException, ClassNotFoundException
    {
        // 3 peers have files
        System.out.println( "Setup: we have " + peers.length
            + " peers; peers[12] has Song A, peers[24] has Song B, peers[42] has Song C" );
        peers[12].announce( "Song A" );
        peers[24].announce( "Song B" );
        peers[42].announce( "Song C" );
        // peer 12 now searches for Song B
        System.out.println( "peers[12] wants to download Song B" );
        String downloaded = peers[12].download( Number160.createHash( "Song B" ) );
        System.out.println( "peers[12] got " + downloaded );
        // now peer 42 is also interested in song 24 and wants to know what other songs people listen to that downloaded
        // song 12
        System.out.println( "peers[42] wants to download Song B" );
        downloaded = peers[42].download( Number160.createHash( "Song B" ) );
        System.out.println( "peers[42] got " + downloaded );
        // now peer 12 downloads again the same song
        System.out.println( "peers[12] wants to download Song B" );
        downloaded = peers[12].download( Number160.createHash( "Song B" ) );
        System.out.println( "peers[12] got " + downloaded );
    }

    private static class MyPeer
    {
        final private Peer peer;

        final private Map<Number160, String> downloaded = new HashMap<Number160, String>();

        public MyPeer( Peer peer )
        {
            this.peer = peer;
            setReplyHandler( peer );
        }

        public void announce( String title )
            throws IOException
        {
            downloaded.put( Number160.createHash( title ), title );
            announce();
        }

        public void announce()
            throws IOException
        {
            for ( Map.Entry<Number160, String> entry : downloaded.entrySet() )
            {
                peer.addTracker( entry.getKey() ).start().awaitUninterruptibly();
                // announce it on DHT
                Collection<String> tmp = new ArrayList<String>( downloaded.values() );
                tmp.remove( entry.getValue() );
                Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
                for ( String song : tmp )
                {
                    dataMap.put( peer.getPeerID().xor( Number160.createHash( song ) ), new Data( song ) );
                }
                peer.put( entry.getKey() ).setDataMap( dataMap ).start().awaitUninterruptibly();
            }
        }

        public String download( Number160 key )
            throws IOException, ClassNotFoundException
        {
            FutureTracker futureTracker = peer.getTracker( key ).start();
            // now we know which peer has this data, and we also know what other things this peer has
            futureTracker.awaitUninterruptibly();
            Collection<TrackerData> trackerDatas = futureTracker.getTrackers();
            for ( TrackerData trackerData : trackerDatas )
            {
                System.out.println( "Peer " + trackerData + " claims to have the content" );
            }
            System.out.println( "Tracker reports that " + trackerDatas.size() + " peer(s) have this song" );
            // here we download
            FutureResponse futureData =
                peer.sendDirect(trackerDatas.iterator().next().getPeerAddress()).setObject( key ).start();
            futureData.awaitUninterruptibly();
            String downloaded = (String) futureData.getObject();
            // get the recommondation
            FutureDHT futureDHT = peer.get( key ).setAll().start();
            futureDHT.awaitUninterruptibly();
            for ( Map.Entry<Number160, Data> entry : futureDHT.getDataMap().entrySet() )
            {
                System.out.println( "peers that downloaded this song also downloaded " + entry.getValue().getObject() );
            }
            // we need to announce that we have this piece now
            announce( downloaded );
            return downloaded;
        }

        private void setReplyHandler( Peer peer )
        {
            peer.setObjectDataReply( new ObjectDataReply()
            {
                @Override
                public Object reply( PeerAddress sender, Object request )
                    throws Exception
                {
                    if ( request != null && request instanceof Number160 )
                        return downloaded.get( (Number160) request );
                    else
                        return null;
                }
            } );
        }
    }
}
