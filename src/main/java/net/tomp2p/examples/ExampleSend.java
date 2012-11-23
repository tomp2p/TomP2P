package net.tomp2p.examples;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

public class ExampleSend
{
    public static void main( String[] args )
        throws Exception
    {
        Peer master = null;
        try
        {
            Peer[] peers = ExampleUtils.createAndAttachNodes( 100, 4001 );
            ExampleUtils.bootstrap( peers );
            master = peers[0];
            setupReplyHandler( peers );
            System.err.println(" ---- query 3 - 6 close peers -----");
            exampleSendRedundant(peers[34]);
            System.err.println(" ---- now we want to query only the closest one -----");
            exampleSendOne(peers[14]);
            Thread.sleep( 60000 );
        }
        catch (Exception e) 
        {
            e.printStackTrace();
        }
        finally
        {
            master.shutdown();
        }
    }

    private static void exampleSendOne(Peer peer)
    {
        RequestP2PConfiguration requestP2PConfiguration = new RequestP2PConfiguration( 1, 10, 0 );
        FutureDHT futureDHT = peer.send( Number160.createHash( "key" ) ).setObject( "hello" ).setRequestP2PConfiguration( requestP2PConfiguration ).start();
        futureDHT.awaitUninterruptibly();
        for(Object object:futureDHT.getRawDirectData2().values()) 
        {
            System.err.println("got:"+ object);    
        }
    }

    private static void exampleSendRedundant(Peer peer)
    {
        FutureDHT futureDHT = peer.send( Number160.createHash( "key" ) ).setObject( "hello" ).start();
        futureDHT.awaitUninterruptibly();
        for(Object object:futureDHT.getRawDirectData2().values()) 
        {
            System.err.println("got:"+ object);    
        }
        
    }

    private static void setupReplyHandler( Peer[] peers)
    {
        for(final Peer peer:peers) 
        {
            peer.setObjectDataReply( new ObjectDataReply()
            {             
                @Override
                public Object reply( PeerAddress sender, Object request )
                    throws Exception
                {                
                    System.err.println("I'm "+peer.getPeerID()+" and I just got the message ["+request+"] from "+sender.getID());
                    return "world";
                }
            } );
        }
    }
}
