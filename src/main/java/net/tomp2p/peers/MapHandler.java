package net.tomp2p.peers;

public interface MapHandler
{

    boolean acceptPeer( boolean firstHand, boolean isInPeerMap, PeerAddress remotePeer );

}
