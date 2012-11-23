package net.tomp2p.peers;

public class DefaultMapAcceptHandler implements MapAcceptHandler
{
    
    private final boolean acceptFirstClassOnly; 
    
    public DefaultMapAcceptHandler(boolean acceptFirstClassOnly)
    {
        this.acceptFirstClassOnly = acceptFirstClassOnly;
    }
    
    public boolean acceptFirstClassOnly()
    {
        return acceptFirstClassOnly;
    }
    
    @Override
    public boolean acceptPeer( boolean firstHand, PeerAddress remotePeer )
    {
        if ( !firstHand && acceptFirstClassOnly )
        {
            // TODO: put peers that come from a referrer in a list, which will
            // be verified, once these peers are verified, having referrer null,
            // they should go into this map. Make this optional, since for
            // Intranet its not required but for Internet it is.
            return false;
        }
        if ( remotePeer.isFirewalledTCP() || remotePeer.isFirewalledUDP())
        {
            // We contacted a peer directly and the peer told us, that it is not
            // reachable. Thus, we ignore this peer.
            return false;
        }
        return true;
    }

}
