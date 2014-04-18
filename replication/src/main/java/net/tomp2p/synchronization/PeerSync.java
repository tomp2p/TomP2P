package net.tomp2p.synchronization;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerInit;
import net.tomp2p.peers.PeerAddress;

public class PeerSync implements PeerInit{

    private SynchronizationRPC synchronizationRPC;
    private Peer peer;

    public void init(Peer peer) {
        this.peer = peer;
        this.synchronizationRPC = new SynchronizationRPC(peer.getPeerBean(),
                peer.getConnectionBean());
    }
    
    public Peer peer() {
        return peer;
    }

    public SynchronizationRPC synchronizationRPC() {
        
        return synchronizationRPC;
    }
    
    public SynchronizationDirectBuilder synchronize(PeerAddress other) {
        return new SynchronizationDirectBuilder(this, other);
    }

}
