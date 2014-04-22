package net.tomp2p.synchronization;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;

public class PeerSync {

    private final SyncRPC synchronizationRPC;
    private final Peer peer;
    private final int blockSize;
    
    PeerSync(Peer peer, final int blockSize) {
    	this.peer = peer;
    	this.synchronizationRPC =  new SyncRPC(peer.getPeerBean(),
                peer.getConnectionBean(), blockSize);
    	this.blockSize = blockSize;
    }
    
    public Peer peer() {
        return peer;
    }

    public SyncRPC synchronizationRPC() {
        return synchronizationRPC;
    }
    
    public SyncBuilder synchronize(PeerAddress other) {
        return new SyncBuilder(this, other, blockSize);
    }

}
