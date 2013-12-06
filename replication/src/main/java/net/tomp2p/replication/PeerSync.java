package net.tomp2p.replication;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerInit;
import net.tomp2p.peers.PeerAddress;

public class PeerSync implements PeerInit{

    private SynchronizationRPC synchronizationRPC;
    private Peer peer;

    public void init(Peer peer) {
        this.peer = peer;
        SynchronizationRPC synchronizationRPC = new SynchronizationRPC(peer.getPeerBean(),
                peer.getConnectionBean());
        setSynchronizationRPC(synchronizationRPC);
    }
    
    public Peer peer() {
        return peer;
    }

    public void setSynchronizationRPC(SynchronizationRPC synchronizationRPC) {
        this.synchronizationRPC = synchronizationRPC;
    }

    public SynchronizationRPC getSynchronizationRPC() {
        if (synchronizationRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return synchronizationRPC;
    }
    
    public SynchronizationDirectBuilder synchronize(PeerAddress other) {
        return new SynchronizationDirectBuilder(this, other);
    }

}
