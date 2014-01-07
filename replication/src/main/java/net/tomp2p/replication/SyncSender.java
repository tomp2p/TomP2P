package net.tomp2p.replication;

import java.util.Map;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.DataMap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerInit;
import net.tomp2p.p2p.ReplicationSender;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class SyncSender implements ReplicationSender, PeerInit {

    private final PeerSync peerSync;
    private Peer peer;

    public SyncSender(PeerSync peerSync) {
        this.peerSync = peerSync;
    }

    @Override
    public void init(Peer peer) {
        peerSync.init(peer);
        this.peer = peer;
    }

    @Override
    public void sendDirect(PeerAddress other, Number160 locationKey, Map<Number640, Data> dataMap) {
        FutureDone<SynchronizationStatistics> future = peerSync.synchronize(other)
                .dataMap(new DataMap(dataMap)).start();
        peer.notifyAutomaticFutures(future);
    }
}
