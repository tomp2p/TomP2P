package net.tomp2p.replication;

import net.tomp2p.p2p.PeerInit;

public interface ReplicationFactor extends PeerInit {
    int replicationFactor();
}
