package net.tomp2p.p2p;

import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public interface ReplicationSender extends PeerInit {
    void sendDirect(final PeerAddress other, final Number160 locationKey, final Map<Number640, Data> dataMap);
}
