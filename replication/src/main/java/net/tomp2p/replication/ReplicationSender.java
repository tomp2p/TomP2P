package net.tomp2p.replication;

import java.util.Map;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public interface ReplicationSender {
	FutureDone<?> sendDirect(final PeerAddress other, final Number160 locationKey, final Map<Number640, Data> dataMap);
}
