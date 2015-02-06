package net.tomp2p.dht;

import net.tomp2p.peers.Number160;

public interface ReplicationListener {

	void dataInserted(Number160 locationKey);
	void dataRemoved(Number160 locationKey);

}
