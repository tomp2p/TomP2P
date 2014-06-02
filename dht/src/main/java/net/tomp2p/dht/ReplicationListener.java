package net.tomp2p.dht;

import net.tomp2p.peers.Number160;

public interface ReplicationListener {

	void updateAndNotifyResponsibilities(Number160 locationKey);

}
