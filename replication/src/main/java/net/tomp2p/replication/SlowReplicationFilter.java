package net.tomp2p.replication;

import net.tomp2p.peers.PeerAddress;

/**
 * Relieves slow peers from the replication duty
 * 
 * @author Nico Rutishauser
 *
 */
public class SlowReplicationFilter implements ReplicationFilter {

	@Override
	public boolean rejectReplication(PeerAddress targetAddress) {
		return targetAddress.slow();
	}

}
