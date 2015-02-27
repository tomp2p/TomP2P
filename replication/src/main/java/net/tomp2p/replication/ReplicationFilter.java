package net.tomp2p.replication;

import net.tomp2p.peers.PeerAddress;

/**
 * Allows to filter peers that should not be considered for the replication
 * @author Nico Rutishauser
 *
 */
public interface ReplicationFilter {

	boolean rejectReplication(PeerAddress targetAddress);
}
