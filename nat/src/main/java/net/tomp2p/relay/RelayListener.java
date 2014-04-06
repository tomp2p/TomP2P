package net.tomp2p.relay;

import net.tomp2p.connection.PeerConnection;

public interface RelayListener {

	void relayFailed(DistributedRelay distributedRelay, PeerConnection peerConnection);

}
