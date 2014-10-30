package net.tomp2p.relay;

import net.tomp2p.peers.PeerAddress;

public interface RelayListener {

	void relayFailed(PeerAddress relayAddress);

}
