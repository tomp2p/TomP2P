package net.tomp2p.relay;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.peers.PeerAddress;

public interface RelayCallback {

	void onRelayAdded(PeerAddress candidate, PeerConnection object);

	void onRelayRemoved(PeerAddress candidate, PeerConnection object);

}
