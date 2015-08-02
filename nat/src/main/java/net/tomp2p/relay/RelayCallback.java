package net.tomp2p.relay;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.peers.PeerAddress;

public interface RelayCallback {

	void onRelayAdded(PeerAddress relay, PeerConnection object);

	void onRelayRemoved(PeerAddress relay, PeerConnection object);

	void onFailure(Exception e);

	void onFullRelays(int activeRelays);

	void onNoMoreRelays(int activeRelays);

	void onShutdown();

}
