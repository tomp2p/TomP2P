package net.tomp2p.relay.android;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;

public class MockedAndroidRelayClient extends AndroidRelayClient {

	public MockedAndroidRelayClient(PeerAddress relayAddress, Peer peer) {
		super(relayAddress, peer);
	}
}
