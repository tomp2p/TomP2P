package net.tomp2p.relay.android;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayClient;

public class MockedAndroidRelayClientConfig extends AndroidRelayClientConfig {

	public static final String DUMMY_GCM_API_KEY = "dummy-gcm-key";
	private final Map<PeerAddress, MockedAndroidRelayClient> clientList;

	public MockedAndroidRelayClientConfig(int peerMapUpdateIntervalS) {
		super(DUMMY_GCM_API_KEY, peerMapUpdateIntervalS);
		this.clientList = new HashMap<PeerAddress, MockedAndroidRelayClient>();
	}
	
	@Override
	public BaseRelayClient createClient(PeerConnection connection, Peer peer) {
		MockedAndroidRelayClient client = new MockedAndroidRelayClient(connection.remotePeer(), peer);
		clientList.put(peer.peerAddress(), client);
		return client;
	}

	/**
	 * Notifies the unreachable peer directly without sending a real GCM message
	 */
	public void mockGCMNotification(PeerAddress unreachablePeer) {
		if(clientList.containsKey(unreachablePeer)) {
			clientList.get(unreachablePeer).sendBufferRequest();
		} else {
			throw new IllegalStateException("No client with PeerAddress " + unreachablePeer + " connected");
		}
	}
	
	public MockedAndroidRelayClient getClient(PeerAddress clientAddress) {
		return clientList.get(clientAddress);
	}
}
