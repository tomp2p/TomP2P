package net.tomp2p.relay.android;

import net.tomp2p.p2p.Peer;
import net.tomp2p.relay.buffer.MessageBufferConfiguration;

/**
 * Mocks the GCM functionality for testing purpose
 * 
 * @author Nico Rutishauser
 *
 */
public class MockedAndroidRelayServerConfig extends AndroidRelayServerConfig {

	private final MockedAndroidRelayClientConfig client;

	public MockedAndroidRelayServerConfig(MessageBufferConfiguration bufferConfig, MockedAndroidRelayClientConfig client) {
		super(bufferConfig);
		this.client = client;
	}

	@Override
	public void start(Peer peer) {
		gcmSender = new MockedGCMSender(peer, client);
	}
}
