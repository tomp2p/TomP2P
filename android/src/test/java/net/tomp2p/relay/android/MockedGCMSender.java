package net.tomp2p.relay.android;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.android.gcm.FutureGCM;
import net.tomp2p.relay.android.gcm.GCMSenderRPC;
import net.tomp2p.relay.android.gcm.IGCMSender;

public class MockedGCMSender extends GCMSenderRPC implements IGCMSender {

	private static final Logger LOG = LoggerFactory.getLogger(MockedGCMSender.class);
	private static final long DEFAULT_GCM_MOCK_DELAY_MS = 1500;
	private static final String DUMMY_REGISTRATION_ID = "dummy-registration-id";
	
	private final MockedAndroidRelayClientConfig client;

	public MockedGCMSender(Peer peer, MockedAndroidRelayClientConfig client) {
		super(peer, DUMMY_REGISTRATION_ID, 0);
		this.client = client;
		LOG.debug("Mocked GCM sender started");
	}

	@Override
	public void send(final FutureGCM futureGCM) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(DEFAULT_GCM_MOCK_DELAY_MS);
				} catch (InterruptedException e) {
					// ignore
				}
				PeerAddress recipient = futureGCM.recipient();
				client.mockGCMNotification(recipient);
				LOG.debug("Mocked sending a message to the unreachable peer {}", recipient);
			}
		}).start();
	}
}
