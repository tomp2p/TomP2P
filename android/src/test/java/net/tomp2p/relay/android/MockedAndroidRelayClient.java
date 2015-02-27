package net.tomp2p.relay.android;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockedAndroidRelayClient extends AndroidRelayClient {

	private static final Logger LOG = LoggerFactory.getLogger(MockedAndroidRelayClient.class);
	private long bufferReceptionDelay = 0L;

	public MockedAndroidRelayClient(PeerAddress relayAddress, Peer peer) {
		super(relayAddress, peer);
	}

	public void bufferReceptionDelay(long bufferReceptionDelay) {
		this.bufferReceptionDelay = bufferReceptionDelay;
	}

	@Override
	public void onReceiveMessageBuffer(Message responseMessage, FutureDone<Void> futureDone) {
		if (bufferReceptionDelay > 0) {
			LOG.debug("Delayed buffer reception");
			try {
				Thread.sleep(bufferReceptionDelay);
			} catch (InterruptedException e) {
				// ignore
			}
		}
		super.onReceiveMessageBuffer(responseMessage, futureDone);
	}
}
