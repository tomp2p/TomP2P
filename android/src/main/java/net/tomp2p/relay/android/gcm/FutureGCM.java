package net.tomp2p.relay.android.gcm;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class FutureGCM extends FutureDone<Void> {

	private final String registrationId;
	private final Number160 senderId;
	private final PeerAddress recipient;

	public FutureGCM(String registrationId, Number160 senderId, PeerAddress recipient) {
		self(this);
		this.registrationId = registrationId;
		this.senderId = senderId;
		this.recipient = recipient;
	}

	/**
	 * The recipient
	 */
	public String registrationId() {
		return registrationId;
	}

	/**
	 * The relay that sent the GCM request (where the messages are buffered)
	 */
	public Number160 senderId() {
		return senderId;
	}

	/**
	 * The unreachable peer receiving the GCM
	 */
	public PeerAddress recipient() {
		return recipient;
	}
}
