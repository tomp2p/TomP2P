package net.tomp2p.relay.android.gcm;

import java.util.List;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;

public class FutureGCM extends FutureDone<Void> {

	private final List<Message> buffer;
	private final String registrationId;
	private final Number160 senderId;

	public FutureGCM(List<Message> buffer, String registrationId, Number160 senderId) {
		self(this);
		this.registrationId = registrationId;
		this.senderId = senderId;
		this.buffer = buffer;
	}

	/**
	 * The buffered messages (not sent over GCM)
	 */
	public List<Message> buffer() {
		return buffer;
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
}
