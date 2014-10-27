package net.tomp2p.relay.android;

public interface MessageBufferListener {

	/**
	 * Notification when the buffer at the relay peer is full. Use {@link MessageBuffer#collectBuffer()} to
	 * collect the messages
	 */
	void bufferFull();
}
