package net.tomp2p.relay.android;

import java.util.List;

public interface MessageBufferListener<T> {

	/**
	 * Notification when the buffer at the relay peer is full. Use {@link MessageBuffer#collectBuffer()} to
	 * collect the messages
	 * 
	 * @param messages the messages that were buffered. Note that the buffer of {@link MessageBuffer} is
	 *            cleared as soon as this method call has been set.
	 */
	void bufferFull(List<T> messages);
}
