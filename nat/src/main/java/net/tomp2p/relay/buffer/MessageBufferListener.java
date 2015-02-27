package net.tomp2p.relay.buffer;

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

	/**
	 * Notification when the buffer at the relay peer has been flushed manually (using
	 * {@link MessageBuffer#flushNow()}.
	 * 
	 * @param messages the messages that were buffered
	 */
	void bufferFlushed(List<T> messages);
}
