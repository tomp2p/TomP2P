package net.tomp2p.relay.android;

import java.util.List;

import net.tomp2p.message.Message;

public interface MessageBufferListener {

	/**
	 * Notification when the buffer at the relay peer is full. Use {@link MessageBuffer#collectBuffer()} to
	 * collect the messages
	 */
	void bufferFull(List<Message> messages);
}
