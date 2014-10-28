package net.tomp2p.relay.android;

import java.util.List;

import net.tomp2p.message.Message;

public interface MessageBufferListener {

	/**
	 * Notification when the buffer at the relay peer is full. Use
	 * {@link MessageBuffer#decomposeCompositeBuffer(ByetBuf)} to decompose the buffer
	 * 
	 * @param message List of all messages
	 */
	void bufferFull(List<Message> message);
}
