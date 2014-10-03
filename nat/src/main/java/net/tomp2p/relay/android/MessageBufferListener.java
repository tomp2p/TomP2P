package net.tomp2p.relay.android;

import java.util.List;

import net.tomp2p.message.Buffer;

public interface MessageBufferListener {

	/**
	 * Notification when the buffer at the relay peer is full. Use
	 * {@link MessageBuffer#decomposeCompositeBuffer(Buffer, List)} to decompose the buffer
	 * 
	 * @param sizeBuffer contains the size of the encoded messages. Used to decompose the buffer
	 * @param messageBuffer composite of all buffered messages
	 */
	void bufferFull(Buffer sizeBuffer, Buffer messageBuffer);
}
