package net.tomp2p.relay.android;

import io.netty.buffer.ByteBuf;

public interface MessageBufferListener {

	/**
	 * Notification when the buffer at the relay peer is full. Use
	 * {@link MessageBuffer#decomposeCompositeBuffer(ByetBuf)} to decompose the buffer
	 * 
	 * @param messageBuffer composite of all buffered messages. Alternating, there's an integer and then a
	 *            message encoded in it. The integer indicates the size of the message. This makes it possible
	 *            to decompose the messages afterwards.
	 */
	void bufferFull(ByteBuf messageBuffer);
}
