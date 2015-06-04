package net.tomp2p.message;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import net.tomp2p.utils.Utils;

public class Buffer {

	private final ByteBuf buffer;
	private final int length;

	private int read = 0;

	public Buffer(final ByteBuf buffer, final int length) {
		this.buffer = buffer;
		this.length = length;
	}

	public Buffer(ByteBuf buffer) {
		this.buffer = buffer;
		this.length = buffer.readableBytes();
	}

	public int length() {
		return length;
	}

	public ByteBuf buffer() {
		return buffer;
	}

	public int readable() {
		int remaining = length - read;
		int available = buffer.readableBytes();
		return Math.min(remaining, available);
	}

	public boolean isComplete() {
		return length == buffer.readableBytes();
	}

	public int incRead(final int read) {
		this.read += read;
		return this.read;
	}

	public Object object() throws ClassNotFoundException, IOException {
		return Utils.decodeJavaObject(buffer.duplicate().readerIndex(0));
	}

	public void reset() {
		read = 0;
		buffer.resetReaderIndex();
	}
}
