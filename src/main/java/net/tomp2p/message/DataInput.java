package net.tomp2p.message;

import java.nio.ByteBuffer;

public interface DataInput
{
	public ByteBuffer[] toByteBuffers(int index, int length);
	public abstract int readInt();
	public abstract void readBytes(byte[] buf);
	public abstract int readUnsignedShort();
	public abstract int readUnsignedByte();
	public abstract int getUnsignedByte();
	public abstract byte[] array();
	public abstract int arrayOffset();
	public abstract int readerIndex();
	public abstract void skipBytes(int size);
	public abstract int readableBytes();
}
