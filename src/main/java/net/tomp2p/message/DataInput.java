package net.tomp2p.message;

public interface DataInput
{
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
