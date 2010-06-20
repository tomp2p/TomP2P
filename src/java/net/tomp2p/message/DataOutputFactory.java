package net.tomp2p.message;

public interface DataOutputFactory
{

	DataOutput create(int count);

	DataOutput create(byte[] data, int offset, int length);

	DataOutput create(byte[] data);
}
