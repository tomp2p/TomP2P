package net.tomp2p.message;

public interface DataOutput
{

	public abstract void writeInt(int intVal);

	public abstract void writeShort(int intVal);

	public abstract void writeByte(int intVal);
}
