package net.tomp2p.synchronization;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RArray {

	private final byte[] array;
	private final int offset;
	private final int length;
	
	private final ByteBuf dataBuffer;

	public RArray(byte[] array, int offset, int length) {
		this.array = array;
		this.offset = offset;
		this.length = length;
		this.dataBuffer = null;
	}
	
	public RArray(ByteBuf dataBuffer) {
		this.array = null;
		this.offset = -1;
		this.length = -1;
		this.dataBuffer = dataBuffer;
	}

	public byte[] array() {
		return array;
	}

	public int offset() {
		return offset;
	}

	public int length() {
		return length;
	}
	
	public ByteBuf dataBuffer() {
		return dataBuffer;
	}
	
	public boolean hasDataBuffer() {
		return dataBuffer != null;
	}

	public void transferTo(ByteBuf buf) {
		if(hasDataBuffer()) {
                    buf.writeBytes(dataBuffer.duplicate());
		} else {
			buf.writeBytes(Unpooled.wrappedBuffer(array, offset, length));
		}
		
	}

}
