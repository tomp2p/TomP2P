package net.tomp2p.synchronization;

import io.netty.buffer.Unpooled;
import net.tomp2p.storage.AlternativeCompositeByteBuf;
import net.tomp2p.storage.DataBuffer;

public class RArray {

	private final byte[] array;
	private final int offset;
	private final int length;
	
	private final DataBuffer dataBuffer;

	public RArray(byte[] array, int offset, int length) {
		this.array = array;
		this.offset = offset;
		this.length = length;
		this.dataBuffer = null;
	}
	
	public RArray(DataBuffer dataBuffer) {
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
	
	public DataBuffer dataBuffer() {
		return dataBuffer;
	}
	
	public boolean hasDataBuffer() {
		return dataBuffer != null;
	}

	public void transferTo(AlternativeCompositeByteBuf buf) {
		if(hasDataBuffer()) {
			dataBuffer.transferTo(buf);
		} else {
			buf.addComponent(Unpooled.wrappedBuffer(array, offset, length));
		}
		
	}

}
