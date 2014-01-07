package net.tomp2p.message;

import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class CompByteBufAllocator {
	
	public AlternativeCompositeByteBuf compDirectBuffer() {
		return AlternativeCompositeByteBuf.compDirectBuffer();
	}

	public AlternativeCompositeByteBuf compBuffer() {
		return AlternativeCompositeByteBuf.compBuffer();
	}

}
