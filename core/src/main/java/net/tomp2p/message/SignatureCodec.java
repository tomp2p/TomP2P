package net.tomp2p.message;

import io.netty.buffer.ByteBuf;

public interface SignatureCodec {

	/**
	 * @return the encoded signature data
	 */
	byte[] encode();

	/**
	 * Write the signature data into the givne buffer
	 * 
	 * @param buf the buffer to write the signature into
	 * @return this instance
	 */
	SignatureCodec write(ByteBuf buf);
}
