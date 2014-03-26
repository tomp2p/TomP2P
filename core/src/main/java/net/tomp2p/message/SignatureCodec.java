package net.tomp2p.message;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface SignatureCodec {

	public abstract SignatureCodec decode(byte[] encodedData) throws IOException;

	/**
	 * @return ASN1 encoded signature
	 * @throws IOException
	 */
	public abstract byte[] encode() throws IOException;

	public abstract SignatureCodec write(ByteBuf buf);

	public abstract SignatureCodec read(ByteBuf buf);

	public abstract int signatureSize();

}