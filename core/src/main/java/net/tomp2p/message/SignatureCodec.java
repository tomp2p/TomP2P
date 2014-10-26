package net.tomp2p.message;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface SignatureCodec {

	public SignatureCodec decode(byte[] encodedData) throws IOException;

	/**
	 * @return ASN1 encoded signature
	 * @throws IOException
	 */
	public byte[] encode() throws IOException;

	public SignatureCodec write(ByteBuf buf);

	public SignatureCodec read(ByteBuf buf);

	public int signatureSize();

}