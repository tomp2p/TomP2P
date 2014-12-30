package net.tomp2p.message;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface SignatureCodec {

	SignatureCodec decode(byte[] encodedData) throws IOException;

	byte[] encode() throws IOException;

	SignatureCodec write(ByteBuf buf);

	SignatureCodec read(ByteBuf buf);

	int signatureSize();
}
