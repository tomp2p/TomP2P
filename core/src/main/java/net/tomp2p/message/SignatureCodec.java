package net.tomp2p.message;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface SignatureCodec {

	byte[] encode() throws IOException;

	SignatureCodec write(ByteBuf buf);
}
