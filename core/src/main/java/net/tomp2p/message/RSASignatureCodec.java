package net.tomp2p.message;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Arrays;

public class RSASignatureCodec implements SignatureCodec {

	private final byte[] encodedData;

	/**
	 * Create a signature codec using an already existing signature (encoded)
	 * 
	 * @param encodedData the encoded signature
	 * @throws IOException
	 */
	public RSASignatureCodec(byte[] encodedData) throws IOException {
		if (encodedData.length != signatureSize()) {
			throw new IOException("RSA signature has size " + signatureSize() + " received: " + encodedData.length);
		}
		this.encodedData = encodedData;
	}

	/**
	 * Create a signature codec from a buffer
	 * 
	 * @param buf the buffer containing the signature at its reader index
	 */
	public RSASignatureCodec(ByteBuf buf) {
		encodedData = new byte[signatureSize()];
		buf.readBytes(encodedData);
	}

	@Override
	public byte[] encode() {
		// no decoding necessary
		return encodedData;
	}

	@Override
	public SignatureCodec write(ByteBuf buf) {
		buf.writeBytes(encodedData);
		return this;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(encodedData);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof RSASignatureCodec)) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		RSASignatureCodec s = (RSASignatureCodec) obj;
		return Arrays.equals(s.encodedData, encodedData);
	}

	@Override
	public int signatureSize() {
		// 1024 bits by default
		return 128;
	}
}
