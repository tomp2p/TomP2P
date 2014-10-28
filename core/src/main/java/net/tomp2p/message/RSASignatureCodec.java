package net.tomp2p.message;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Arrays;

public class RSASignatureCodec implements SignatureCodec{
	
	private byte[] encodedData;

	@Override
    public SignatureCodec decode(byte[] encodedData) throws IOException {
	    // no decoding necessary
		if(encodedData.length != signatureSize()) {
			throw new IOException("RSA signature has size "+signatureSize()+" received: "+encodedData.length);
		}
		this.encodedData = encodedData;
	    return this;
    }

	@Override
    public byte[] encode() throws IOException {
	    // no decoding necessary
	    return encodedData;
    }

	@Override
    public SignatureCodec write(ByteBuf buf) {
	    buf.writeBytes(encodedData);
	    return this;
    }

	@Override
    public SignatureCodec read(ByteBuf buf) {
		encodedData = new byte[signatureSize()];
	    buf.readBytes(encodedData);
	    return this;
    }

	@Override
    public int signatureSize() {
		//1024 bits by default
	    return 128;
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

}
