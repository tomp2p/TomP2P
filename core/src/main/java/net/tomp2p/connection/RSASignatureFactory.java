package net.tomp2p.connection;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

import net.tomp2p.message.RSASignatureCodec;
import net.tomp2p.message.SignatureCodec;
import net.tomp2p.p2p.PeerBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The signature is done with SHA1withRSA.
 * 
 * @author Seppi
 */
public class RSASignatureFactory implements SignatureFactory {
	
    private static final long serialVersionUID = -4788883684758981915L;
	private static final Logger LOG = LoggerFactory.getLogger(RSASignatureFactory.class);
	
	/**
	 * @return The signature mechanism
	 */
	private Signature signatureInstance() {
		try {
			return Signature.getInstance("SHA1withRSA");
		} catch (NoSuchAlgorithmException e) {
			LOG.error("could not find algorithm", e);
			return null;
		}
	}

	@Override
	public PublicKey decodePublicKey(final byte[] me) {
		X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(me);
		try {
			KeyFactory keyFactory = KeyFactory.getInstance("RSA");
			return keyFactory.generatePublic(pubKeySpec);
		} catch (NoSuchAlgorithmException e) {
			LOG.error("could not find algorithm", e);
			return null;
		} catch (InvalidKeySpecException e) {
			LOG.error("wrong keyspec", e);
			return null;
		}
	}

	// decodes with header
	@Override
	public PublicKey decodePublicKey(ByteBuf buf) {
		if (buf.readableBytes() < 2) {
			return null;
		}
		int len = buf.getUnsignedShort(buf.readerIndex());

		if (buf.readableBytes() - 2 < len) {
			return null;
		}
		buf.skipBytes(2);

		if (len <= 0) {
			return PeerBuilder.EMPTY_PUBLIC_KEY;
		}

		byte me[] = new byte[len];
		buf.readBytes(me);
		return decodePublicKey(me);
	}

	@Override
	public void encodePublicKey(PublicKey publicKey, ByteBuf buf) {
		byte[] data = publicKey.getEncoded();
		buf.writeShort(data.length);
		buf.writeBytes(data);
	}

	@Override
	public SignatureCodec sign(PrivateKey privateKey, ByteBuf buf) throws InvalidKeyException,
			SignatureException, IOException {
		Signature signature = signatureInstance();
		signature.initSign(privateKey);
		ByteBuffer[] byteBuffers = buf.nioBuffers();
		int len = byteBuffers.length;
		for (int i = 0; i < len; i++) {
			ByteBuffer buffer = byteBuffers[i];
			signature.update(buffer);
		}
		
		byte[] signatureData = signature.sign();

		SignatureCodec decodedSignature = new RSASignatureCodec(signatureData);
		return decodedSignature;
	}

	@Override
	public boolean verify(PublicKey publicKey, ByteBuf buf, SignatureCodec signatureEncoded)
			throws SignatureException, InvalidKeyException {
		Signature signature = signatureInstance();
		signature.initVerify(publicKey);
		ByteBuffer[] byteBuffers = buf.nioBuffers();
		int len = byteBuffers.length;
		for (int i = 0; i < len; i++) {
			ByteBuffer buffer = byteBuffers[i];
			signature.update(buffer);
		}
		byte[] signatureReceived = signatureEncoded.encode();
		return signature.verify(signatureReceived);
	}

	@Override
	public Signature update(PublicKey receivedPublicKey, ByteBuffer[] byteBuffers)
			throws InvalidKeyException, SignatureException {
		Signature signature = signatureInstance();
		signature.initVerify(receivedPublicKey);
		int arrayLength = byteBuffers.length;
		for (int i = 0; i < arrayLength; i++) {
			signature.update(byteBuffers[i]);
		}
		return signature;
	}

	@Override
    public SignatureCodec signatureCodec(ByteBuf buf) throws IOException {
	    return new RSASignatureCodec(buf);
    }
}
