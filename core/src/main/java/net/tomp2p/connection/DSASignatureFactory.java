/*
 * Copyright 2013 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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

import net.tomp2p.message.DSASignatureCodec;
import net.tomp2p.message.SignatureCodec;
import net.tomp2p.p2p.PeerMaker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default signature is done with SHA1withDSA.
 * 
 * @author Thomas Bocek
 * 
 */
public class DSASignatureFactory implements SignatureFactory {

	private static final Logger LOG = LoggerFactory.getLogger(DSASignatureFactory.class);

	/**
	 * @return The signature mechanism
	 */
	private Signature signatureInstance() {
		try {
			return Signature.getInstance("SHA1withDSA");
		} catch (NoSuchAlgorithmException e) {
			LOG.error("could not find algorithm", e);
			return null;
		}
	}

	@Override
	public PublicKey decodePublicKey(final byte[] me) {
		X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(me);
		try {
			KeyFactory keyFactory = KeyFactory.getInstance("DSA");
			return keyFactory.generatePublic(pubKeySpec);
		} catch (NoSuchAlgorithmException e) {
			LOG.error("could not find algorithm", e);
			return null;
		} catch (InvalidKeySpecException e) {
			LOG.error("wrong keyspec", e);
			return null;
		}
	}

	//decodes with header
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
			return PeerMaker.EMPTY_PUBLICKEY;
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

		SignatureCodec decodedSignature = new DSASignatureCodec();
		decodedSignature.decode(signatureData);
		return decodedSignature;
	}

	@Override
	public boolean verify(PublicKey publicKey, ByteBuf buf, SignatureCodec signatureEncoded)
			throws SignatureException, InvalidKeyException, IOException {
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
    public Signature update(PublicKey receivedPublicKey, ByteBuffer[] byteBuffers) throws InvalidKeyException, SignatureException {
		Signature signature = signatureInstance();
		signature.initVerify(receivedPublicKey);
		int arrayLength = byteBuffers.length;
		for (int i = 0; i < arrayLength; i++) {
			signature.update(byteBuffers[i]);
		}
	    return signature;
    }

	@Override
    public SignatureCodec signatureCodec() {
	    return new DSASignatureCodec();
    }
}
