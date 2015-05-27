/*
 * Copyright 2009 Thomas Bocek
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
package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.message.SignatureCodec;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Utils;

/**
 * This class holds the data for the transport. The data is already serialized
 * and a hash may be created. It is reasonable to create the hash on the remote
 * peer, but not on the local peer. The remote peer uses the hash to tell the
 * other peers, which version is stored and its used quite often.
 * 
 * @author Thomas Bocek
 */
public class Data {

	private static final int MAX_BYTE_SIZE = 256;

	/**
	 * small means 8 bit, medium is 32bit.
	 * 
	 * @author Thomas Bocek
	 * 
	 */
	public enum Type {SMALL, LARGE}

	private final Type type;
	private final int length;
	// the buffer contains data without the header
	private final DataBuffer buffer;

	// these flags can be modified
	private boolean basedOnFlag;
	private boolean signed;
	private boolean flag1;
	private boolean ttl;
	private boolean flag2;
	private boolean protectedEntry;
	private boolean publicKeyFlag;
	private boolean prepareFlag;

	// can be added later
	private SignatureCodec signature;
	private int ttlSeconds = -1;
	private Set<Number160> basedOnSet = new HashSet<Number160>(0);
	private PublicKey publicKey;
	//this goes never over the network! If this is set, we have to sign lazy
	private transient PrivateKey privateKey;

	// never serialized over the network in this object
	private long validFromMillis;
	private SignatureFactory signatureFactory;
	private Number160 hash;
	private boolean meta;
	private boolean releaseAfterSend;
	private boolean released = false;
	
	public Data(final DataBuffer buffer) {
		this(buffer, buffer.length());
	}

	/**
	 * Creates a Data object that does have the complete data, but not the complete header.
	 * 
	 * @param buffer
	 * 			  The buffer containing the data.
	 * @param length
	 *            The expected length of the buffer. This does not include the
	 *            header + size (2, 5 or 9).
	 */
	public Data(final DataBuffer buffer, final int length) {
		this.length = length;
		if (length < MAX_BYTE_SIZE) {
			this.type = Type.SMALL;
		} else {
			this.type = Type.LARGE;
		}
		this.buffer = buffer;
		this.validFromMillis = System.currentTimeMillis();
	}

	/**
	 * Creates an empty Data object. The data can be filled at a later stage
	 * using append(ByteBuf).
	 * 
	 * @param header
	 *            The 8 bit header
	 * @param length
	 *            The length, depending on the header values.
	 */
	public Data(final int header, final int length) {
		this.publicKeyFlag = hasPublicKey(header);
		this.flag1 = isFlag1(header);
		this.flag2 = isFlag2(header);
		this.basedOnFlag = hasBasedOn(header);
		this.signed = isSigned(header);
		this.ttl = hasTTL(header);
		this.protectedEntry = isProtectedEntry(header);
		this.type = type(header);
		this.prepareFlag = hasPrepareFlag(header);

		if (type == Type.SMALL && length > 255) {
			throw new IllegalArgumentException("Type is not small");
		} else if (type == Type.LARGE && (length <= 255)) {
			throw new IllegalArgumentException("Type is not large");
		}

		this.length = length;
		this.buffer = new DataBuffer();
		this.validFromMillis = System.currentTimeMillis();
	}

	public Data(final Object object) throws IOException {
		this(Utils.encodeJavaObject(object));
	}

	public Data(final byte[] buffer) {
		this(buffer, 0, buffer.length);
	}
	
	public Data() {
		this(Utils.EMPTY_BYTE_ARRAY);
	}

	/**
	 * Creates a Data object from an already existing existing buffer.
	 * 
	 * @param buffer
	 *            The data buffer
	 */
	public Data(final byte[] buffer, final int offest, final int length) {
		if(buffer.length == 0) {
			this.buffer = new DataBuffer(0);
		} else {
			this.buffer = new DataBuffer(buffer, offest, length);
		}
		this.length = length;
		if (length < MAX_BYTE_SIZE) {
			this.type = Type.SMALL;
		} else {
			this.type = Type.LARGE;
		}
		this.validFromMillis = System.currentTimeMillis();
	}
	
	public boolean isEmpty() {
		return length == 0;
	}

	/**
	 * Reads the header. Does not modify the buffer positions if header could
	 * not be fully read.
	 * 
	 * Header format:
	 * <pre>
	 * 1 byte - header
	 * 1 or 4 bytes - length
	 * 4 or 0 bytes - ttl (hasTTL)
	 * 1 or 0 bytes - number of basedon keys (hasBasedOn)
	 * n x 20 bytes - basedon keys (hasBasedOn, number of basedon keys)
	 * 2 or 0 bytes - length of public key (hasPublicKey)
	 * n bytes - public key (hasPublicKey, length of public key)
	 * </pre>
	 * 
	 * 
	 * @param buf
	 *            The buffer to read from
	 * @return The data object, may be partially filled
	 */
	public static Data decodeHeader(final ByteBuf buf, final SignatureFactory signatureFactory) {
		// 2 is the smallest packet size, we could start if we know 1 byte to
		// decode the header, but we always need
		// a second byte. Thus, we are waiting for at least 2 bytes.
		if (buf.readableBytes() < Utils.BYTE_BYTE_SIZE + Utils.BYTE_BYTE_SIZE) {
			return null;
		}
		final int header = buf.getUnsignedByte(buf.readerIndex());
		final Data.Type type = Data.type(header);
		
		// length
		final int length;
		final int indexLength = Utils.BYTE_BYTE_SIZE;
		final int indexTTL;
		switch (type) {
		case SMALL:
			length = buf.getUnsignedByte(buf.readerIndex() + indexLength);
			indexTTL = indexLength + Utils.BYTE_BYTE_SIZE;
			break;
		case LARGE:
			indexTTL = indexLength + Utils.INTEGER_BYTE_SIZE;
			if (buf.readableBytes() < indexTTL) {
				return null;
			}
			length = buf.getInt(buf.readerIndex() + indexLength);
			break;
		default:
			throw new IllegalArgumentException("Unknown Type.");
		}
		
		//TTL
		final int ttl;
		final int indexBasedOnNr;
		if(hasTTL(header)) {
			indexBasedOnNr = indexTTL + Utils.INTEGER_BYTE_SIZE;
			if (buf.readableBytes() < indexBasedOnNr) {
				return null;
			}
			ttl = buf.getInt(buf.readerIndex() + indexTTL);
		} else {
			ttl = -1;
			indexBasedOnNr = indexTTL;
		}
		
		// nr basedOn + basedOn
		final int numBasedOn;
		final int indexPublicKeySize;
		final int indexBasedOn;
		final Set<Number160> basedOn = new HashSet<Number160>();
		if (hasBasedOn(header)) {
			// get nr of based on keys
			indexBasedOn = indexBasedOnNr + Utils.BYTE_BYTE_SIZE;
			if (buf.readableBytes() < indexBasedOn) {
				return null;
			}
			numBasedOn = buf.getUnsignedByte(buf.readerIndex() + indexBasedOnNr) + 1;
			indexPublicKeySize = indexBasedOn + (numBasedOn * Number160.BYTE_ARRAY_SIZE);
			if (buf.readableBytes() < indexPublicKeySize) {
				return null;
			}
			//get basedon
			int index = buf.readerIndex() + indexBasedOnNr + Utils.BYTE_BYTE_SIZE;
			final byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
			for (int i = 0; i < numBasedOn; i++) {
				buf.getBytes(index, me);
				index += Number160.BYTE_ARRAY_SIZE;
				basedOn.add(new Number160(me));
			}
			
		} else {
			// no based on keys
			indexPublicKeySize = indexBasedOnNr;
			numBasedOn = 0;
		}
		
		// public key size + public key
		final int publicKeySize;
		final int indexPublicKey;
		final int indexEnd;
		final PublicKey publicKey;
		if(hasPublicKey(header)) {
			indexPublicKey = indexPublicKeySize + Utils.SHORT_BYTE_SIZE;
			if (buf.readableBytes() < indexPublicKey) {
				return null;
			}
			publicKeySize = buf.getUnsignedShort(buf.readerIndex() + indexPublicKeySize);
			indexEnd = indexPublicKey + publicKeySize;
			if (buf.readableBytes() < indexEnd) {
				return null;
			}
			//get public key
			buf.skipBytes(indexPublicKeySize);
			publicKey = signatureFactory.decodePublicKey(buf);
		} else {
			publicKeySize = 0;
			indexPublicKey = indexPublicKeySize;
			buf.skipBytes(indexPublicKey);
			publicKey = null;
		}
		
		// now, we have read the header and the length
		final Data data = new Data(header, length);
		data.ttlSeconds = ttl;
		data.basedOnSet = basedOn;
		data.publicKey = publicKey;
		return data;
	}
	
	/**
	 * Add data to the byte buffer.
	 * 
	 * @param buf
	 *            The byte buffer to append
	 * @return True if we are done reading
	 */
	public boolean decodeBuffer(final ByteBuf buf) {
		final int already = buffer.length();
		final int remaining = length() - already;
		// already finished
		if (remaining == 0) {
			return true;
		}
		// make sure it gets not garbage collected. But we need to keep track of
		// it and when this object gets collected, we need to release the buffer
		final int transfered = buffer.transferFrom(buf, remaining);
		return transfered == remaining;
	}
	
	public boolean decodeDone(final ByteBuf buf, SignatureFactory signatureFactory) {
		if (signed) {
			if(buf.readableBytes() < signatureFactory.signatureSize()) {
				// don't even try to create a signature
				return false;
			}
			
			signature = signatureFactory.signatureCodec(buf);
		}
		return true;
	}

	public boolean decodeDone(final ByteBuf buf, PublicKey publicKey, SignatureFactory signatureFactory) {
		if (signed) {
			if(publicKey != PeerBuilder.EMPTY_PUBLIC_KEY && publicKey!= null && 
					(this.publicKey==null || this.publicKey == PeerBuilder.EMPTY_PUBLIC_KEY)) {
				this.publicKey = publicKey;
			}
			
			if(buf.readableBytes() < signatureFactory.signatureSize()) {
				// don't even try to create a signature
				return false;
			}
			
			signature = signatureFactory.signatureCodec(buf);
		}
		return true;
	}

	public boolean verify(SignatureFactory signatureFactory) throws InvalidKeyException, SignatureException {
		return verify(publicKey, signatureFactory);
	}

	public boolean verify(PublicKey publicKey, SignatureFactory signatureFactory) throws InvalidKeyException, SignatureException {
		return signatureFactory.verify(publicKey, toByteBuffers(), signature);
	}

	/**
	 * * Header format:
	 * <pre>
	 * 1 byte - header
	 * 1 or 4 bytes - length
	 * 4 or 0 bytes - ttl (hasTTL)
	 * 1 or 0 bytes - number of basedon keys (hasBasedOn)
	 * n x 20 bytes - basedon keys (hasBasedOn, number of basedon keys)
	 * 2 or 0 bytes - length of public key (hasPublicKey)
	 * n bytes - public key (hasPublicKey, length of public key)
	 * </pre>
	 * 
	 * @param buf
	 * @param signatureFactory
	 */
	public void encodeHeader(final ByteBuf buf, SignatureFactory signatureFactory) {
		int header = type.ordinal();
		if (prepareFlag) {
			header |= 0x02;
		}
		if (flag1) {
			header |= 0x04;
		}
		if (flag2) {
			header |= 0x08;
		}
		if (ttl) {
			header |= 0x10;
		}
		if (signed && publicKeyFlag && protectedEntry) {
			header |= (0x20 | 0x40);
		} else if (signed && publicKeyFlag) {
			header |= 0x40;
		} else if (publicKeyFlag) {
			header |= 0x20;
		}
		if (basedOnFlag) {
			header |= 0x80;
		}
		switch (type) {
		case SMALL:
			buf.writeByte(header);
			buf.writeByte(length);
			break;
		case LARGE:
			buf.writeByte(header);
			buf.writeInt(length);
			break;
		default:
			throw new IllegalArgumentException("Unknown Type.");
		}
		if (ttl) {
			buf.writeInt(ttlSeconds);
		}
		if (basedOnFlag) {
			buf.writeByte(basedOnSet.size() - 1);
			for (Number160 basedOn : basedOnSet) {
				buf.writeBytes(basedOn.toByteArray());
			}
		}
		if (publicKeyFlag) {
			if (publicKey == null) {
				buf.writeShort(0);
			} else {
				signatureFactory.encodePublicKey(publicKey, buf);
			}
		}
	}
	
	public boolean encodeBuffer(final AlternativeCompositeByteBuf buf) {
		final int transferred = buffer.transferTo(buf);
		return transferred == length();
	}
	
	public void encodeDone(final ByteBuf buf, SignatureFactory signatureFactory) throws InvalidKeyException, SignatureException, IOException {
		encodeDone(buf, signatureFactory, null);
	}

	public void encodeDone(final ByteBuf buf, SignatureFactory signatureFactory, PrivateKey messagePrivateKey) throws InvalidKeyException, SignatureException, IOException {
		if (signed) {
			if(signature == null && privateKey != null) {
				signature = signatureFactory.sign(privateKey, toByteBuffers());
			} else if (signature == null && messagePrivateKey != null) {
				signature = signatureFactory.sign(messagePrivateKey, toByteBuffers());
			} else if (signature == null) {
				throw new IllegalArgumentException("A private key is required to sign.");
			}
			signature.write(buf);
		}
	}

	/**
	 * If you use this, make sure you are aware of Nettys reference counting.
	 * 
	 * http://netty.io/wiki/reference-counted-objects.html
	 *  
	 * Once this object is destroyed, the ByteBuf cannot be accessed anymore (unless you use retain/release). 
	 * If you don't release it, you may run into java.lang.OufOfMemoryError: Direct Buffer Memory exceptions
	 * 
	 * @return 
	 */
	public ByteBuf buffer() {
		return buffer.toByteBuf();
	}

	public Object object() throws ClassNotFoundException, IOException {
		return Utils.decodeJavaObject(buffer);
	}

	public long validFromMillis() {
		return validFromMillis;
	}
	
	public Data validFromMillis(long validFromMillis) {
	    this.validFromMillis = validFromMillis;
	    return this;
    }
	
	public Data signNow(KeyPair keyPair, SignatureFactory signatureFactory) throws InvalidKeyException, SignatureException, IOException {
		return signNow(keyPair, signatureFactory, false);
	}
		
	public Data protectEntryNow(KeyPair keyPair, SignatureFactory signatureFactory) throws InvalidKeyException, SignatureException, IOException {
		return signNow(keyPair, signatureFactory, true);
	}	
	
	private Data signNow(KeyPair keyPair, SignatureFactory signatureFactory, boolean protectedEntry) throws InvalidKeyException, SignatureException, IOException {
		if (this.signature == null) {
			this.signed = true;
			this.signature = signatureFactory.sign(keyPair.getPrivate(), toByteBuffers());
			this.publicKey = keyPair.getPublic();
			this.publicKeyFlag = true;
			this.protectedEntry = protectedEntry;
		}
		return this;
	}
	
	public Data signNow(PrivateKey privateKey, SignatureFactory signatureFactory) throws InvalidKeyException, SignatureException, IOException {
		return signNow(privateKey, signatureFactory, false);
	}
		
	public Data protectEntryNow(PrivateKey privateKey, SignatureFactory signatureFactory) throws InvalidKeyException, SignatureException, IOException {
		return signNow(privateKey, signatureFactory, true);
	}	

	private Data signNow(PrivateKey privateKey, SignatureFactory signatureFactory, boolean protectedEntry) throws InvalidKeyException, SignatureException, IOException {
		if (this.signature == null) {
			this.signed = true;
			this.signature = signatureFactory.sign(privateKey, toByteBuffers());
			this.publicKeyFlag = true;
			this.protectedEntry = protectedEntry;
		}
		return this;
	}
	
	public Data protectEntry() {
		this.signed = true;
		this.publicKeyFlag = true;
		this.protectedEntry = true;
		return this;
	}
		
	public Data protectEntry(PrivateKey privateKey) {
		this.signed = true;
		this.publicKeyFlag = true;
		this.protectedEntry = true;
		this.privateKey = privateKey;
		return this;
	}
	
	public Data protectEntry(KeyPair keyPair) {
		this.signed = true;
		this.publicKeyFlag = true;
		this.protectedEntry = true;
		this.privateKey = keyPair.getPrivate();
		this.publicKey = keyPair.getPublic();
		return this;
	}
	
	public Data sign() {
		this.signed = true;
		this.publicKeyFlag = true;
		return this;
	}
	
	public Data sign(PrivateKey privateKey) {
		this.signed = true;
		this.publicKeyFlag = true;
		this.privateKey = privateKey;
		return this;
	}
	
	public Data sign(KeyPair keyPair) {
		this.signed = true;
		this.publicKeyFlag = true;
		this.privateKey = keyPair.getPrivate();
		this.publicKey = keyPair.getPublic();
		return this;
	}

	public int length() {
		return length;
	}

	public long expirationMillis() {
		return ttlSeconds <= 0 ? Long.MAX_VALUE : validFromMillis + (ttlSeconds * 1000L);
	}

	public int ttlSeconds() {
		return ttlSeconds;
	}

	public Data ttlSeconds(int ttlSeconds) {
		this.ttlSeconds = ttlSeconds;
		this.ttl = true;
		return this;
	}

	public Data addBasedOn(Number160 basedOn) {
		this.basedOnSet.add(basedOn);
		this.basedOnFlag = true;
		return this;
	}

	public Set<Number160> basedOnSet() {
		return basedOnSet;
	}

	public SignatureFactory signatureFactory() {
		if (signatureFactory == null) {
			return new DSASignatureFactory();
		} else {
			return signatureFactory;
		}
	}

	public Data signatureFactory(SignatureFactory signatureFactory) {
		this.signatureFactory = signatureFactory;
		return this;
	}

	public boolean isProtectedEntry() {
		return protectedEntry;
	}

	public boolean isSigned() {
		return signed;
	}
	
	public Data signed(boolean signed) {
		this.signed = signed;
		this.publicKeyFlag = signed;
		return this;
	}
	
	public Data signed() {
		signed(true);
		return this;
	}

	public boolean isFlag1() {
		return flag1;
	}

	public Data flag1(boolean flag1) {
		if(flag1 && this.flag2) {
			throw new IllegalArgumentException("Cannot set both flags. This means that data is deleted.");
		}
		this.flag1 = flag1;
		return this;
	}

	public Data flag1() {
		return flag1(true);
	}

	public boolean isFlag2() {
		return flag2;
	}

	public Data flag2(boolean flag2) {
		if(flag2 && this.flag1) {
			throw new IllegalArgumentException("Cannot set both flags. This means that data is deleted.");
		}
		this.flag2 = flag2;
		return this;
	}

	public Data flag2() {
		return flag2(true);
	}

	public boolean hasPrepareFlag() {
		return prepareFlag;
	}

	public Data prepareFlag(boolean prepareFlag) {
		this.prepareFlag = prepareFlag;
		return this;
	}

	public Data prepareFlag() {
		this.prepareFlag = true;
		return this;
	}
	
	public Data deleted() {
		return deleted(true);
	}
	
	public Data deleted(boolean deleted) {
		if(this.flag1 || this.flag2) {
			throw new IllegalArgumentException("Cannot set deleted, because one flag is already set.");
		}
		this.flag1 = deleted;
		this.flag2 = deleted;
		return this;
	}
	
	public boolean isDeleted() {
		return this.flag1 && this.flag2;
	}

	public boolean hasPublicKey() {
		return publicKeyFlag;
	}

	public Data publicKeyFlag(boolean publicKeyFlag) {
		this.publicKeyFlag = publicKeyFlag;
		return this;
	}

	public Data publicKeyFlag() {
		this.publicKeyFlag = true;
		return this;
	}
	
	public boolean isMeta() {
		return meta;
	}

	public Data meta(boolean meta) {
		this.meta = meta;
		return this;
	}

	public Data meta() {
		this.meta = true;
		return this;
	}
	
	public boolean isReleaseAfterSend() {
		return releaseAfterSend;
	}

	public Data releaseAfterSend(boolean releaseAfterSend) {
		this.releaseAfterSend = releaseAfterSend;
		return this;
	}

	public Data releaseAfterSend() {
		this.releaseAfterSend = true;
		return this;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Data[l:");
		sb.append(length).append(",t:");
		sb.append(ttlSeconds()).append(",hasPK:");
		sb.append(publicKey != null).append(",h:");
		sb.append(signature).append("]");
		return sb.toString();
	}

	/**
	 * @return A shallow copy where the data is shared but the reader and writer
	 *         index is not shared
	 */
	public Data duplicate() {
		Data data = new Data(buffer.shallowCopy(), length).publicKey(publicKey)
				.signature(signature).ttlSeconds(ttlSeconds);
		// duplicate based on keys
		data.basedOnSet.addAll(basedOnSet);

		// duplicate all the flags.
		// although signature, basedOn, and ttlSeconds set a flag, they will be overwritten with the data from this class
		data.publicKeyFlag = publicKeyFlag;
		data.flag1 = flag1;
		data.flag2 = flag2;
		data.basedOnFlag = basedOnFlag;
		data.signed = signed;
		data.ttl = ttl;
		data.protectedEntry = protectedEntry;
		data.privateKey = privateKey;
		data.validFromMillis = validFromMillis;
		data.prepareFlag = prepareFlag;
		data.releaseAfterSend = releaseAfterSend;
		return data;
	}
	
	public Data duplicateMeta() {
		Data data = new Data().publicKey(publicKey)
				.signature(signature).ttlSeconds(ttlSeconds);
		// duplicate based on keys
		data.basedOnSet.addAll(basedOnSet);

		// duplicate all the flags.
		// although signature, basedOn, and ttlSeconds set a flag, they will be overwritten with the data from this class
		data.publicKeyFlag = publicKeyFlag;
		data.flag1 = flag1;
		data.flag2 = flag2;
		data.basedOnFlag = basedOnFlag;
		data.signed = signed;
		data.ttl = ttl;
		data.protectedEntry = protectedEntry;
		data.privateKey = privateKey;
		data.validFromMillis = validFromMillis;
		data.prepareFlag = prepareFlag;
		data.releaseAfterSend = releaseAfterSend;
		return data;
	}

	public static Type type(final int header) {
		return Type.values()[header & 0x1];
	}

	private static boolean hasPrepareFlag(final int header) {
		return (header & 0x02) > 0;
	}

	private static boolean isFlag1(final int header) {
		return (header & 0x04) > 0;
	}

	private static boolean isFlag2(final int header) {
		return (header & 0x08) > 0;
	}

	private static boolean hasTTL(final int header) {
		return (header & 0x10) > 0;
	}

	private static boolean hasPublicKey(final int header) {
		return ((header >> 5) & (0x01 | 0x02)) > 0;
	}

	private static boolean isProtectedEntry(final int header) {
		return ((header >> 5) & (0x01 | 0x02)) > 2;
	}

	private static boolean isSigned(final int header) {
		return ((header >> 5) & (0x01 | 0x02)) > 1;
	}

	private static boolean hasBasedOn(final int header) {
		return (header & 0x80) > 0;
	}

	/**
	 * @return The byte array that is the payload. Here we copy the buffer
	 */
	public byte[] toBytes() {
		return buffer.bytes();
	}

	/**
	 * @return The ByteBuffers that is the payload. We do not make a copy here
	 */
	public ByteBuffer[] toByteBuffers() {
		return buffer.bufferList().toArray(new ByteBuffer[0]);
	}

	public PublicKey publicKey() {
		return publicKey;
	}
	
	/**
	 * @return A private key if we want to sign it lazy (during encoding).
	 */
	public PrivateKey privateKey() {
		return privateKey;
	}

	public Data publicKey(PublicKey publicKey) {
		this.publicKeyFlag = true;
		this.publicKey = publicKey;
		return this;
	}

	public SignatureCodec signature() {
		return signature;
	}

	public Data signature(SignatureCodec signature) {
		this.signature = signature;
		return this;
	}

	@Override
	public int hashCode() {
		BitSet bs = new BitSet(8);
		bs.set(0, signed);
		bs.set(1, ttl);
		bs.set(2, basedOnFlag);
		bs.set(3, protectedEntry);
		bs.set(4, publicKeyFlag);
		bs.set(5, flag1);
		bs.set(6, flag2);
		bs.set(7, prepareFlag);
		int hashCode = bs.hashCode() ^ ttlSeconds ^ type.ordinal() ^ length;
		for (Number160 basedOn : basedOnSet) {
			hashCode = hashCode ^ basedOn.hashCode();
		}
		// This is a slow operation, use with care!
		return hashCode ^ buffer.hashCode();
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof Data)) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		Data d = (Data) obj;
		//ignore ttl -> it's still the same data even if ttl is different
		if (d.signed != signed  || d.basedOnFlag != basedOnFlag 
				|| d.protectedEntry != protectedEntry || d.publicKeyFlag != publicKeyFlag 
				|| flag1!=d.flag1 || flag2!=d.flag2 || prepareFlag!=d.prepareFlag) {
			return false;
		}
		if (d.type != type || d.length != length) {
			return false;
		}
		//ignore ttl -> it's still the same data even if ttl is different
		return Utils.equals(basedOnSet, d.basedOnSet) && Utils.equals(signature, d.signature)
				&& d.buffer.equals(buffer); // This is a slow operation, use
											// with care!
	}

	public Number160 hash() {
		if (hash == null) {
			hash = Utils.makeSHAHash(buffer);
		}
		return hash;
	}
	
	public Data release() {
		synchronized (buffer.lockObject()) {
			released = true;
			buffer.release();    
        }
		return this;
	}
	
	public Object lockObject() {
		return buffer.lockObject();
	}
	
	public boolean isReleased() {
		return released;
	}

}
