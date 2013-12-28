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
import java.security.PublicKey;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicReference;

import net.tomp2p.connection.DefaultSignatureFactory;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

/**
 * This class holds the data for the transport. The data is already serialized and a hash may be created. It is
 * reasonable to create the hash on the remote peer, but not on the local peer. The remote peer uses the hash to tell
 * the other peers, which version is stored and its used quite often.
 * 
 * @author Thomas Bocek
 */
public class Data {

    private static final int MAX_BYTE_SIZE = 256;

    /**
     * Tiny means 8 bit, small means 16bit, medium is 32bit.
     * 
     * @author Thomas Bocek
     * 
     */
    public enum Type {
        SMALL, MEDIUM, LARGE
    }

    private final Type type;

    private final boolean isFlag1;
    private final boolean isFlag2;
    private final boolean isProtectedEntry;

    // these flags can be modified
    private boolean hasBasedOn;
    private boolean hasHash;
    private boolean hasTTL;

    private final int length;

    // the buffer contains data without the header
    private final DataBuffer buffer;

    // can be added later
    private Number160 hash;
    private int ttlSeconds = -1;
    private Number160 basedOn = null;

    // never serialized over the network in this object
    private final long validFromMillis;
    private Number160 peerId;
    private AtomicReference<PublicKey> publicKeyReference;
    private SignatureFactory signatureFactory;

    /**
     * Create a data object that does have the complete data.
     * 
     * @param length
     *            The expected length of the buffer. This does not include the header + size (2, 5, or 9).
     * @param version
     *            The version of a data object, optional
     * @param ttlSeconds
     *            The TTL of a data object, optional
     * @param hasHash
     *            Indication if a hash should also be transmitted
     * @param isProtectedEntry
     *            True if this entry is protected
     */
    public Data(final DataBuffer buffer, final int length, final Number160 basedOn, final int ttlSeconds,
            final boolean hasHash, final boolean isProtectedEntry, final boolean isFlag1,
            final boolean isFlag2) {
        this.hasBasedOn = basedOn != null;
        this.hasHash = hasHash;
        this.hasTTL = ttlSeconds != -1;
        this.ttlSeconds = ttlSeconds;
        this.isProtectedEntry = isProtectedEntry;
        this.length = length;
        this.isFlag1 = isFlag1;
        this.isFlag2 = isFlag2;
        if (length < MAX_BYTE_SIZE) {
            this.type = Type.SMALL;
        } else if (length < MAX_BYTE_SIZE * MAX_BYTE_SIZE) {
            this.type = Type.MEDIUM;
        } else {
            this.type = Type.LARGE;
        }
        this.buffer = buffer;
        this.validFromMillis = Timings.currentTimeMillis();
        this.basedOn = basedOn;
    }

    /**
     * Creates an empty data object. The data can be filled at a later stage using {@link #append(ByteBuf)}.
     * 
     * @param header
     *            The 8 bit header
     * @param length
     *            The length, which depends on the header values
     */
    public Data(final int header, final int length) {
        this.isFlag1 = isFlag1(header);
        this.isFlag2 = isFlag2(header);
        this.hasBasedOn = hasBasedOn(header);
        this.hasHash = hasHash(header);
        this.hasTTL = hasTTL(header);
        this.isProtectedEntry = isProtectedEntry(header);
        this.type = type(header);

        if (type == Type.SMALL && length > 255) {
            throw new IllegalArgumentException("Type is not small");
        } else if (type == Type.MEDIUM && (length <= 255 || length > (255 * 255))) {
            throw new IllegalArgumentException("Type is not medium");
        } else if (type == Type.LARGE && (length <= 255 * 255)) {
            throw new IllegalArgumentException("Type is not large");
        }

        this.length = length;
        this.buffer = new DataBuffer();
        this.validFromMillis = Timings.currentTimeMillis();
    }

    public Data(final Object object) throws IOException {
        this(Utils.encodeJavaObject(object));
    }

    public Data(final Object object, boolean isProtectedEntry) throws IOException {
        this(Utils.encodeJavaObject(object), null, 0, false, isProtectedEntry);
    }

    /**
     * Facade for {@link #Data2(ByteBuf, int, int, boolean, boolean)}.
     * 
     * @param object
     *            The object that will be converted to a byte buffer
     * @param version
     *            The version of a data object, optional
     * @param ttlSeconds
     *            The ttl of a data object, optional
     * @param hasHash
     *            Indication if a hash should also be transmitted
     * @param isProtectedEntry
     *            True if this entry is protected
     * @throws IOException
     *             If the object conversion did not succeed
     */
    public Data(final Object object, final Number160 basedOn, final int ttlSeconds, final boolean hasHash,
            final boolean isProtectedEntry) throws IOException {
        this(Utils.encodeJavaObject(object), basedOn, ttlSeconds, hasHash, isProtectedEntry);
    }

    public Data(final byte[] buffer, final Number160 basedOn, final int ttlSeconds, final boolean hasHash,
            final boolean isProtectedEntry) throws IOException {
        this(buffer, 0, buffer.length, basedOn, ttlSeconds, hasHash, isProtectedEntry);
    }

    public Data(final byte[] buffer) {
        this(buffer, 0, buffer.length, null, -1, false, false);

    }

    public Data(final byte[] buffer, final boolean hasHash, final boolean isProtectedEntry) {
        this(buffer, 0, buffer.length, null, -1, hasHash, isProtectedEntry);

    }

    public Data(final byte[] buffer, final boolean isFlag1) {
        this(buffer, 0, buffer.length, null, -1, false, false, isFlag1, false);
    }

    public Data(final boolean isFlag2) {
        this(new byte[0], 0, 0, null, -1, false, false, false, isFlag2);
    }

    public Data(final byte[] buffer, final int offest, final int length, final Number160 basedOn,
            final int ttlSeconds, final boolean hasHash, final boolean isProtectedEntry) {
        this(buffer, offest, length, basedOn, ttlSeconds, hasHash, isProtectedEntry, false, false);
    }

    /**
     * Creates a data object from an already existing byte buffer.
     * 
     * @param buffer
     *            The data buffer
     * @param version
     *            The version of a data object, optional
     * @param ttlSeconds
     *            The ttl of a data object, optional
     * @param hasHash
     *            Indication if a hash should also be transmitted
     * @param isProtectedEntry
     *            True if this entry is protected
     */
    public Data(final byte[] buffer, final int offest, final int length, final Number160 basedOn,
            final int ttlSeconds, final boolean hasHash, final boolean isProtectedEntry,
            final boolean isFlag1, final boolean isFlag2) {
        this.hasBasedOn = basedOn != null;
        this.hasHash = hasHash;
        this.hasTTL = ttlSeconds != -1;
        this.isProtectedEntry = isProtectedEntry;
        this.buffer = new DataBuffer(buffer);
        this.isFlag1 = isFlag1;
        this.isFlag2 = isFlag2;
        this.length = length;
        if (length < MAX_BYTE_SIZE) {
            this.type = Type.SMALL;
        } else if (length < MAX_BYTE_SIZE * MAX_BYTE_SIZE) {
            this.type = Type.MEDIUM;
        } else {
            this.type = Type.LARGE;
        }
        this.validFromMillis = Timings.currentTimeMillis();
        this.ttlSeconds = ttlSeconds;
        this.basedOn = basedOn;
    }

    private static boolean hasEnoughDataForPublicKey(final ByteBuf buf, final int toRead) {
        final int len = buf.getUnsignedShort(buf.readerIndex() + toRead - 2);
        if (len > 0 && buf.readableBytes() < toRead + len) {
            return false;
        }
        return true;
    }

    /**
     * Reads the header. Does not modify the buffer positions if header could not be fully read.
     * 
     * @param buf
     *            The buffer to read from
     * @return The data object, may be partially filled
     */
    public static Data decodeHeader(final ByteBuf buf, final SignatureFactory signatureFactory) {
        // 2 is the smallest packet size, we could start if we know 1 byte to decode the header, but we need always need
        // a second byte. Thus, we are waiting for at least 2 bytes.
        if (buf.readableBytes() < Utils.BYTE_SIZE + Utils.BYTE_SIZE) {
            return null;
        }
        final int header = buf.getUnsignedByte(buf.readerIndex());
        final Data.Type type = Data.type(header);
        final int len;
        final int toRead;
        final int meta = (hasTTL(header) ? Utils.INTEGER_BYTE_SIZE : 0)
                + (hasBasedOn(header) ? Number160.BYTE_ARRAY_SIZE : 0) + (isProtectedEntry(header) ? 2 : 0);
        // final int meta2 = ;
        switch (type) {
        case SMALL:
            toRead = meta + Utils.BYTE_SIZE + Utils.BYTE_SIZE;
            if (buf.readableBytes() < toRead) {
                return null;
            }
            // read the length of the public key
            if (isProtectedEntry(header) && !hasEnoughDataForPublicKey(buf, toRead)) {
                return null;
            }
            len = buf.skipBytes(Utils.BYTE_SIZE).readUnsignedByte();
            break;
        case MEDIUM:
            toRead = meta + Utils.SHORT_BYTE_SIZE + Utils.BYTE_SIZE;
            if (buf.readableBytes() < toRead) {
                return null;
            }
            // read the length of the public key
            if (isProtectedEntry(header) && !hasEnoughDataForPublicKey(buf, toRead)) {
                return null;
            }
            len = buf.skipBytes(Utils.BYTE_SIZE).readUnsignedShort();
            break;
        case LARGE:
            toRead = meta + Utils.INTEGER_BYTE_SIZE + Utils.BYTE_SIZE;
            if (buf.readableBytes() < toRead) {
                return null;
            }
            // read the length of the public key
            if (isProtectedEntry(header) && !hasEnoughDataForPublicKey(buf, toRead)) {
                return null;
            }
            len = buf.skipBytes(Utils.BYTE_SIZE).readInt();
            break;
        default:
            throw new IllegalArgumentException("unknown type");
        }
        final Data data = new Data(header, len);
        if (data.hasTTL) {
            data.ttlSeconds = buf.readInt();
        }
        if (data.hasBasedOn) {
            byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
            buf.readBytes(me);
            data.basedOn = new Number160(me);
        }
        if (data.isProtectedEntry) {
            PublicKey publicKey = signatureFactory.decodePublicKey(buf);
            data.publicKey(new AtomicReference<PublicKey>(publicKey));
        }
        return data;
    }

    public boolean decodeDone(final ByteBuf buf) {
        if (hasHash) {
            if (buf.readableBytes() < Number160.BYTE_ARRAY_SIZE) {
                return false;
            }
            byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
            buf.readBytes(me);
            hash = new Number160(me);
        }
        return true;
    }

    public void encodeHeader(final AlternativeCompositeByteBuf buf) {
        int header = type.ordinal();
        if (isFlag1) {
            header |= 0x04;
        }
        if (isFlag2) {
            header |= 0x08;
        }
        if (isProtectedEntry) {
            header |= 0x10;
        }
        if (hasTTL) {
            header |= 0x20;
        }
        if (hasHash) {
            header |= 0x40;
        }
        if (hasBasedOn) {
            header |= 0x80;
        }

        switch (type) {
        case SMALL:
            buf.writeByte(header);
            buf.writeByte(length);
            break;
        case MEDIUM:
            buf.writeByte(header);
            buf.writeShort(length);
            break;
        case LARGE:
            buf.writeByte(header);
            buf.writeInt(length);
            break;
        default:
            throw new IllegalArgumentException("unknown size");
        }
        if (hasTTL) {
            buf.writeInt(ttlSeconds);
        }
        if (hasBasedOn) {
            buf.writeBytes(basedOn.toByteArray());
        }
        if (isProtectedEntry) {
            if (publicKeyReference == null) {
                buf.writeShort(0);
            } else {
                signatureFactory().encodePublicKey(publicKeyReference.get(), buf);
            }
        }
        buffer.transferTo(buf);
    }

    public void encodeDone(final ByteBuf buf) {
        if (hasHash) {
            if (hash == null) {
                hash = Utils.makeSHAHash(buffer);
            }
            buf.writeBytes(hash.toByteArray());
        }
    }

    public ByteBuf buffer() {
        return buffer.toByteBuffer();
    }

    public Object object() throws ClassNotFoundException, IOException {
        DataBuffer dataBuffer = buffer.shallowCopy();
        return Utils.decodeJavaObject(dataBuffer);
    }

    public long validFromMillis() {
        return validFromMillis;
    }

    public Number160 hash() {
        if (this.hash == null) {
            this.hash = Utils.makeSHAHash(buffer);
        }
        return hash;
    }

    public int length() {
        return length;
    }

    public long expirationMillis() {
        return ttlSeconds <= 0 ? Long.MAX_VALUE : validFromMillis + (ttlSeconds * 1000L);
    }

    public Number160 peerId() {
        return peerId;
    }

    public Data peerId(Number160 peerId) {
        this.peerId = peerId;
        return this;
    }

    public int ttlSeconds() {
        return ttlSeconds;
    }

    public Data ttlSeconds(int ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
        this.hasTTL = true;
        return this;
    }

    public Data basedOn(Number160 basedOn) {
        this.basedOn = basedOn;
        this.hasBasedOn = true;
        return this;
    }

    public Number160 basedOn() {
        return basedOn;
    }

    public PublicKey publicKey() {
        if (publicKeyReference == null) {
            return null;
        } else {
            return publicKeyReference.get();
        }
    }

    public Data publicKey(AtomicReference<PublicKey> publicKeyReference) {
        this.publicKeyReference = publicKeyReference;
        return this;
    }

    public SignatureFactory signatureFactory() {
        if (signatureFactory == null) {
            return new DefaultSignatureFactory();
        } else {
            return signatureFactory;
        }
    }

    public Data signatureFactory(SignatureFactory signatureFactory) {
        this.signatureFactory = signatureFactory;
        return this;
    }

    public boolean isProtectedEntry() {
        return isProtectedEntry;
    }

    public boolean hasHash() {
        return hasHash;
    }

    public boolean isFlag1() {
        return isFlag1;
    }

    public boolean isFlag2() {
        return isFlag2;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Data[l:");
        sb.append(length).append(",t:");
        sb.append(ttlSeconds()).append(",hasPK:");
        sb.append(publicKeyReference != null).append(",h:");
        sb.append(hash()).append("]");
        return sb.toString();
    }

    public boolean encodeBuffer(final AlternativeCompositeByteBuf buf) {
        int already = buffer.alreadyTransferred();

        int remaining = length() - already;
        // already finished
        if (remaining == 0) {
            return true;
        }
        buffer.transferTo(buf);
        return buffer.alreadyTransferred() == length();
    }

    /**
     * Add data to the byte buffer.
     * 
     * @param buf
     *            The byte buffer to append
     * @return True if we are done reading
     */
    public boolean decodeBuffer(final ByteBuf buf) {
        final int already = buffer.alreadyTransferred();
        final int remaining = length() - already;
        // already finished
        if (remaining == 0) {
            return true;
        }
        // make sure it gets not garbage collected. But we need to keep track of it and when this object gets collected,
        // we need to release the buffer
        final int transfered = buffer.transferFrom(buf, remaining);
        return transfered == remaining;
    }
    
    public void resetAlreadyTransferred() {
    	buffer.resetAlreadyTransferred();
    }

   

    /**
     * @return A shallow copy where the data is shared but the reader and writer index is not shared
     */
    public Data duplicate() {
        return new Data(buffer.shallowCopy(), length, basedOn, ttlSeconds, hasHash, isProtectedEntry,
                isFlag1, isFlag2).publicKey(publicKeyReference).signatureFactory(signatureFactory);
    }

    public static Type type(final int header) {
        return Type.values()[header & 0x3];
    }

    private static boolean isFlag1(final int header) {
        return (header & 0x04) > 0;
    }

    private static boolean isFlag2(final int header) {
        return (header & 0x08) > 0;
    }

    private static boolean isProtectedEntry(final int header) {
        return (header & 0x10) > 0;
    }

    private static boolean hasTTL(final int header) {
        return (header & 0x20) > 0;
    }

    private static boolean hasHash(final int header) {
        return (header & 0x40) > 0;
    }

    private static boolean hasBasedOn(final int header) {
        return (header & 0x80) > 0;
    }

    public byte[] toBytes() {
        // TODO: avoid copy, use DataBuffer directly
        ByteBuf buf = buffer.toByteBuffer();
        byte[] me = new byte[buf.readableBytes()];
        buf.readBytes(me);
        return me;
    }

    public DataBuffer dataBuffer() {
        return buffer;
    }

    @Override
    public int hashCode() {
        BitSet bs = new BitSet(4);
        bs.set(0, hasHash);
        bs.set(1, hasTTL);
        bs.set(2, hasBasedOn);
        bs.set(3, isProtectedEntry);
        int hashCode = bs.hashCode() ^ ttlSeconds ^ type.ordinal() ^ length;
        if (basedOn != null) {
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
        if (d.hasHash != hasHash || d.hasTTL != hasTTL || d.hasBasedOn != hasBasedOn
                || d.isProtectedEntry != isProtectedEntry) {
            return false;
        }
        if (d.ttlSeconds != ttlSeconds || d.type != type || d.length != length) {
            return false;
        }
        if (basedOn != null) {
            if (!basedOn.equals(d.basedOn)) {
                return false;
            }
        } else {
            if (d.basedOn != null) {
                return false;
            }
        }
        // This is a slow operation, use with care!
        return d.buffer.equals(buffer);
    }
}
