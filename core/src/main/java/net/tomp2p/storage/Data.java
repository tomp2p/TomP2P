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
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.Serializable;
import java.security.PublicKey;
import java.util.concurrent.atomic.AtomicReference;

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
public class Data implements Serializable {
    private static final long serialVersionUID = -5023493840082652284L;

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

    // version
    private final boolean hasVersion;

    private final boolean hasHash;

    private final boolean hasTTL;

    private final boolean isProtectedEntry;

    private final int length;

    private final ByteBuf buffer;

    private final int startReaderIndex;

    // can be added later
    private Number160 hash;
    private int ttlSeconds = -1;
    private int version = -1;

    // never serialized over the network in this object
    private final long validFromMillis;
    private Number160 peerId;
    private AtomicReference<PublicKey> publicKeyReference;

    /**
     * Create a data object that does not have the complete data yet.
     * 
     * @param length
     *            The expected length of the buffer. This does not include the header, version, etc. sizes.
     * @param version
     *            The version of a data object, optional
     * @param ttlSeconds
     *            The TTL of a data object, optional
     * @param hasHash
     *            Indication if a hash should also be transmitted
     * @param isProtectedEntry
     *            True if this entry is protected
     */
    public Data(final int length, final int version, final int ttlSeconds, final boolean hasHash,
            final boolean isProtectedEntry) {
        this.hasVersion = version != -1;
        this.hasHash = hasHash;
        this.hasTTL = ttlSeconds != -1;
        this.isProtectedEntry = isProtectedEntry;
        this.length = length + additionalHeader();
        if (length < MAX_BYTE_SIZE) {
            this.type = Type.SMALL;
        } else if (length < MAX_BYTE_SIZE * MAX_BYTE_SIZE) {
            this.type = Type.MEDIUM;
        } else {
            this.type = Type.LARGE;
        }
        this.buffer = Unpooled.buffer();
        this.startReaderIndex = 0;
        this.validFromMillis = Timings.currentTimeMillis();
    }

    private int additionalHeader() {
        return (hasVersion ? 4 : 0) + (hasTTL ? 4 : 0) + (hasHash ? 20 : 0);
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
        this.hasVersion = hasVersion(header);
        this.hasHash = hasHash(header);
        this.hasTTL = hasTTL(header);
        this.isProtectedEntry = isProtectedEntry(header);
        this.type = type(header);
        this.length = length;
        this.buffer = Unpooled.buffer();
        this.startReaderIndex = 0;
        this.validFromMillis = Timings.currentTimeMillis();
    }

    public Data(final Object object) throws IOException {
        this(object, -1, -1, false, false);
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
    public Data(final Object object, final int version, final int ttlSeconds, final boolean hasHash,
            final boolean isProtectedEntry) throws IOException {
        this(Unpooled.wrappedBuffer(Utils.encodeJavaObject(object)), version, ttlSeconds, hasHash,
                isProtectedEntry);
    }

    public Data(final byte[] buffer) {
        this(Unpooled.wrappedBuffer(buffer), -1, -1, false, false);

    }

    public Data(final byte[] buffer, final boolean hasHash, final boolean isProtectedEntry) {
        this(Unpooled.wrappedBuffer(buffer), -1, -1, hasHash, isProtectedEntry);

    }

    public Data(final ByteBuf buffer, final boolean hasHash, final boolean isProtectedEntry) {
        this(buffer, -1, -1, hasHash, isProtectedEntry);

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
    public Data(final ByteBuf buffer, final int version, final int ttlSeconds, final boolean hasHash,
            final boolean isProtectedEntry) {
        this.hasVersion = version != -1;
        this.hasHash = hasHash;
        this.hasTTL = ttlSeconds != -1;
        this.isProtectedEntry = isProtectedEntry;
        this.buffer = buffer;
        this.startReaderIndex = buffer.readerIndex();
        this.length = buffer.readableBytes() + (hasVersion ? Utils.INTEGER_BYTE_SIZE : 0)
                + (hasTTL ? Utils.INTEGER_BYTE_SIZE : 0) + (hasHash ? Number160.BYTE_ARRAY_SIZE : 0);
        if (length < MAX_BYTE_SIZE) {
            this.type = Type.SMALL;
        } else if (length < MAX_BYTE_SIZE * MAX_BYTE_SIZE) {
            this.type = Type.MEDIUM;
        } else {
            this.type = Type.LARGE;
        }
        this.validFromMillis = Timings.currentTimeMillis();
        this.ttlSeconds = ttlSeconds;
        this.version = version;
    }

    /**
     * Reads the header. Does not modify the buffer positions if header could not be fully read.
     * 
     * @param buf
     *            The buffer to read from
     * @return The data object, may be partially filled
     */
    public static Data decodeHeader(final ByteBuf buf) {
        if (buf.readableBytes() < 1 + 1) {
            return null;
        }
        final int header = buf.getUnsignedByte(buf.readerIndex());
        final Data.Type type = Data.type(header);
        final int len;
        final int meta = (hasTTL(header) ? Utils.INTEGER_BYTE_SIZE : 0)
                + (hasVersion(header) ? Utils.INTEGER_BYTE_SIZE : 0);
        switch (type) {
        case SMALL:
            if (buf.readableBytes() < meta + Utils.BYTE_SIZE + Utils.BYTE_SIZE) {
                return null;
            }
            len = buf.skipBytes(1).readUnsignedByte();
            break;
        case MEDIUM:
            if (buf.readableBytes() < meta + Utils.SHORT_BYTE_SIZE + Utils.BYTE_SIZE) {
                return null;
            }
            len = buf.skipBytes(1).readUnsignedShort();
            break;
        case LARGE:
            if (buf.readableBytes() < meta + Utils.INTEGER_BYTE_SIZE + Utils.BYTE_SIZE) {
                return null;
            }
            len = buf.skipBytes(1).readInt();
            break;
        default:
            throw new IllegalArgumentException("unknown type");
        }
        final Data data = new Data(header, len);
        if (data.hasTTL) {
            data.ttlSeconds = buf.readInt();
        }
        if (data.hasVersion) {
            data.version = buf.readInt();
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

    public void encode(final ByteBuf buf) {
        // if we encode for the second time, we need to reset the writer index, as this will increase.
        buffer.readerIndex(startReaderIndex);
        int header = type.ordinal();
        if (isProtectedEntry) {
            header |= 0x10;
        }
        if (hasTTL) {
            header |= 0x20;
        }
        if (hasHash) {
            header |= 0x40;
        }
        if (hasVersion) {
            header |= 0x80;
        }
        buf.writeByte(header);
        switch (type) {
        case SMALL:
            buf.writeByte(length);
            break;
        case MEDIUM:
            buf.writeShort(length);
            break;
        case LARGE:
            buf.writeInt(length);
            break;
        default:
            throw new IllegalArgumentException("unknown size");
        }
        if (hasTTL) {
            buf.writeInt(ttlSeconds);
        }
        if (hasVersion) {
            buf.writeInt(version);
        }
        buf.writeBytes(buffer);
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
        return buffer;
    }

    public Object object() throws ClassNotFoundException, IOException {
        byte[] tmp = new byte[bufferLength()];
        buffer.getBytes(startReaderIndex, tmp);
        return Utils.decodeJavaObject(tmp, 0, this.length);
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

    public int bufferLength() {
        return length - additionalHeader();
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
        return this;
    }

    public int version() {
        return version;
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

    public boolean protectedEntry() {
        return isProtectedEntry;
    }

    public boolean hasHash() {
        return hasHash;
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

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Data)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        Data d = (Data) obj;
        if (d.hasHash != hasHash || d.hasTTL != hasTTL || d.hasVersion != hasVersion
                || d.isProtectedEntry != isProtectedEntry) {
            return false;
        }
        if (d.version != version || d.ttlSeconds != ttlSeconds || d.type != type || d.length != length) {
            return false;
        }
        return d.buffer.duplicate().readerIndex(0).equals(buffer.duplicate().readerIndex(0));
    }

    public boolean encodeBuffer(final ByteBuf buf) {
        int maxRead = buf.readableBytes();
        int already = buffer.writerIndex();
        int remaining = bufferLength() - already;
        // already finished
        if (remaining == 0) {
            return true;
        }
        buffer.writeBytes(buf, Math.min(maxRead, remaining));
        return buffer.writerIndex() == bufferLength();
    }

    /**
     * Add data to the byte buffer.
     * 
     * @param buf
     *            The byte buffer to append
     * @return True if we are done reading
     */
    public boolean decodeBuffer(final ByteBuf buf) {
        int maxRead = buf.readableBytes() - (hasHash ? Number160.BYTE_ARRAY_SIZE : 0);
        int already = buffer.writerIndex();
        int remaining = length - additionalHeader() - already;
        // already finished
        if (remaining == 0) {
            return true;
        }
        buffer.writeBytes(buf, Math.min(maxRead, remaining));
        return buffer.writerIndex() == length - additionalHeader();
    }
    
    /**
     * @return A shallow copy where the data is shared but the reader and writer index is not shared
     */
    public Data duplicate() {
        return new Data(buffer.duplicate(), version, ttlSeconds, hasHash, isProtectedEntry);
    }

    public static Type type(final int header) {
        return Type.values()[header & 0xf];
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

    private static boolean hasVersion(final int header) {
        return (header & 0x80) > 0;
    }
}
