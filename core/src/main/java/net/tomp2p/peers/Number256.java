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
package net.tomp2p.peers;

import java.nio.ByteBuffer;
import java.security.SecureRandom;

import net.tomp2p.crypto.Crypto;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

/**
 * This class represents a 160 bit number. This class is preferred over BigInteger as we always have 160bit, and thus,
 * methods can be optimized.
 * 
 * @author Thomas Bocek
 */
public final class Number256 extends Number implements Comparable<Number256> {
    private static final long serialVersionUID = -6386562272459272306L;

    // This key has *always* 160 bit. Do not change.
    public static final int BITS = 256;

    private static final long LONG_MASK = 0xffffffffL;

    private static final int BYTE_MASK = 0xff;

    private static final int CHAR_MASK = 0xf;

    private static final int STRING_LENGTH = 66;

    // a map used for String <-> Key conversion
    private static final char[] DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
            'd', 'e', 'f' };

    // size of the backing integer array
    public static final int LONG_ARRAY_SIZE = BITS / Long.SIZE;

    // size of a byte array
    public static final int BYTE_ARRAY_SIZE = BITS / Byte.SIZE;

    public static final int CHARS_PER_LONG = Long.SIZE / (Byte.SIZE / 2);

    // backing integer array
    private final long[] val;

    // constants
    public static final Number256 ZERO = new Number256(0);

    public static final Number256 ONE = new Number256(1);

    public static final Number256 MAX_VALUE = new Number256(-1, -1, -1, -1);

    /**
     * Create a Key with value 0.
     */
    public Number256() {
        this.val = new long[LONG_ARRAY_SIZE];
    }

    /**
     * Create an instance with an integer array. This integer array will be copied into the backing array.
     * 
     * @param val
     *            The value to copy to the backing array. Since this class stores 160bit numbers, the array needs to be
     *            of size 5 or smaller.
     */
    public Number256(final long... val) {
        if (val.length > LONG_ARRAY_SIZE) {
            throw new IllegalArgumentException(String.format("Can only deal with arrays of size smaller or equal to %s. Provided array has %s length.", LONG_ARRAY_SIZE, val.length));
        }
        this.val = new long[LONG_ARRAY_SIZE];
        final int len = val.length;
        for (int i = len - 1, j = LONG_ARRAY_SIZE - 1; i >= 0; i--, j--) {
            this.val[j] = val[i];
        }
    }

    /**
     * Create a Key from a string. The string has to be of length 42 to fit into the backing array. Note that this
     * string is *always* in hexadecimal, there is no 0x... required before the number.
     * 
     * @param val
     *            The characters allowed are [0-9a-f], which is in hexadecimal
     */
    public Number256(final String val) {
        if (val.length() > STRING_LENGTH) {
            throw new IllegalArgumentException(String.format("Can only deal with strings of size smaller or equal to %s. Provided string has %s length.", STRING_LENGTH, val.length()));
        }
        if (val.indexOf("0x") != 0) {
            throw new IllegalArgumentException(val
                    + " is not in hexadecimal form. Decimal form is not supported yet");
        }
        this.val = new long[LONG_ARRAY_SIZE];
        final char[] tmp = val.toCharArray();
        final int len = tmp.length;
        for (int i = STRING_LENGTH - len, j = 2; i < (STRING_LENGTH - 2); i++, j++) {
            this.val[i >> 4] <<= 4; //divide by 16, we handle 4bits in this loop, 64/4 = 16

            int digit = Character.digit(tmp[j], 16);
            if (digit < 0) {
                throw new RuntimeException("Not a hexadecimal number \"" + tmp[j]
                        + "\". The range is [0-9a-f]");
            }
            // += or |= does not matter here
            this.val[i >> 4] += digit & CHAR_MASK; //divide by 16
        }
    }

    /**
     * Creates a Key with the integer value.
     * 
     * @param val
     *            integer value
     */
    public Number256(final long val) {
        this.val = new long[LONG_ARRAY_SIZE];
        this.val[LONG_ARRAY_SIZE - 1] = val;
    }

    /**
     * Creates a new Key using the byte array. The array is copied to the backing int[]
     * 
     * @param val
     *            byte array
     */
    public Number256(final byte[] val) {
        this(val, 0, val.length);
    }

    /**
     * Creates a new Key using the byte array. The array is copied to the backing int[] starting at the given offset.
     * 
     * @param val
     *            byte array
     * @param offset
     *            The offset where to start
     * @param length
     *            the length to read
     */
    public Number256(final byte[] val, final int offset, final int length) {
        if (length > BYTE_ARRAY_SIZE) {
            throw new IllegalArgumentException(String.format("Can only deal with byte arrays of size smaller or equal to %s. Provided array has %s length.", BYTE_ARRAY_SIZE, length));
        }
        this.val = new long[LONG_ARRAY_SIZE];
        for(int i=0; i<LONG_ARRAY_SIZE;i++) {
            this.val[i] = Utils.byteArrayToLong(val, offset + (i*8));
        }
    }

    /**
     * Creates a new Key with random values in it.
     * 
     * @param secureRandom
     *            The object to create pseudo random numbers. For testing and debugging, the seed in the random class
     *            can be set to make the random values repeatable.
     */
    public Number256(final SecureRandom secureRandom) {
        this.val = new long[LONG_ARRAY_SIZE];
        for (int i = 0; i < LONG_ARRAY_SIZE; i++) {
            this.val[i] = secureRandom.nextLong();
        }
    }

    /**
     * Creates a new key with a long for the first 64bits, and using the lower 96bits for the rest.
     * 
     * @param timestamp
     *            The long value that will be set in the beginning (most significant)
     * @param number192
     *            The rest will be filled with this number
     */
    public Number256(final long timestamp, Number256 number192) {
        this.val = new long[LONG_ARRAY_SIZE];
        this.val[0] = timestamp;
        this.val[1] = number192.val[1];
        this.val[2] = number192.val[2];
        this.val[3] = number192.val[3];
    }

    /**
     * @return The first (most significant) 64bits
     */
    public long timestamp() {
        return this.val[0];
    }
    
    /**
     * @return The lower (least significant) 96 bits
     */
    public Number256 number192() {
        return new Number256(0, val[1], val[2], val[3], val[4]);
    }

    /**
     * Xor operation. This operation is ~2.5 times faster than with BigInteger
     *
     * @param key
     *            The second operand for the xor operation
     * @return A new key with the result of the xor operation
     */
    public Number256 xor(final Number256 key) {
        final long[] result = new long[LONG_ARRAY_SIZE];
        for (int i = 0; i < LONG_ARRAY_SIZE; i++) {
            result[i] = this.val[i] ^ key.val[i];
        }
        return new Number256(result);
    }

    /**
     * Returns a copy of the backing array, which is always of size 5.
     * 
     * @return a copy of the backing array
     */
    public long[] toLongArray() {
        final long[] retVal = new long[LONG_ARRAY_SIZE];
        System.arraycopy(val,0, retVal, 0, LONG_ARRAY_SIZE);
        return retVal;
    }

    /**
     * Fills the byte array with this number.
     * 
     * @param me
     *            the byte array
     * @param offset
     *            where to start in the byte array
     * @return the offset we have read
     */
    public int toByteArray(final byte[] me, final int offset) {
        if (offset + BYTE_ARRAY_SIZE > me.length) {
            throw new RuntimeException("array too small");
        }
        for (int i = 0; i < LONG_ARRAY_SIZE; i++) {
            // multiply by eight
            final int idx = offset + (i << 3);

            me[idx + 0] = (byte) (val[i] >> 56);
            me[idx + 1] = (byte) (val[i] >> 48);
            me[idx + 2] = (byte) (val[i] >> 40);
            me[idx + 3] = (byte) (val[i] >> 32);
            me[idx + 4] = (byte) (val[i] >> 24);
            me[idx + 5] = (byte) (val[i] >> 16);
            me[idx + 6] = (byte) (val[i] >> 8);
            me[idx + 7] = (byte) (val[i]);
        }
        return offset + BYTE_ARRAY_SIZE;
    }

    /**
     * Returns a byte array, which is always of size 20.
     * 
     * @return a byte array
     */
    public byte[] toByteArray() {
        final byte[] retVal = new byte[BYTE_ARRAY_SIZE];
        toByteArray(retVal, 0);
        return retVal;
    }

    /**
     * Shows the content in a human readable manner.
     *
     * @return A human readable representation of this key
     */
    public String toString() {
        final StringBuilder sb = new StringBuilder("0x");
        for (int i = 0; i < LONG_ARRAY_SIZE; i++) {
            toHex(val[i], sb);
        }
        return sb.toString();
    }

    /**
     * Checks if this number is zero.
     * 
     * @return True if this number is zero, false otherwise
     */
    public boolean isZero() {
        for (int i = 0; i < LONG_ARRAY_SIZE; i++) {
            if (this.val[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Calculates the number of bits used to represent this number. All leading (leftmost) zero bits are ignored
     * 
     * @return The bits used
     */
    public int bitLength() {
        for (int i = 0; i < LONG_ARRAY_SIZE; i++) {
            if (this.val[i] != 0) {
                return (Long.SIZE * (LONG_ARRAY_SIZE - (i-1))) +
                        (Long.SIZE - Long.numberOfLeadingZeros(this.val[i]));
            }
        }
        return 0;
    }

    @Override
    public double doubleValue() {
        return Double.NaN;
    }

    @Override
    public float floatValue() {
        return Float.NaN;
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    public int intValueMSB() {
        return intValue();
    }

    public int intValueLSB() {
        return (int) (longValueLSB() >>> 32);
    }

    /**
     * For debugging...
     * 
     * @param pos
     *            the position in the internal array
     * @return the long of the unsigned int
     */
    long longValueAt(final int pos) {
        return this.val[pos];
    }

    @Override
    public long longValue() {
        return val[LONG_ARRAY_SIZE - 1];
    }

    public long longValueMSB() {
        return longValue();
    }

    public long longValueLSB() {
        return val[0];
    }

    @Override
    public int compareTo(final Number256 o) {
        for (int i = 0; i < LONG_ARRAY_SIZE; i++) {
            long b1 = val[i] & LONG_MASK;
            long b2 = o.val[i] & LONG_MASK;
            if (b1 < b2) {
                return -1;
            } else if (b1 > b2) {
                return 1;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Number256)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        final Number256 key = (Number256) obj;
        for (int i = 0; i < LONG_ARRAY_SIZE; i++) {
            if (key.val[i] != val[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (int i = 0; i < LONG_ARRAY_SIZE; i++) {
            hashCode = 31 * hashCode + (int)(val[i] ^ (val[i] >>> 32));
        }
        return hashCode;
    }

    /**
     * Convert an integer to hex value.
     * 
     * @param longValue
     *            The integer to convert
     * @param sb
     *            The string builder where to store the result
     */
    private static void toHex(long longValue, final StringBuilder sb) {
        // 4 bits form a char, thus we have 256/4=64 chars in a key, with an
        // long array size of 4, this gives 16 chars per long
        final char[] buf = new char[CHARS_PER_LONG];
        int charPos = CHARS_PER_LONG;
        for (int i = 0; i < CHARS_PER_LONG; i++) {
            buf[--charPos] = DIGITS[(int)(longValue & CHAR_MASK)];
            // for hexadecimal, we have 4 bits per char, which ranges from
            // [0-9a-f]
            longValue >>>= 4;
        }
        sb.append(buf, charPos, (CHARS_PER_LONG - charPos));
    }

    /**
     * Creates a new Number256 using SHA1 on the string.
     * 
     * @param string
     *            The value to hash from
     * @return A hash based on SHA1 of the string
     */
    public static Number256 createHash(final String string) {
        return Utils.makeSHAHash(string);
    }

	public static Pair<Number256, Integer> decode(byte[] me, int offset) {
		Number256 number160 = new Number256(me, offset, BYTE_ARRAY_SIZE);
		return new Pair<>(number160, offset + BYTE_ARRAY_SIZE);
	}

	public static Number256 decode(ByteBuffer buf) {
		return new Number256(buf.getLong(), buf.getLong(), buf.getLong(), buf.getLong());
	}

	public int encode(byte[] me, int offset) {
		return toByteArray(me, offset);
	}

	public Number256 encode(ByteBuffer buf) {
		buf.putLong(val[0]).putLong(val[1]).putLong(val[2]).putLong(val[3]);
		return this;
	}

    public byte[] xorOverlappedBy4(Number256 peerId) {
        final byte[] result = new byte[BYTE_ARRAY_SIZE + 4];
        //32bit steps
        final byte[] peer1 = toByteArray();
        final byte[] peer2 = peerId.toByteArray();

        //fill the first and last 4 bytes -> this is used to find the relevant peer Ids.
        for (int i = 0; i < 4; i++) {
            result[i] = peer1[i];
            System.out.println("::"+result[i]);
            result[i+32] = peer2[i+(32-4)];
        }

        for (int i = 4; i < BYTE_ARRAY_SIZE; i++) {
            result[i] = (byte) (peer1[i] ^ peer2[i-4]);
        }
        return result;
    }

    public Number256 deXorOverlappedBy4(byte[] xored, int senderIdShort) {
        final byte[] result = new byte[BYTE_ARRAY_SIZE];

        final byte[] peer1 = toByteArray();

       // final int len = BYTE_ARRAY_SIZE - 4;
        Utils.intToByteArray(senderIdShort, result, 0);
        for (int i = 4; i < BYTE_ARRAY_SIZE; i++) {
            result[i] = (byte) (peer1[i-4] ^ xored[i]);
        }

        return new Number256(result);
    }
}
