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

import java.util.Random;

import net.tomp2p.utils.Utils;

/**
 * This class represents a 160 bit number. This class is preferred over BigInteger as we always have 160bit, and thus,
 * methods can be optimized.
 * 
 * @author Thomas Bocek
 */
public final class Number160 extends Number implements Comparable<Number160> {
    private static final long serialVersionUID = -6386562272459272306L;

    // This key has *always* 160 bit. Do not change.
    public static final int BITS = 160;

    public static final Number160 MAX_VALUE = new Number160(new int[] {-1, -1, -1, -1, -1 });

    private static final long LONG_MASK = 0xffffffffL;

    private static final int BYTE_MASK = 0xff;

    private static final int CHAR_MASK = 0xf;

    private static final int STRING_LENGTH = 42;

    // a map used for String <-> Key conversion
    private static final char[] DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f' };

    // size of the backing integer array
    public static final int INT_ARRAY_SIZE = BITS / Integer.SIZE;

    // size of a byte array
    public static final int BYTE_ARRAY_SIZE = BITS / Byte.SIZE;

    public static final int CHARS_PER_INT = 8;

    // backing integer array
    private final int[] val;

    // constants
    public static final Number160 ZERO = new Number160(0);

    public static final Number160 ONE = new Number160(1);

    /**
     * Create a Key with value 0.
     */
    public Number160() {
        this.val = new int[INT_ARRAY_SIZE];
    }

    /**
     * Create an instance with an integer array. This integer array will be copied into the backing array.
     * 
     * @param val
     *            The value to copy to the backing array. Since this class stores 160bit numbers, the array needs to be
     *            of size 5 or smaller.
     */
    public Number160(final int... val) {
        if (val.length > INT_ARRAY_SIZE) {
            throw new IllegalArgumentException("Can only deal with arrays of smaller or equal " + INT_ARRAY_SIZE
                    + ". Your array has " + val.length);
        }
        this.val = new int[INT_ARRAY_SIZE];
        final int len = val.length;
        for (int i = len - 1, j = INT_ARRAY_SIZE - 1; i >= 0; i--, j--) {
            this.val[j] = val[i];
        }
    }

    /**
     * Create a Key from a string. The string has to be of length 40 to fit into the backing array. Note that this
     * string is *always* in hexadecimal, there is no 0x... required before the number.
     * 
     * @param val
     *            The characters allowed are [0-9a-f], which is in hexadecimal
     */
    public Number160(final String val) {
        if (val.length() > STRING_LENGTH) {
            throw new IllegalArgumentException(
                    "Can only deal with strings of size smaller or equal than 42. Your string has " + val.length());
        }
        if (val.indexOf("0x") != 0) {
            throw new IllegalArgumentException(val
                    + " is not in hexadecimal form. Decimal form is not supported yet");
        }
        this.val = new int[INT_ARRAY_SIZE];
        final char[] tmp = val.toCharArray();
        final int len = tmp.length;
        for (int i = STRING_LENGTH - len, j = 2; i < (STRING_LENGTH - 2); i++, j++) {
            // CHECKSTYLE:OFF
            this.val[i >> 3] <<= 4;

            int digit = Character.digit(tmp[j], 16);
            if (digit < 0) {
                throw new RuntimeException("Not a hexadecimal number \"" + tmp[j] + "\". The range is [0-9a-f]");
            }
            // += or |= does not matter here
            this.val[i >> 3] += digit & CHAR_MASK;
            // CHECKSTYLE:ON
        }
    }

    /**
     * Creates a Key with the integer value.
     * 
     * @param val
     *            integer value
     */
    public Number160(final int val) {
        this.val = new int[INT_ARRAY_SIZE];
        this.val[INT_ARRAY_SIZE - 1] = val;
    }

    /**
     * Creates a Key with the long value.
     * 
     * @param val
     *            long value
     */
    public Number160(final long val) {
        this.val = new int[INT_ARRAY_SIZE];
        this.val[INT_ARRAY_SIZE - 1] = (int) val;
        this.val[INT_ARRAY_SIZE - 2] = (int) (val >> Integer.SIZE);
    }

    /**
     * Creates a new Key using the byte array. The array is copied to the backing int[]
     * 
     * @param val
     *            byte array
     */
    public Number160(final byte[] val) {
        this(val, 0, val.length);
    }

    /**
     * Creates a new Key using the byte array. The array is copied to the backing int[] starting at the given offest.
     * 
     * @param val
     *            byte array
     * @param offset
     *            The offset where to start
     * @param length
     *            the length to read
     */
    public Number160(final byte[] val, final int offset, final int length) {
        if (length > BYTE_ARRAY_SIZE) {
            throw new IllegalArgumentException(
                    "Can only deal with byte arrays of size smaller or equal than 20. Your array has " + length);
        }
        this.val = new int[INT_ARRAY_SIZE];
        for (int i = length + offset - 1, j = BYTE_ARRAY_SIZE - 1, k = 0; i >= offset; i--, j--, k++) {
            // += or |= does not matter here
            // CHECKSTYLE:OFF
            this.val[j >> 2] |= (val[i] & BYTE_MASK) << ((k % 4) << 3);
            // CHECKSTYLE:ON
        }
    }

    /**
     * Creates a new Key with random values in it.
     * 
     * @param random
     *            The object to create pseudo random numbers. For testing and debugging, the seed in the random class
     *            can be set to make the random values repeatable.
     */
    public Number160(final Random random) {
        this.val = new int[INT_ARRAY_SIZE];
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
            this.val[i] = random.nextInt();
        }
    }

    /**
     * Xor operation. This operation is ~2.5 times faster than with BigInteger
     * 
     * @param key
     *            The second operand for the xor operation
     * @return A new key with the resurt of the xor operation
     */
    public Number160 xor(final Number160 key) {
        final int[] result = new int[INT_ARRAY_SIZE];
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
            result[i] = this.val[i] ^ key.val[i];
        }
        return new Number160(result);
    }

    /**
     * Returns a copy of the backing array, which is always of size 5.
     * 
     * @return a copy of the backing array
     */
    public int[] toIntArray() {
        final int[] retVal = new int[INT_ARRAY_SIZE];
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
            retVal[i] = this.val[i];
        }
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
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
            // multiply by four
            final int idx = offset + (i << 2);
            // CHECKSTYLE:OFF
            me[idx + 0] = (byte) (val[i] >> 24);
            me[idx + 1] = (byte) (val[i] >> 16);
            me[idx + 2] = (byte) (val[i] >> 8);
            me[idx + 3] = (byte) (val[i]);
            // CHECKSTYLE:ON
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
     * @param removeLeadingZero
     *            Indicates of leading zeros should be removed
     * @return A human readable representation of this key
     */
    public String toString(final boolean removeLeadingZero) {
        boolean removeZero = removeLeadingZero;
        final StringBuilder sb = new StringBuilder("0x");
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
            toHex(val[i], removeZero, sb);
            if (removeZero && val[i] != 0) {
                removeZero = false;
            }
        }
        return sb.toString();
    }

    /**
     * Checks if this number is zero.
     * 
     * @return True if this number is zero, false otherwise
     */
    public boolean isZero() {
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
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
        int bits = 0;
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
            if (this.val[i] != 0) {
                bits += Integer.SIZE - Integer.numberOfLeadingZeros(this.val[i]);
                bits += Integer.SIZE * (INT_ARRAY_SIZE - ++i);
                break;
            }
        }
        return bits;
    }

    @Override
    public String toString() {
        return toString(true);
    }

    @Override
    public double doubleValue() {
        double d = 0;
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
            d *= LONG_MASK + 1;
            d += this.val[i] & LONG_MASK;
        }
        return d;
    }

    @Override
    public float floatValue() {
        return (float) doubleValue();
    }

    @Override
    public int intValue() {
        return this.val[INT_ARRAY_SIZE - 1];
    }

    /**
     * For debugging...
     * 
     * @param pos
     *            the position in the internal array
     * @return the long of the unsigned int
     */
    long unsignedInt(final int pos) {
        return this.val[pos] & LONG_MASK;
    }

    @Override
    public long longValue() {
        return ((this.val[INT_ARRAY_SIZE - 1] & LONG_MASK) << Integer.SIZE)
                + (this.val[INT_ARRAY_SIZE - 2] & LONG_MASK);
    }

    @Override
    public int compareTo(final Number160 o) {
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
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
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Number160)) {
            return false;
        }
        final Number160 key = (Number160) obj;
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
            if (key.val[i] != val[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
            // CHECKSTYLE:OFF
            hashCode = (int) (31 * hashCode + (val[i] & LONG_MASK));
            // CHECKSTYLE:ON
        }
        return hashCode;
    }

    /**
     * Convert an integer to hex value.
     * 
     * @param integer2
     *            The integer to convert
     * @param removeLeadingZero
     *            idicate if leading zeros should be ignored
     * @param sb
     *            The string bulider where to store the result
     */
    private static void toHex(final int integer2, final boolean removeLeadingZero, final StringBuilder sb) {
        // 4 bits form a char, thus we have 160/4=40 chars in a key, with an
        // integer array size of 5, this gives 8 chars per integer

        final char[] buf = new char[CHARS_PER_INT];
        int charPos = CHARS_PER_INT;
        int integer = integer2;
        for (int i = 0; i < CHARS_PER_INT && !(removeLeadingZero && integer == 0); i++) {
            buf[--charPos] = DIGITS[integer & CHAR_MASK];
            // for hexadecimal, we have 4 bits per char, which ranges from
            // [0-9a-f]
            // CHECKSTYLE:OFF
            integer >>>= 4;
            // CHECKSTYLE:ON
        }
        sb.append(buf, charPos, (CHARS_PER_INT - charPos));
    }

    /**
     * Create a new Number160 from the integer, which fills all the 160bits. A new random object will be created.
     * 
     * @param integerValue
     *            The value to hash from
     * @return A hash from based on pseudo random, to fill the 160bits
     */
    public static Number160 createHash(final int integerValue) {
        Random r = new Random(integerValue);
        return new Number160(r);
    }

    /**
     * Create a new Number160 from the long, which fills all the 160bit. A new random object will be created, thus, its
     * thread safe
     * 
     * @param longValue
     *            The value to hash from
     * @return A hash based on pseudo random, to fill the 160bits
     */
    public static Number160 createHash(final long longValue) {
        Random r = new Random(longValue);
        return new Number160(r);
    }

    /**
     * Create a new Number160 using SHA1 on the string.
     * 
     * @param string
     *            The value to hash from
     * @return A hash based on SHA1 of the string
     */
    public static Number160 createHash(final String string) {
        return Utils.makeSHAHash(string);
    }
}
