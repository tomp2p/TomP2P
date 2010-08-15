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
 * 
 * This class represents a 160 bit number. This class is preferred over
 * BigInteger as we always have 160bit, and thus, methods can be optimized.
 * 
 * @author Thomas Bocek
 * 
 */
final public class Number160 extends Number implements Comparable<Number160>
{
	private final static long serialVersionUID = -6386562272459272306L;
	// This key has *always* 160 bit. Do not change.
	public final static int BITS = 160;
	public final static Number160 MAX_VALUE = new Number160(new int[] { -1, -1, -1, -1, -1 });
	private final static long LONG_MASK = 0xffffffffL;
	private final static int BYTE_MASK = 0xff;
	private final static int CHAR_MASK = 0xf;
	// a map used for String <-> Key conversion
	private final static char[] DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
			'b', 'c', 'd', 'e', 'f' };
	// size of the backing integer array
	public final static int INT_ARRAY_SIZE = BITS / 32;
	// size of a byte array
	public final static int BYTE_ARRAY_SIZE = BITS / 8;
	// backing integer array
	private final int[] val;
	// constants
	public final static Number160 ZERO = new Number160(0);
	public final static Number160 ONE = new Number160(1);

	/**
	 * Create a Key with value 0.
	 */
	public Number160()
	{
		this.val = new int[INT_ARRAY_SIZE];
	}

	public Number160(ShortString shortString)
	{
		this.val = shortString.toNumber160().val;
	}

	/**
	 * Create an instance with an integer array. This integer array will be
	 * copied into the backing array.
	 * 
	 * @param val The value to copy to the backing array. Since this class
	 *        stores 160bit numbers, the array needs to be of size 5 or smaller.
	 */
	public Number160(final int... val)
	{
		if (val.length > INT_ARRAY_SIZE)
			throw new IllegalArgumentException("Can only deal with arrays of smaller or equal "
					+ INT_ARRAY_SIZE + ". Your array has " + val.length);
		this.val = new int[INT_ARRAY_SIZE];
		final int len = val.length;
		for (int i = len - 1, j = INT_ARRAY_SIZE - 1; i >= 0; i--, j--)
			this.val[j] = val[i];
	}

	/**
	 * Create a Key from a string. The string has to be of length 40 to fit into
	 * the backing array. Note that this string is *always* in hexadecimal,
	 * there is no 0x... required before the number.
	 * 
	 * @param val The characters allowed are [0-9a-f], which is in hexadecimal
	 */
	public Number160(final String val)
	{
		if (val.length() > 42)
		{
			throw new IllegalArgumentException(
					"Can only deal with strings of size smaller or equal than 42. Your string has "
							+ val.length());
		}
		if (val.indexOf("0x") != 0)
		{
			throw new IllegalArgumentException(val
					+ " is not in hexadecimal form. Decimal form is not supported yet");
		}
		this.val = new int[INT_ARRAY_SIZE];
		final char[] tmp = val.toCharArray();
		final int len = tmp.length;
		for (int i = 42 - len, j = 2; i < 40; i++, j++)
		{
			this.val[i >> 3] <<= 4;
			int digit = Character.digit(tmp[j], 16);
			if (digit < 0)
				throw new RuntimeException("Not a hexadecimal number \"" + tmp[j]
						+ "\". The range is [0-9a-f]");
			// += or |= does not matter here
			this.val[i >> 3] += digit & CHAR_MASK;
		}
	}

	/**
	 * Creates a Key with the integer value
	 * 
	 * @param val integer value
	 */
	public Number160(final int val)
	{
		this.val = new int[INT_ARRAY_SIZE];
		this.val[INT_ARRAY_SIZE - 1] = val;
	}

	public Number160(final long val)
	{
		this.val = new int[INT_ARRAY_SIZE];
		this.val[INT_ARRAY_SIZE - 1] = (int) val;
		this.val[INT_ARRAY_SIZE - 2] = (int) (val >> 32);
	}

	/**
	 * Creates a new Key using the byte array. The array is copied to the
	 * backing int[]
	 * 
	 * @param val
	 */
	public Number160(final byte[] val)
	{
		this(val, 0, val.length);
	}

	/**
	 * Creates a new Key using the byte array. The array is copied to the
	 * backing int[] starting at the given offest.
	 * 
	 * @param val
	 * @param offset The offset where to start
	 */
	public Number160(final byte[] val, final int offset, final int length)
	{
		if (length > 20)
		{
			throw new IllegalArgumentException(
					"Can only deal with byte arrays of size smaller or equal than 20. Your array has "
							+ length);
		}
		this.val = new int[INT_ARRAY_SIZE];
		for (int i = length + offset - 1, j = 20 - 1, k = 0; i >= offset; i--, j--, k++)
			// += or |= does not matter here
			this.val[j >> 2] |= (val[i] & BYTE_MASK) << ((k % 4) << 3);
	}

	/**
	 * Creates a new Key with random values in it.
	 * 
	 * @param random The object to create pseudo random numbers. For testing and
	 *        debugging, the seed in the random class can be set to make the
	 *        random values repeatable.
	 */
	public Number160(final Random random)
	{
		this.val = new int[INT_ARRAY_SIZE];
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
			this.val[i] = random.nextInt();
	}

	/**
	 * Xor operation. This operation is ~2.5 times faster than with BigInteger
	 * 
	 * @param key The second operand for the xor operation
	 * @return A new key with the resurt of the xor operation
	 */
	public Number160 xor(final Number160 key)
	{
		final int[] result = new int[INT_ARRAY_SIZE];
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
			result[i] = this.val[i] ^ key.val[i];
		return new Number160(result);
	}

	/**
	 * Shift right the 160bit number.
	 * 
	 * @param nr The number of bits to be shifted
	 * @return A new 160bit number with the shifted number
	 */
	public Number160 shiftRight(int nr)
	{
		final int[] result = new int[INT_ARRAY_SIZE];
		// do the array shifting stuff
		final int[] val2 = new int[INT_ARRAY_SIZE];
		if (nr >= 32)
		{
			int tmp = nr / 32;
			for (int i = INT_ARRAY_SIZE - 1; (i - tmp) >= 0; i--)
				val2[i] = this.val[i - tmp];
		}
		else
			System.arraycopy(val, 0, val2, 0, INT_ARRAY_SIZE);
		// do the bit shifting and overlapping stuff
		nr = nr % 32;
		if (nr == 0)
			return new Number160(val2);
		final int mask = -1 >>> (32 - nr);
		for (int i = INT_ARRAY_SIZE - 1; i >= 0; i--)
		{
			result[i] = val2[i] >>> nr;
			// look forward and handle overlaps
			if (i > 0)
			{
				int overlap = val2[i - 1] & mask;
				overlap = overlap << (32 - nr);
				result[i] |= overlap;
			}
		}
		return new Number160(result);
	}

	/**
	 * Shift left the 160bit number.
	 * 
	 * @param nr The number of bits to be shifted
	 * @return A new 160bit number with the shifted number
	 */
	public Number160 shiftLeft(int nr)
	{
		final int[] result = new int[INT_ARRAY_SIZE];
		// do the array shifting stuff
		final int[] val2 = new int[INT_ARRAY_SIZE];
		if (nr >= 32)
		{
			int tmp = nr / 32;
			for (int i = INT_ARRAY_SIZE - 1; (i - tmp) >= 0; i--)
				val2[i - tmp] = this.val[i];
		}
		else
			System.arraycopy(val, 0, val2, 0, INT_ARRAY_SIZE);
		// do the bit shifting and overlapping stuff
		nr = nr % 32;
		if (nr == 0)
			return new Number160(val2);
		final int mask = (-1 >>> (32 - nr)) << (32 - nr);
		for (int i = INT_ARRAY_SIZE - 1; i >= 0; i--)
		{
			result[i] = val2[i] << nr;
			// look forward and handle overlaps
			if (i < INT_ARRAY_SIZE - 1)
			{
				int overlap = val2[i + 1] & mask;
				overlap = overlap >>> (32 - nr);
				result[i] |= overlap;
			}
		}
		return new Number160(result);
	}

	/**
	 * Returns a copy of the backing array, which is always of size 5.
	 * 
	 * @return a copy of the backing array
	 */
	public int[] toIntArray()
	{
		final int[] retVal = new int[INT_ARRAY_SIZE];
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
			retVal[i] = this.val[i];
		return retVal;
	}

	/**
	 * Fills the byte array with this number
	 * 
	 * @param me
	 */
	public void toByteArray(byte[] me, int offset)
	{
		if (offset + BYTE_ARRAY_SIZE > me.length)
			throw new RuntimeException("array too small");
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
		{
			final int idx = offset + (i << 2);
			me[idx + 0] = (byte) (val[i] >> 24);
			me[idx + 1] = (byte) (val[i] >> 16);
			me[idx + 2] = (byte) (val[i] >> 8);
			me[idx + 3] = (byte) (val[i]);
		}
	}

	/**
	 * Returns a byte array, which is always of size 20.
	 * 
	 * @return a byte array
	 */
	public byte[] toByteArray()
	{
		final byte[] retVal = new byte[BYTE_ARRAY_SIZE];
		toByteArray(retVal, 0);
		return retVal;
	}

	/**
	 * Shows the content in a human readable manner
	 * 
	 * @param removeLeadingZero Indicates of leading zeros should be removed
	 * @return A human readable representation of this key
	 */
	public String toString(boolean removeLeadingZero)
	{
		final StringBuilder sb = new StringBuilder("0x");
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
		{
			toHex(val[i], removeLeadingZero, sb);
			if (removeLeadingZero && val[i] != 0)
				removeLeadingZero = false;
		}
		return sb.toString();
	}

	/**
	 * Checks if this number is zero
	 * 
	 * @return True if this number is zero, false otherwise
	 */
	public boolean isZero()
	{
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
		{
			if (this.val[i] != 0)
				return false;
		}
		return true;
	}

	/**
	 * Calculates the number of bits used to represent this number. All leading
	 * (leftmost) zero bits are ignored
	 * 
	 * @return The bits used
	 */
	public int bitLength()
	{
		int bits = 0;
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
		{
			if (this.val[i] != 0)
			{
				bits += 32 - Integer.numberOfLeadingZeros(this.val[i]);
				bits += 32 * (INT_ARRAY_SIZE - ++i);
				break;
			}
		}
		return bits;
	}

	@Override
	public String toString()
	{
		return toString(true);
	}

	@Override
	public double doubleValue()
	{
		double d = 0;
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
		{
			d *= LONG_MASK + 1;
			d += this.val[i] & LONG_MASK;
		}
		return d;
	}

	@Override
	public float floatValue()
	{
		return (float) doubleValue();
	}

	@Override
	public int intValue()
	{
		return this.val[INT_ARRAY_SIZE - 1];
	}

	/**
	 * For debugging...
	 * 
	 * @param pos
	 * @return
	 */
	public long unsignedInt(int pos)
	{
		return this.val[pos] & LONG_MASK;
	}

	@Override
	public long longValue()
	{
		return ((this.val[INT_ARRAY_SIZE - 1] & LONG_MASK) << 32)
				+ (this.val[INT_ARRAY_SIZE - 2] & LONG_MASK);
	}

	@Override
	public int compareTo(final Number160 o)
	{
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
		{
			long b1 = val[i] & LONG_MASK;
			long b2 = o.val[i] & LONG_MASK;
			if (b1 < b2)
				return -1;
			else if (b1 > b2)
				return 1;
		}
		return 0;
	}

	@Override
	public boolean equals(final Object obj)
	{
		if (obj == this)
			return true;
		if (!(obj instanceof Number160))
			return false;
		final Number160 key = (Number160) obj;
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
			if (key.val[i] != val[i])
				return false;
		return true;
	}

	@Override
	public int hashCode()
	{
		int hashCode = 0;
		for (int i = 0; i < INT_ARRAY_SIZE; i++)
			hashCode = (int) (31 * hashCode + (val[i] & LONG_MASK));
		return hashCode;
	}

	/**
	 * Convert an integer to hex value
	 * 
	 * @param integer The integer to convert
	 * @param removeLeadingZero idicate if leading zeros should be ignored
	 * @param sb The string bulider where to store the result
	 */
	private static void toHex(int integer, final boolean removeLeadingZero, final StringBuilder sb)
	{
		// 4 bits form a char, thus we have 160/4=40 chars in a key, with an
		// integer array size of 5, this gives 8 chars per integer
		final int CHARS_PER_INT = 8;
		final char[] buf = new char[CHARS_PER_INT];
		int charPos = CHARS_PER_INT;
		for (int i = 0; i < CHARS_PER_INT && !(removeLeadingZero && integer == 0); i++)
		{
			buf[--charPos] = DIGITS[integer & CHAR_MASK];
			// for hexadecimal, we have 4 bits per char, which ranges from
			// [0-9a-f]
			integer >>>= 4;
		}
		sb.append(buf, charPos, (CHARS_PER_INT - charPos));
	}

	/**
	 * Create a new Number160 from the integer, which fills all the 160bits. A
	 * new random object will be created.
	 * 
	 * @param integerValue The value to hash from
	 * @return A hash from based on pseudo random, to fill the 160bits
	 */
	public static Number160 createHash(int integerValue)
	{
		Random r = new Random(integerValue);
		return new Number160(r);
	}

	/**
	 * Create a new Number160 from the long, which fills all the 160bit. A new
	 * random object will be created, thus, its thread safe
	 * 
	 * @param longValue The value to hash from
	 * @return A hash based on pseudo random, to fill the 160bits
	 */
	public static Number160 createHash(long longValue)
	{
		Random r = new Random(longValue);
		return new Number160(r);
	}

	/**
	 * Create a new Number160 using SHA1 on the string.
	 * 
	 * @param string The value to hash from
	 * @return A hash based on SHA1 of the string
	 */
	public static Number160 createHash(String string)
	{
		return Utils.makeSHAHash(string);
	}
}
