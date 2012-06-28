package net.tomp2p.rpc;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * A simple Bloom Filter (see http://en.wikipedia.org/wiki/Bloom_filter) that
 * uses java.util.Random as a primitive hash function, and which implements
 * Java's Set interface for convenience.
 * 
 * Only the add(), addAll(), contains(), and containsAll() methods are
 * implemented. Calling any other method will yield an
 * UnsupportedOperationException.
 * 
 * This code may be used, modified, and redistributed provided that the author
 * tag below remains intact.
 * 
 * @author Ian Clarke <ian@uprizer.com>
 * @author Thomas Bocek <tom@tomp2p.net> Added methods to get and create a
 *         SimpleBloomFilter from existing data. The data can be either a BitSet
 *         or a bye[].
 * 
 * @param <E>
 *            The type of object the BloomFilter should contain
 */
public class SimpleBloomFilter<E> implements Set<E>, Serializable
{
	private static final long serialVersionUID = 3527833617516722215L;
	private final int k;
	private final BitSet bitSet;
	private final int bitArraySize, expectedElements;

	/**
	 * Construct an empty SimpleBloomFilter. You must specify the number of bits
	 * in the Bloom Filter, and also you should specify the number of items you
	 * expect to add. The latter is used to choose some optimal internal values
	 * to minimize the false-positive rate (which can be estimated with
	 * expectedFalsePositiveRate()).
	 * 
	 * @param bitArraySize
	 *            The number of bits in the bit array (often called 'm' in the
	 *            context of bloom filters).
	 * @param expectedElements
	 *            The typical number of items you expect to be added to the
	 *            SimpleBloomFilter (often called 'n').
	 */
	public SimpleBloomFilter(int bitArraySize, int expectedElements)
	{
		this(bitArraySize, expectedElements, new BitSet(bitArraySize));
	}

	/**
	 * Constructs a SimpleBloomFilter out of existing data. You must specify the
	 * number of bits in the Bloom Filter, and also you should specify the
	 * number of items you expect to add. The latter is used to choose some
	 * optimal internal values to minimize the false-positive rate (which can be
	 * estimated with expectedFalsePositiveRate()).
	 * 
	 * @param bitArraySize
	 *            The number of bits in the bit array (often called 'm' in the
	 *            context of bloom filters).
	 * @param expectedElements
	 *            The typical number of items you expect to be added to the
	 *            SimpleBloomFilter (often called 'n').
	 * @param rawBitArray
	 *            The data that will be used in the backing BitSet
	 */
	public SimpleBloomFilter(byte[] rawBitArray)
	{
		this(rawBitArray, 0, rawBitArray.length);
	}
	
	public SimpleBloomFilter(ChannelBuffer channelBuffer, int length)
	{
		int bitArraySize = channelBuffer.readInt();
		int expectedElements = channelBuffer.readInt();
		if (bitArraySize % 8 != 0)
		{
			throw new RuntimeException("BitArraySize must be a multiple of 8, it is "+bitArraySize);
		}
		this.bitArraySize = bitArraySize;
		this.expectedElements = expectedElements;
		this.k = (int) Math.ceil((bitArraySize / (double) expectedElements) * Math.log(2.0));
		int arrayLength = length - 8;
		if(arrayLength > 0)
		{
			byte[] me = new byte[arrayLength];
			channelBuffer.readBytes(me);
			this.bitSet = fromByteArray(new BitSet(), me, 0, arrayLength);
		}
		else
		{
			this.bitSet = new BitSet();
		}
	}

	/**
	 * Constructs a SimpleBloomFilter out of existing data. You must specify the
	 * number of bits in the Bloom Filter, and also you should specify the
	 * number of items you expect to add. The latter is used to choose some
	 * optimal internal values to minimize the false-positive rate (which can be
	 * estimated with expectedFalsePositiveRate()).
	 * 
	 * @param bitArraySize
	 *            The number of bits in the bit array (often called 'm' in the
	 *            context of bloom filters).
	 * @param expectedElements
	 *            The typical number of items you expect to be added to the
	 *            SimpleBloomFilter (often called 'n').
	 * @param rawBitArray
	 *            The data that will be used in the backing BitSet
	 * @param offset
	 *            The offset of the array
	 * @param length
	 *            The length of the array
	 */
	public SimpleBloomFilter(byte[] rawBitArray, int offset, int length)
	{
		this(byteArrayToInt(rawBitArray, 4 + offset), byteArrayToInt(rawBitArray, 0 + offset), fromByteArray(new BitSet(), rawBitArray,
				offset + 8, length - 8));
	}

	/**
	 * Constructs a SimpleBloomFilter out of existing data. You must specify the
	 * number of bits in the Bloom Filter, and also you should specify the
	 * number of items you expect to add. The latter is used to choose some
	 * optimal internal values to minimize the false-positive rate (which can be
	 * estimated with expectedFalsePositiveRate()).
	 * 
	 * @param bitArraySize
	 *            The number of bits in the bit array (often called 'm' in the
	 *            context of bloom filters).
	 * @param expectedElements
	 *            he typical number of items you expect to be added to the
	 *            SimpleBloomFilter (often called 'n').
	 * @param bitSet
	 *            The data that will be used in the backing BitSet
	 * @throws RuntimeException
	 *             If bitArraySize is not a multiple of eight.
	 */
	public SimpleBloomFilter(int bitArraySize, int expectedElements, BitSet bitSet)
	{
		if (bitArraySize % 8 != 0)
		{
			throw new RuntimeException("BitArraySize must be a multiple of 8, it is "+bitArraySize);
		}
		this.bitArraySize = bitArraySize;
		this.expectedElements = expectedElements;
		this.k = (int) Math.ceil((bitArraySize / (double) expectedElements) * Math.log(2.0));
		this.bitSet = bitSet;
	}

	/**
	 * Calculates the approximate probability of the contains() method returning
	 * true for an object that had not previously been inserted into the bloom
	 * filter. This is known as the "false positive probability".
	 * 
	 * @return The estimated false positive rate
	 */
	public double expectedFalsePositiveProbability()
	{
		return Math.pow((1 - Math.exp(-k * (double) expectedElements / bitArraySize)), k);
	}

	/**
	 * Returns the expected elements that was provided by the user.
	 * 
	 * @return The expected elements that was provided by the user
	 */
	public int getExpectedElements()
	{
		return expectedElements;
	}

	/*
	 * @return This method will always return false
	 * 
	 * @see java.util.Set#add(java.lang.Object)
	 */
	public boolean add(E o)
	{
		Random r = new Random(o.hashCode());
		for (int x = 0; x < k; x++)
		{
			bitSet.set(r.nextInt(bitArraySize), true);
		}
		return false;
	}

	/**
	 * @return This method will always return false
	 */
	public boolean addAll(Collection<? extends E> c)
	{
		for (E o : c)
		{
			add(o);
		}
		return false;
	}

	/**
	 * Clear the Bloom Filter
	 */
	public void clear()
	{
		for (int x = 0; x < bitSet.length(); x++)
		{
			bitSet.set(x, false);
		}
	}

	/**
	 * @return False indicates that o was definitely not added to this Bloom
	 *         Filter, true indicates that it probably was. The probability can
	 *         be estimated using the expectedFalsePositiveProbability() method.
	 */
	public boolean contains(Object o)
	{
		Random r = new Random(o.hashCode());
		for (int x = 0; x < k; x++)
		{
			if (!bitSet.get(r.nextInt(bitArraySize)))
				return false;
		}
		return true;
	}

	public boolean containsAll(Collection<?> c)
	{
		for (Object o : c)
		{
			if (!contains(o))
				return false;
		}
		return true;
	}

	/**
	 * Not implemented
	 */
	public boolean isEmpty()
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public Iterator<E> iterator()
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public boolean remove(Object o)
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public boolean removeAll(Collection<?> c)
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public boolean retainAll(Collection<?> c)
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public int size()
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public Object[] toArray()
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public <T> T[] toArray(T[] a)
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns the bitset that backs the bloom filter
	 * 
	 * @return bloom filter as a bitset
	 */
	public BitSet getBitSet()
	{
		return bitSet;
	}

	/**
	 * Sets bits in the bitset from a byte[]
	 * 
	 * @param bits
	 *            The bits that will be filled
	 * @param bytes
	 *            The data
	 * @return The filled bitset. It is the same object as passed as argument.
	 */
	private static BitSet fromByteArray(BitSet bits, byte[] bytes, int offset, int length)
	{
		for (int i = 0; i < length * 8; i++)
		{
			if ((bytes[offset + (length - i / 8 - 1)] & (1 << (i % 8))) > 0)
			{
				bits.set(i);
			}
		}
		return bits;
	}

	/**
	 * Returns a byte array of at least length 8. The most significant bit in
	 * the result is guaranteed not to be a 1 (since BitSet does not support
	 * sign extension). The byte-ordering of the result is big-endian which
	 * means the most significant bit is in element 0. The bit at index 0 of the
	 * bit set is assumed to be the least significant bit.
	 * 
	 * @return a byte array representation of the bitset and the expected
	 *         elements
	 */
	public byte[] toByteArray()
	{
		if (bitSet.length() == 0)
			return intToByteArray(expectedElements, bitArraySize, new byte[8]);
		int length = (bitSet.length() / 8 + 1);
		byte[] bytes = new byte[8 + length];
		intToByteArray(expectedElements, bitArraySize, bytes);

		for (int i = 0; i < bitSet.length(); i++)
		{
			if (bitSet.get(i))
			{
				bytes[8 + (length - i / 8 - 1)] |= 1 << (i % 8);
			}
		}
		return bytes;
	}

	public static final byte[] intToByteArray(int value1, int value2, byte[] me)
	{
		me[0] = (byte) (value1 >>> 24);
		me[1] = (byte) (value1 >>> 16);
		me[2] = (byte) (value1 >>> 8);
		me[3] = (byte) (value1);
		me[4] = (byte) (value2 >>> 24);
		me[5] = (byte) (value2 >>> 16);
		me[6] = (byte) (value2 >>> 8);
		me[7] = (byte) (value2);
		return me;
	}

	public static final int byteArrayToInt(byte[] me, int offset)
	{
		return (me[0 + offset] << 24) + ((me[1 + offset] & 0xFF) << 16) + ((me[2 + offset] & 0xFF) << 8)
				+ (me[3 + offset] & 0xFF);
	}

	public SimpleBloomFilter<E> merge(SimpleBloomFilter<E> toMerge)
	{
		if (toMerge.bitArraySize != bitArraySize)
			throw new RuntimeException("this is not supposed to happen");
		BitSet mergedBitSet = (BitSet) bitSet.clone();
		mergedBitSet.or(toMerge.bitSet);
		return new SimpleBloomFilter<E>(bitArraySize, expectedElements, mergedBitSet);
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if(!(obj instanceof SimpleBloomFilter))
		{
			return false;
		}
		@SuppressWarnings("unchecked")
		SimpleBloomFilter<E> o = (SimpleBloomFilter<E>)obj;
		return o.k == k 
				&& o.bitArraySize == bitArraySize 
				&& expectedElements == o.expectedElements 
				&& bitSet.equals(o.bitSet);
	}
	
	@Override
	public int hashCode()
	{
		int hash = 7;
		hash = 31 * hash + bitSet.hashCode();
		hash = 31 * hash + k;
		hash = 31 * hash + expectedElements;
		hash = 31 * hash + bitArraySize;
		return hash;
	}
	
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		int length = bitSet.length();
		for(int i=0;i<length;i++)
		{
			sb.append(bitSet.get(i)?"1":"0");
		}
		return sb.toString();
	}
}