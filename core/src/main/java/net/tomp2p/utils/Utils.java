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
package net.tomp2p.utils;

import io.netty.buffer.ByteBuf;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.SequenceInputStream;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.DataBuffer;

/**
 * 
 * @author Thomas Bocek
 * @author Maxat Pernebayev
 * 
 */
public class Utils {
	private static final Random random = new Random();
	public static final int IPV4_BYTES = 4;
	public static final int IPV6_BYTES = 16;
	public static final int BYTE_BITS = 8;
	public static final int MASK_FF = 0xff;
	public static final int MASK_80 = 0x80;
	public static final int INTEGER_BYTE_SIZE = 4;
	public static final int LONG_BYTE_SIZE = 8;
	public static final int BYTE_SIZE = 1;
	public static final int SHORT_BYTE_SIZE = 2;
	public static final int MASK_0F = 0xf;
	public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

	public static ByteBuffer loadFile(File file) throws IOException {
		FileInputStream fis = null;
		FileChannel channel = null;
		try {
			fis = new FileInputStream(file);
			channel = fis.getChannel();
			return channel.map(MapMode.READ_ONLY, 0, channel.size());
		} finally {
			bestEffortclose(channel, fis);
		}
	}

	public static Number160 makeSHAHash(File file) {
		FileInputStream fis = null;
		FileChannel channel = null;
		try {
			fis = new FileInputStream(file);
			channel = fis.getChannel();
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			for (long offest = 0; offest < channel.size(); offest += 10 * 1024) {
				ByteBuffer buffer;
				if (channel.size() - offest < 10 * 1024)
					buffer = channel.map(MapMode.READ_ONLY, offest, (int) channel.size() - offest);
				else
					buffer = channel.map(MapMode.READ_ONLY, offest, 10 * 1024);
				md.update(buffer);
			}
			byte[] digest = md.digest();
			return new Number160(digest);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return Number160.ZERO;
		} catch (IOException e) {
			e.printStackTrace();
			return Number160.ZERO;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return Number160.ZERO;
		} finally {
			bestEffortclose(channel, fis);
		}
	}

	public static Number160 makeSHAHash(String strInput) {
		byte[] buffer = strInput.getBytes();
		return makeSHAHash(buffer);
	}

	public static Number160 makeSHAHash(ByteBuffer buffer) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(buffer);
			byte[] digest = md.digest();
			return new Number160(digest);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return new Number160();
		}
	}

	/**
	 * Calculates the SHA-1 hash of the Netty byte buffer.
	 * 
	 * @param buffer
	 *            The buffer that stores data
	 * @return The 160bit hash number
	 */
	public static Number160 makeSHAHash(final ByteBuf buf) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			final ByteBuffer[] byteBuffers = buf.nioBuffers();
			final int len = byteBuffers.length;
			for (int i = 0; i < len; i++) {
				md.update(byteBuffers[i]);
			}
			byte[] digest = md.digest();
			return new Number160(digest);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return new Number160();
		}
	}

	public static Number160 makeSHAHash(DataBuffer buffer) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			DataBuffer copy = buffer.shallowCopy();
			for (ByteBuffer byteBuffer : copy.bufferList()) {
				md.update(byteBuffer);
			}
			byte[] digest = md.digest();
			return new Number160(digest);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return new Number160();
		}
	}

	public static Number160 makeSHAHash(byte[] buffer) {
		return makeSHAHash(ByteBuffer.wrap(buffer));
	}

	public static Number160 makeSHAHash(byte[] buffer, int offset, int length) {
		return makeSHAHash(ByteBuffer.wrap(buffer, offset, length));
	}

	/**
	 * It returns MD5 hash for the buffer.
	 * 
	 * @param buffer
	 *            The buffer to generate the checksum from
	 * @return The strong checksum
	 */
	public static byte[] makeMD5Hash(byte[] buffer) {
		return makeMD5Hash(buffer, 0, buffer.length);
	}

	public static byte[] makeMD5Hash(byte[] buffer, int offset, int length) {
		MessageDigest m;
		try {
			m = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return new byte[0];
		}
		m.update(buffer, offset, length);
		return m.digest();
	}

	public static Number160 createRandomNodeID() {
		// TODO: this hardcoded, bad style
		byte[] me = new byte[20];
		random.nextBytes(me);
		Number160 id = new Number160(me);
		return id;
	}

	public static byte[] compress(byte[] input) {
		// Create the compressor with highest level of compression
		Deflater compressor = new Deflater();
		compressor.setLevel(Deflater.BEST_SPEED);
		// Give the compressor the data to compress
		compressor.setInput(input);
		compressor.finish();
		// Create an expandable byte array to hold the compressed data.
		// You cannot use an array that's the same size as the orginal because
		// there is no guarantee that the compressed data will be smaller than
		// the uncompressed data.
		ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);
		// Compress the data
		byte[] buf = new byte[1024];
		while (!compressor.finished()) {
			int count = compressor.deflate(buf);
			bos.write(buf, 0, count);
		}
		try {
			bos.close();
		} catch (IOException e) {
		}
		// Get the compressed data
		return bos.toByteArray();
	}

	public static byte[] uncompress(byte[] compressedData, int offset, int length) {
		// Create the decompressor and give it the data to compress
		Inflater decompressor = new Inflater();
		decompressor.setInput(compressedData, offset, length);
		// Create an expandable byte array to hold the decompressed data
		ByteArrayOutputStream bos = new ByteArrayOutputStream(length);
		// Decompress the data
		byte[] buf = new byte[1024];
		while (!decompressor.finished()) {
			try {
				int count = decompressor.inflate(buf);
				bos.write(buf, 0, count);
			} catch (DataFormatException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
		try {
			bos.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		// Get the decompressed data
		return bos.toByteArray();
	}

	public static byte[] uncompress(byte[] compressedData) {
		return uncompress(compressedData, 0, compressedData.length);
	}

	public static byte[] encodeJavaObject(Object attachement) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(attachement);
		// no need to call close of flush since we use ByteArrayOutputStream
		byte[] data = bos.toByteArray();
		return data;
	}

	public static Object decodeJavaObject(ByteBuf channelBuffer) throws ClassNotFoundException, IOException {
		InputStream is = new MultiByteBufferInputStream(channelBuffer);
		ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(is));
		Object obj = ois.readObject();
		ois.close();
		return obj;
	}

	public static synchronized Object decodeJavaObject(DataBuffer dataBuffer) throws ClassNotFoundException,
			IOException {

		List<ByteBuffer> buffers = dataBuffer.shallowCopy().bufferList();
		int count = buffers.size();
		Vector<InputStream> is = new Vector<InputStream>(count);
		for (ByteBuffer byteBuffer : buffers) {
			is.add(createInputStream(byteBuffer));
		}
		SequenceInputStream sis = new SequenceInputStream(is.elements());
		ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(sis));
		// TODO: investigate this issue
		/*
		 * ObjectInputStream ois = null; try { ois = new ObjectInputStream(new
		 * BufferedInputStream(sis)); } catch (Throwable t) { for (ByteBuffer
		 * byteBuffer : buffers) { byteBuffer.rewind(); int read =
		 * byteBuffer.capacity(); byteBuffer.limit(read); byte me[] = new
		 * byte[read]; byteBuffer.get(me);
		 * System.err.println("wrong array1 ("+System
		 * .identityHashCode(byteBuffer)+"): "+Arrays.toString(me));
		 * 
		 * if(dataBuffer.test!=null) { dataBuffer.test.readerIndex(0);
		 * dataBuffer.test.writerIndex(dataBuffer.test.capacity()); me = new
		 * byte[dataBuffer.test.readableBytes()]; dataBuffer.test.readBytes(me);
		 * System
		 * .err.println("wrong array2 ("+System.identityHashCode(byteBuffer
		 * )+"): "+Arrays.toString(me)); }
		 * 
		 * } t.printStackTrace(); }
		 */
		Object obj = ois.readObject();
		ois.close();
		return obj;
	}

	public static InputStream createInputStream(final ByteBuffer buf) {
		return new InputStream() {
			@Override
			public int read() throws IOException {
				if (!buf.hasRemaining()) {
					return -1;
				}
				return buf.get() & 0xFF;
			}

			@Override
			public int read(byte[] bytes, int off, int len) throws IOException {
				if (!buf.hasRemaining()) {
					return -1;
				}

				len = Math.min(len, buf.remaining());
				buf.get(bytes, off, len);
				return len;
			}
		};
	}

	public static Object decodeJavaObject(byte[] me, int offset, int length) throws ClassNotFoundException, IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(me, offset, length);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Object obj = ois.readObject();
		// no need to call close of flush since we use ByteArrayInputStream
		return obj;
	}

	/**
	 * Stores the differences of two collections in a result collection. The
	 * result will contain items from collection1 without those items that are
	 * in collection2.
	 * 
	 * @param collection1
	 *            The first collection (master collection) that will be iterated
	 *            and checked against duplicates in collection2.
	 * @param result
	 *            The collection to store the result
	 * @param collection2
	 *            The second collection that will be searched for duplicates
	 * @return Returns the collection the user specified as the resulting
	 *         collection
	 */
	public static <K> Collection<K> difference(Collection<K> collection1, Collection<K> result,
			Collection<K> collection2) {
		for (Iterator<K> iterator = collection1.iterator(); iterator.hasNext();) {
			K item = iterator.next();
			if (!collection2.contains(item)) {
				result.add(item);
			}
		}
		return result;
	}

	/**
	 * Stores the differences of multiple collections in a result collection.
	 * The result will contain items from collection1 without those items that
	 * are in collections2. The calling method might need to provide a
	 * 
	 * @SuppressWarnings("unchecked") since generics and arrays do not mix well.
	 * @param collection1
	 *            The first collection (master collection) that will be iterated
	 *            and checked against duplicates in collection2.
	 * @param result
	 *            The collection to store the result
	 * @param collection2
	 *            The second collections that will be searched for duplicates
	 * @return Returns the collection the user specified as the resulting
	 *         collection
	 */
	@SafeVarargs
	public static <K> Collection<K> difference(Collection<K> collection1, Collection<K> result,
			Collection<K>... collections2) {
		for (Iterator<K> iterator = collection1.iterator(); iterator.hasNext();) {
			K item = iterator.next();
			int size = collections2.length;
			boolean found = false;
			for (int i = 0; i < size; i++) {
				if (collections2[i].contains(item)) {
					found = true;
					break;
				}
			}
			if (!found) {
				result.add(item);
			}
		}
		return result;
	}

	public static void bestEffortclose(Closeable... closables) {
		// best effort close;
		for (Closeable closable : closables) {
			if (closable != null) {
				try {
					closable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static final byte[] intToByteArray(int value) {
		return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
	}

	public static final int byteArrayToInt(byte[] b) {
		return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8) + (b[3] & 0xFF);
	}

	/**
	 * Returns a random element from a collection. This method is pretty slow
	 * O(n), but the Java collection framework does not offer a better solution.
	 * This method puts the collection into a {@link List} and fetches a random
	 * element using {@link List#get(int)}.
	 * 
	 * @param collection
	 *            The collection from which we want to pick a random element
	 * @param rnd
	 *            The random object
	 * @return A random element
	 */
	public static <K> K pollRandom(Collection<K> collection, Random rnd) {
		int size = collection.size();
		if (size == 0) {
			return null;
		}
		int index = rnd.nextInt(size);
		List<K> values = new ArrayList<K>(collection);
		K retVal = values.get(index);
		// now we need to remove this element
		collection.remove(retVal);
		return retVal;
	}

	public static <K, V> Entry<K, V> pollRandomKey(Map<K, V> queueToAsk, Random rnd) {
		int size = queueToAsk.size();
		if (size == 0)
			return null;
		List<K> keys = new ArrayList<K>();
		keys.addAll(queueToAsk.keySet());
		int index = rnd.nextInt(size);
		final K key = keys.get(index);
		final V value = queueToAsk.remove(key);
		return new Entry<K, V>() {
			@Override
			public K getKey() {
				return key;
			}

			@Override
			public V getValue() {
				return value;
			}

			@Override
			public V setValue(V value) {
				return null;
			}
		};
	}

	public static <K> Collection<K> subtract(final Collection<K> a, final Collection<K> b) {
		ArrayList<K> list = new ArrayList<K>(a);
		for (Iterator<K> it = b.iterator(); it.hasNext();) {
			list.remove(it.next());
		}
		return list;
	}

	public static <K, V> Map<K, V> subtract(final Map<K, V> a, final Collection<K> b) {
		Map<K, V> map = new HashMap<K, V>(a);
		for (Iterator<K> it = b.iterator(); it.hasNext();) {
			map.remove(it.next());
		}
		return map;
	}

	public static <K, V> Map<K, V> disjunction(final Map<K, V> a, final Collection<K> b) {
		Map<K, V> map = new HashMap<K, V>();
		for (Iterator<Entry<K, V>> it = a.entrySet().iterator(); it.hasNext();) {
			Entry<K, V> entry = it.next();
			if (!b.contains(entry.getKey())) {
				map.put(entry.getKey(), entry.getValue());
			}
		}
		return map;
	}

	public static <K> Collection<K> limit(final Collection<K> a, final int size) {
		ArrayList<K> list = new ArrayList<K>();
		int i = 0;
		for (Iterator<K> it = a.iterator(); it.hasNext() && i < size;) {
			list.add(it.next());
		}
		return list;
	}

	public static <K, V> Map<K, V> limit(final Map<K, V> a, final int i) {
		Map<K, V> map = new HashMap<K, V>(a);
		int remove = a.size() - i;
		for (Iterator<K> it = a.keySet().iterator(); it.hasNext() && remove >= 0;) {
			map.remove(it.next());
			remove--;
		}
		return map;
	}

	public static String debugArray(byte[] array, int offset, int length) {
		String digits = "0123456789abcdef";
		StringBuilder sb = new StringBuilder(length * 2);
		for (int i = 0; i < length; i++) {
			int bi = array[offset + i] & 0xff;
			sb.append(digits.charAt(bi >> 4));
			sb.append(digits.charAt(bi & 0xf));
		}
		return sb.toString();
	}

	public static String debugArray(byte[] array) {
		return debugArray(array, 0, array.length);
	}

	/*
	 * public static boolean checkEntryProtection(Map<?, Data> dataMap) { for
	 * (Data data : dataMap.values()) { if (data.isProtectedEntry()) { return
	 * true; } } return false; }
	 */

	public static TrackerData limitRandom(TrackerData activePeers, int trackerSize) {
		// TODO Auto-generated method stub
		return activePeers;
	}

	public static <K> K getLast(List<K> list) {
		if (!list.isEmpty()) {
			return list.get(list.size() - 1);
		}
		return null;
	}

	/*
	 * Copyright (C) 2008 The Guava Authors Licensed under the Apache License,
	 * Version 2.0 (the "License"); you may not use this file except in
	 * compliance with the License. You may obtain a copy of the License at
	 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
	 * law or agreed to in writing, software distributed under the License is
	 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
	 * KIND, either express or implied. See the License for the specific
	 * language governing permissions and limitations under the License.
	 */
	/**
	 * Returns an Inet4Address having the integer value specified by the
	 * argument.
	 * 
	 * @param address
	 *            {@code int}, the 32bit integer address to be converted
	 * @return {@link Inet4Address} equivalent of the argument
	 */
	public static Inet4Address fromInteger(int address) {
		return getInet4Address(toByteArray(address));
	}

	/**
	 * Returns a big-endian representation of {@code value} in a 4-element byte
	 * array; equivalent to {@code ByteBuffer.allocate(4).putInt(value).array()}
	 * . For example, the input value {@code 0x12131415} would yield the byte
	 * array {@code 0x12, 0x13, 0x14, 0x15} .
	 * <p>
	 * If you need to convert and concatenate several values (possibly even of
	 * different types), use a shared {@link java.nio.ByteBuffer} instance, or
	 * use {@link com.google.common.io.ByteStreams#newDataOutput()} to get a
	 * growable buffer.
	 */
	private static byte[] toByteArray(int value) {
		return new byte[] { (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value };
	}

	/**
	 * Returns an {@link Inet4Address}, given a byte array representation of the
	 * IPv4 address.
	 * 
	 * @param bytes
	 *            byte array representing an IPv4 address (should be of length
	 *            4).
	 * @return {@link Inet4Address} corresponding to the supplied byte array.
	 * @throws IllegalArgumentException
	 *             if a valid {@link Inet4Address} can not be created.
	 */
	private static Inet4Address getInet4Address(byte[] bytes) {
		if (bytes.length != 4) {
			throw new IllegalArgumentException("Byte array has invalid length for an IPv4 address");
		}

		try {
			InetAddress ipv4 = InetAddress.getByAddress(bytes);
			if (!(ipv4 instanceof Inet4Address)) {
				throw new UnknownHostException(String.format("'%s' is not an IPv4 address.", ipv4.getHostAddress()));
			}
			return (Inet4Address) ipv4;
		} catch (UnknownHostException e) {
			/*
			 * This really shouldn't happen in practice since all our byte
			 * sequences should be valid IP addresses. However {@link
			 * InetAddress#getByAddress} is documented as potentially throwing
			 * this "if IP address is of illegal length". This is mapped to
			 * IllegalArgumentException since, presumably, the argument
			 * triggered some bizarre processing bug.
			 */
			throw new IllegalArgumentException(String.format("Host address '%s' is not a valid IPv4 address.",
					Arrays.toString(bytes)), e);
		}
	}

	// as seen here:
	// http://stackoverflow.com/questions/617414/create-a-temporary-directory-in-java
	public static File createTempDir() throws IOException {
		final File temp;
		temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

		if (!(temp.delete())) {
			throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
		}

		if (!(temp.mkdir())) {
			throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
		}
		return (temp);
	}

	public static void nullCheck(Object... objects) {
		int counter = 0;
		for (Object object : objects) {
			if (object == null) {
				throw new IllegalArgumentException("Null not allowed in paramenetr nr. " + counter);
			}
			counter++;
		}

	}

	public static boolean nullCheckRetVal(Object... objects) {
		for (Object object : objects) {
			if (object == null) {
				return true;
			}
		}
		return false;
	}

	public static Collection<Number160> extractContentKeys(Collection<Number480> collection) {
		Collection<Number160> result = new ArrayList<Number160>(collection.size());
		for (Number480 number480 : collection) {
			result.add(number480.contentKey());
		}
		return result;
	}

	/**
	 * Converts a byte array to a Inet4Address.
	 * 
	 * @param me
	 *            the byte array
	 * @param offset
	 *            where to start in the byte array
	 * @return The Inet4Address
	 * 
	 * @exception IndexOutOfBoundsException
	 *                if copying would cause access of data outside array bounds
	 *                for <code>src</code>.
	 * @exception NullPointerException
	 *                if either <code>src</code> is <code>null</code>.
	 */
	public static InetAddress inet4FromBytes(final byte[] src, final int offset) {
		// IPv4 is 32 bit
		byte[] tmp2 = new byte[IPV4_BYTES];
		System.arraycopy(src, offset, tmp2, 0, IPV4_BYTES);
		try {
			return Inet4Address.getByAddress(tmp2);
		} catch (UnknownHostException e) {
			/*
			 * This really shouldn't happen in practice since all our byte
			 * sequences have the right length. However {@link
			 * InetAddress#getByAddress} is documented as potentially throwing
			 * this "if IP address is of illegal length".
			 */
			throw new IllegalArgumentException(String.format("Host address '%s' is not a valid IPv4 address.",
					Arrays.toString(tmp2)), e);
		}
	}

	/**
	 * Converts a byte array to a Inet6Address.
	 * 
	 * @param me
	 *            me the byte array
	 * @param offset
	 *            where to start in the byte array
	 * @return The Inet6Address
	 * 
	 * @exception IndexOutOfBoundsException
	 *                if copying would cause access of data outside array bounds
	 *                for <code>src</code>.
	 * @exception NullPointerException
	 *                if either <code>src</code> is <code>null</code>.
	 */
	public static InetAddress inet6FromBytes(final byte[] me, final int offset) {
		// IPv6 is 128 bit
		byte[] tmp2 = new byte[IPV6_BYTES];
		System.arraycopy(me, offset, tmp2, 0, IPV6_BYTES);
		try {
			return Inet6Address.getByAddress(tmp2);
		} catch (UnknownHostException e) {
			/*
			 * This really shouldn't happen in practice since all our byte
			 * sequences have the right length. However {@link
			 * InetAddress#getByAddress} is documented as potentially throwing
			 * this "if IP address is of illegal length".
			 */
			throw new IllegalArgumentException(String.format("Host address '%s' is not a valid IPv4 address.",
					Arrays.toString(tmp2)), e);
		}
	}

	/**
	 * Convert a byte to a bit set. BitSet.valueOf(new byte[] {b}) is only
	 * available in 1.7, so we need to do this on our own.
	 * 
	 * @param b
	 *            The byte to be converted
	 * @return The resulting bit set
	 */
	public static BitSet createBitSet(final byte b) {
		final BitSet bitSet = new BitSet(8);
		for (int i = 0; i < Utils.BYTE_BITS; i++) {
			bitSet.set(i, (b & (1 << i)) != 0);
		}
		return bitSet;
	}

	/**
	 * Convert a BitSet to a byte. Cannot use relayType.toByteArray()[0]; since
	 * its only available in 1.7
	 * 
	 * @param bitSet
	 *            The bit set
	 * @return The resulting byte
	 */
	public static byte createByte(final BitSet bitSet) {
		byte b = 0;
		for (int i = 0; i < Utils.BYTE_BITS; i++) {
			if (bitSet.get(i)) {
				b |= 1 << i;
			}
		}
		return b;
	}

	/**
	 * Adds a listener to the response future and releases all aquired channels
	 * in channel creator.
	 * 
	 * @param channelCreator
	 *            The channel creator that will be shutdown and all connections
	 *            will be closed
	 * @param baseFutures
	 *            The futures to listen to. If all the futures finished, then
	 *            the channel creator is shutdown. If null provided, the channel
	 *            creator is shutdown immediately.
	 */
	public static void addReleaseListener(final ChannelCreator channelCreator, final BaseFuture... baseFutures) {
		if (baseFutures == null) {
			channelCreator.shutdown();
			return;
		}
		final int count = baseFutures.length;
		final AtomicInteger finished = new AtomicInteger(0);
		for (BaseFuture baseFuture : baseFutures) {
			baseFuture.addListener(new BaseFutureAdapter<BaseFuture>() {
				@Override
				public void operationComplete(final BaseFuture future) throws Exception {
					if (finished.incrementAndGet() == count) {
						channelCreator.shutdown();
					}
				}
			});
		}
	}

	/**
	 * Compares if two sets have the exact same elements.
	 * 
	 * @param set1
	 *            The first set
	 * @param set2
	 *            The second set
	 * @param <T>
	 *            Type of the collection
	 * @return True if both sets have the exact same elements
	 */
	public static <T> boolean isSameSets(final Collection<T> set1, final Collection<T> set2) {
		if (set1 == null ^ set2 == null) {
			return false;
		}
		if (set1.size() != set2.size()) {
			return false;
		}
		for (T obj : set1) {
			if (!set2.contains(obj)) {
				return false;
			}
		}
		return true;
	}

	public static boolean equals(Object a, Object b) {
		return (a == b) || (a != null && a.equals(b));
	}

	public static String hash(PublicKey publicKey) {
		if (publicKey == null) {
			return "null";
		}
		return String.valueOf(publicKey.hashCode());
	}

	public static Map<Number640, Byte> setMapError(Map<Number640, ?> dataMap, byte reason) {
		Map<Number640, Byte> retVal = new HashMap<Number640, Byte>();
		for (Number640 key : dataMap.keySet()) {
			retVal.put(key, Byte.valueOf(reason));
		}
		return retVal;
	}
}
