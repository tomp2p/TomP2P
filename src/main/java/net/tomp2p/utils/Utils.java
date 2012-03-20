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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionReservation;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.TrackerData;

public class Utils
{
	private static final Random random = new Random();

	public static ByteBuffer loadFile(File file) throws IOException
	{
		FileInputStream fis = null;
		FileChannel channel = null;
		try
		{
			fis = new FileInputStream(file);
			channel = fis.getChannel();
			return channel.map(MapMode.READ_ONLY, 0, channel.size());
		}
		finally
		{
			bestEffortclose(channel, fis);
		}
	}

	public static Number160 makeSHAHash(File file)
	{
		FileInputStream fis = null;
		FileChannel channel = null;
		try
		{
			fis = new FileInputStream(file);
			channel = fis.getChannel();
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			for (long offest = 0; offest < channel.size(); offest += 10 * 1024)
			{
				ByteBuffer buffer;
				if (channel.size() - offest < 10 * 1024)
					buffer = channel.map(MapMode.READ_ONLY, offest, (int) channel.size() - offest);
				else
					buffer = channel.map(MapMode.READ_ONLY, offest, 10 * 1024);
				md.update(buffer);
			}
			byte[] digest = md.digest();
			return new Number160(digest);
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
			return Number160.ZERO;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return Number160.ZERO;
		}
		catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
			return Number160.ZERO;
		}
		finally
		{
			bestEffortclose(channel, fis);
		}
	}

	public static Number160 makeSHAHash(String strInput)
	{
		byte[] buffer = strInput.getBytes();
		return makeSHAHash(buffer);
	}

	public static Number160 makeSHAHash(ByteBuffer buffer)
	{
		try
		{
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(buffer);
			byte[] digest = md.digest();
			return new Number160(digest);
		}
		catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
			return new Number160();
		}
	}

	public static Number160 makeSHAHash(byte[] buffer)
	{
		return makeSHAHash(ByteBuffer.wrap(buffer));
	}

	public static Number160 makeSHAHash(byte[] buffer, int offset, int length)
	{
		return makeSHAHash(ByteBuffer.wrap(buffer, offset, length));
	}

	public static Number160 createRandomNodeID()
	{
		// TODO: this hardcoded, bad style
		byte[] me = new byte[20];
		random.nextBytes(me);
		Number160 id = new Number160(me);
		return id;
	}

	public static byte[] compress(byte[] input)
	{
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
		while (!compressor.finished())
		{
			int count = compressor.deflate(buf);
			bos.write(buf, 0, count);
		}
		try
		{
			bos.close();
		}
		catch (IOException e)
		{
		}
		// Get the compressed data
		return bos.toByteArray();
	}

	public static byte[] uncompress(byte[] compressedData, int offset, int length)
	{
		// Create the decompressor and give it the data to compress
		Inflater decompressor = new Inflater();
		decompressor.setInput(compressedData, offset, length);
		// Create an expandable byte array to hold the decompressed data
		ByteArrayOutputStream bos = new ByteArrayOutputStream(length);
		// Decompress the data
		byte[] buf = new byte[1024];
		while (!decompressor.finished())
		{
			try
			{
				int count = decompressor.inflate(buf);
				bos.write(buf, 0, count);
			}
			catch (DataFormatException e)
			{
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
		try
		{
			bos.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		// Get the decompressed data
		return bos.toByteArray();
	}

	public static byte[] uncompress(byte[] compressedData)
	{
		return uncompress(compressedData, 0, compressedData.length);
	}

	public static byte[] encodeJavaObject(Object attachement) throws IOException
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(attachement);
		// no need to call close of flush since we use ByteArrayOutputStream
		byte[] data = bos.toByteArray();
		return data;
	}

	public static Object decodeJavaObject(byte[] me, int offset, int length) throws ClassNotFoundException, IOException
	{
		ByteArrayInputStream bais = new ByteArrayInputStream(me, offset, length);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Object obj = ois.readObject();
		// no need to call close of flush since we use ByteArrayInputStream
		return obj;
	}
	
	/**
	 * Stores the differences of two collections in a result collection.
	 * The result will contain items from collection1 without those items that
	 * are in collection2.
	 * 
	 * @param collection1 The first collection (master collection) that will be
	 *        iterated and checked against duplicates in collection2.
	 * @param result The collection to store the result
	 * @param collection2 The second collection that will be searched for
	 *        duplicates
	 * @return Returns the collection the user specified as the resulting
	 *         collection
	 */
	public static <K> Collection<K> difference(Collection<K> collection1, Collection<K> result, Collection<K> collection2)
	{
		for (Iterator<K> iterator = collection1.iterator(); iterator.hasNext();)
		{
			K item = iterator.next();
			if (!collection2.contains(item))
			{
				result.add(item);
			}
		}
		return result;
	}

	/**
	 * Stores the differences of multiple collections in a result collection.
	 * The result will contain items from collection1 without those items that
	 * are in collections2. The calling method might need to provide a
	 * @SuppressWarnings("unchecked") since generics and arrays do not mix well.
	 * 
	 * @param collection1 The first collection (master collection) that will be
	 *        iterated and checked against duplicates in collection2.
	 * @param result The collection to store the result
	 * @param collection2 The second collections that will be searched for
	 *        duplicates
	 * @return Returns the collection the user specified as the resulting
	 *         collection
	 */
	public static <K> Collection<K> difference(Collection<K> collection1, Collection<K> result, Collection<K>... collections2)
	{
		for (Iterator<K> iterator = collection1.iterator(); iterator.hasNext();)
		{
			K item = iterator.next();
			int size = collections2.length;
			for(int i=0;i<size;i++)
			{
				if (!collections2[i].contains(item))
				{
					result.add(item);
				}
			}
		}
		return result;
	}

	public static void bestEffortclose(Closeable... closables)
	{
		// best effort close;
		for (Closeable closable : closables)
		{
			if (closable != null)
			{
				try
				{
					closable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
	}

	public static final byte[] intToByteArray(int value)
	{
		return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
	}

	public static final int byteArrayToInt(byte[] b)
	{
		return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8) + (b[3] & 0xFF);
	}

	public static <K> K pollRandom(SortedSet<K> queueToAsk, Random rnd)
	{
		int size = queueToAsk.size();
		if (size == 0)
			return null;
		int index = rnd.nextInt(size);
		List<K> values = new ArrayList<K>(queueToAsk);
		K retVal = values.get(index);
		//now we need to remove this element
		queueToAsk.remove(retVal);
		return retVal;
	}

	public static <K, V> Entry<K, V> pollRandomKey(Map<K, V> queueToAsk, Random rnd)
	{
		int size = queueToAsk.size();
		if (size == 0)
			return null;
		List<K> keys = new ArrayList<K>();
		keys.addAll(queueToAsk.keySet());
		int index = rnd.nextInt(size);
		final K key = keys.get(index);
		final V value = queueToAsk.remove(key);
		return new Entry<K, V>()
		{
			@Override
			public K getKey()
			{
				return key;
			}

			@Override
			public V getValue()
			{
				return value;
			}

			@Override
			public V setValue(V value)
			{
				return null;
			}
		};
	}

	public static <K> Collection<K> subtract(final Collection<K> a, final Collection<K> b)
	{
		ArrayList<K> list = new ArrayList<K>(a);
		for (Iterator<K> it = b.iterator(); it.hasNext();)
		{
			list.remove(it.next());
		}
		return list;
	}

	public static <K, V> Map<K, V> subtract(final Map<K, V> a, final Collection<K> b)
	{
		Map<K, V> map = new HashMap<K, V>(a);
		for (Iterator<K> it = b.iterator(); it.hasNext();)
		{
			map.remove(it.next());
		}
		return map;
	}
	
	public static <K, V> Map<K, V> disjunction(final Map<K, V> a, final Collection<K> b)
	{
		Map<K, V> map = new HashMap<K, V>();
		for (Iterator<Entry<K, V>> it = a.entrySet().iterator(); it.hasNext();)
		{
			Entry<K, V> entry=it.next();
			if(!b.contains(entry.getKey()))
				map.put(entry.getKey(), entry.getValue());
		}
		return map;
	}

	public static <K> Collection<K> limit(final Collection<K> a, final int size)
	{
		ArrayList<K> list = new ArrayList<K>();
		int i = 0;
		for (Iterator<K> it = a.iterator(); it.hasNext() && i < size;)
		{
			list.add(it.next());
		}
		return list;
	}

	public static <K, V> Map<K, V> limit(final Map<K, V> a, final int i)
	{
		Map<K, V> map = new HashMap<K, V>(a);
		int remove = a.size() - i;
		for (Iterator<K> it = a.keySet().iterator(); it.hasNext() && remove >= 0;)
		{
			map.remove(it.next());
			remove--;
		}
		return map;
	}

	public static Collection<Number160> convert(final Collection<TrackerData> a)
	{
		ArrayList<Number160> retVal = new ArrayList<Number160>();
		for (Iterator<TrackerData> it = a.iterator(); it.hasNext();)
		{
			retVal.add(it.next().getPeerAddress().getID());
		}
		return retVal;
	}

	public static String debugArray(byte[] array, int offset, int length)
	{
		String digits = "0123456789abcdef";
	    StringBuilder sb = new StringBuilder(length * 2);
	    for (int i=0;i<length;i++) {
	        int bi = array[offset+i] & 0xff;
	        sb.append(digits.charAt(bi >> 4));
	        sb.append(digits.charAt(bi & 0xf));
	    }
	    return sb.toString();
	}

	public static String debugArray(byte[] array)
	{
		return debugArray(array, 0, array.length);
	}

	public static boolean checkEntryProtection(Map<Number160, Data> dataMap)
	{
		for(Data data:dataMap.values())
		{
			if(data.isProtectedEntry())
			{
				return true;
			}
		}
		return false;
	}
	public static void addReleaseListener(BaseFuture baseFuture, final ConnectionReservation connectionReservation, final ChannelCreator cc, final int nr) 
	{
		baseFuture.addListener(new BaseFutureAdapter<BaseFuture>() 
		{
			@Override
			public void operationComplete(BaseFuture future) throws Exception {
				connectionReservation.release(cc, nr);
			}
		});
	}
	
	public static void addReleaseListenerAll(BaseFuture baseFuture, final ConnectionReservation connectionReservation, final ChannelCreator channelCreator) 
	{
		if(channelCreator == null)
			throw new IllegalArgumentException("channelCreator cannot be null");
		baseFuture.addListener(new BaseFutureAdapter<BaseFuture>() 
		{
			@Override
			public void operationComplete(BaseFuture future) throws Exception 
			{
				connectionReservation.release(channelCreator);
			}
		});
	}

	public static Map<Number160, TrackerData> limitRandom(
			Map<Number160, TrackerData> activePeers, int trackerSize) {
		// TODO Auto-generated method stub
		return activePeers;
	}

	public static <K> K getLast(List<K> list)
	{
		if (!list.isEmpty())
		{
			return list.get(list.size()-1);
		}
		return null;
	}
	
	
	/*
	 * Copyright (C) 2008 The Guava Authors
	 *
	 * Licensed under the Apache License, Version 2.0 (the "License");
	 * you may not use this file except in compliance with the License.
	 * You may obtain a copy of the License at
	 *
	 * http://www.apache.org/licenses/LICENSE-2.0
	 *
	 * Unless required by applicable law or agreed to in writing, software
	 * distributed under the License is distributed on an "AS IS" BASIS,
	 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	 * See the License for the specific language governing permissions and
	 * limitations under the License.
	 */
	/**
     * Returns an Inet4Address having the integer value specified by
	 * the argument.
	 *
	 * @param address {@code int}, the 32bit integer address to be converted
	 * @return {@link Inet4Address} equivalent of the argument
	 */
	public static Inet4Address fromInteger(int address) 
	{
		return getInet4Address(toByteArray(address));
	}
	/**
	 * Returns a big-endian representation of {@code value} in a 4-element byte
	 * array; equivalent to {@code ByteBuffer.allocate(4).putInt(value).array()}.
	 * For example, the input value {@code 0x12131415} would yield the byte array
	 * {@code {0x12, 0x13, 0x14, 0x15}}.
	 *
	 * <p>If you need to convert and concatenate several values (possibly even of
	 * different types), use a shared {@link java.nio.ByteBuffer} instance, or use
	 * {@link com.google.common.io.ByteStreams#newDataOutput()} to get a growable
	 * buffer.
	 */
	private static byte[] toByteArray(int value) 
	{
		return new byte[] {
				(byte) (value >> 24),
				(byte) (value >> 16),
				(byte) (value >> 8),
				(byte) value};
	}
	/**
	 * Returns an {@link Inet4Address}, given a byte array representation
	 * of the IPv4 address.
	 *
	 * @param bytes byte array representing an IPv4 address (should be
	 *              of length 4).
	 * @return {@link Inet4Address} corresponding to the supplied byte
	 *         array.
	 * @throws IllegalArgumentException if a valid {@link Inet4Address}
	 *         can not be created.
	 */
	private static Inet4Address getInet4Address(byte[] bytes) 
	{
		if(bytes.length != 4)
		{
			throw new IllegalArgumentException("Byte array has invalid length for an IPv4 address");
		}

		try 
		{
			InetAddress ipv4 = InetAddress.getByAddress(bytes);
			if (!(ipv4 instanceof Inet4Address)) 
			{
				throw new UnknownHostException(String.format("'%s' is not an IPv4 address.", ipv4.getHostAddress()));
			}
			return (Inet4Address) ipv4;
		} 
		catch (UnknownHostException e) 
		{
			/*
			 * This really shouldn't happen in practice since all our byte
			 * sequences should be valid IP addresses.
			 *
			 * However {@link InetAddress#getByAddress} is documented as
			 * potentially throwing this "if IP address is of illegal length".
			 *
			 * This is mapped to IllegalArgumentException since, presumably,
			 * the argument triggered some bizarre processing bug.
			 */
			throw new IllegalArgumentException(String.format("Host address '%s' is not a valid IPv4 address.", Arrays.toString(bytes)), e);
		}
	}
	
	// as seen here: http://stackoverflow.com/questions/617414/create-a-temporary-directory-in-java
	public static File createTempDir() throws IOException
	{
	    final File temp;
	    temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

	    if(!(temp.delete()))
		{
	    	throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
		}
	    
	    if(!(temp.mkdir()))
		{
	    	throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
		}
	    return (temp);
	}
}