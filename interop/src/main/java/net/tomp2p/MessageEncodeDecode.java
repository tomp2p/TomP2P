package net.tomp2p;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import net.tomp2p.message.Buffer;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.KeyCollection;
import net.tomp2p.message.KeyMap640Keys;
import net.tomp2p.message.KeyMapByte;
import net.tomp2p.message.Message;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.AlternativeCompositeByteBuf;
import net.tomp2p.storage.Data;

public class MessageEncodeDecode {

	// 20 bytes (Number160 length)
	static byte[] sampleBytes1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
	static byte[] sampleBytes2 = new byte[] {19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
	static byte[] sampleBytes3 = new byte[Number160.BYTE_ARRAY_SIZE];
	
	static Number160 sample160_1 = Number160.ZERO;
	static Number160 sample160_2 = Number160.ONE;
	static Number160 sample160_3 = Number160.MAX_VALUE;
	static Number160 sample160_4 = new Number160(sampleBytes1);
	static Number160 sample160_5 = new Number160(sampleBytes2);
	
	static Number640 sample640_1 = Number640.ZERO;
	static Number640 sample640_2 = new Number640(new Number160(sampleBytes1), new Number160(sampleBytes2), new Number160(sampleBytes3), Number160.MAX_VALUE);
	static Number640 sample640_3 = new Number640(Number160.MAX_VALUE, new Number160(sampleBytes3), new Number160(sampleBytes2), new Number160(sampleBytes1));
	
	static Data sampleData1 = new Data(sampleBytes1);
	static Data sampleData2 = new Data(sampleBytes2);
	static Data sampleData3 = new Data(sampleBytes3);
	
	public static byte[] encodeMessageEmpty() throws Exception {

		Message m = Utils2.createDummyMessage();
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessageKey() throws Exception {
		
		Message m = Utils2.createDummyMessage();
		m.key(sample160_1);
		m.key(sample160_2);
		m.key(sample160_3);
		m.key(sample160_4);
		m.key(sample160_5);
		m.key(sample160_1);
		m.key(sample160_2);
		m.key(sample160_3);
		
		return encodeMessage(m);
	}

	public static byte[] encodeMessageMapKey640Data() throws Exception {
		
		// create sample data maps
				
		Map<Number640, Data> sampleMap1 = new HashMap<Number640, Data>();
		sampleMap1.put(sample640_1, sampleData1);
		sampleMap1.put(sample640_2, sampleData1);
		sampleMap1.put(sample640_3, sampleData1);
		
		Map<Number640, Data> sampleMap2 = new HashMap<Number640, Data>();
		sampleMap2.put(sample640_1, sampleData2);
		sampleMap2.put(sample640_2, sampleData2);
		sampleMap2.put(sample640_3, sampleData2);
		
		Map<Number640, Data> sampleMap3 = new HashMap<Number640, Data>();
		sampleMap3.put(sample640_1, sampleData3);
		sampleMap3.put(sample640_2, sampleData3);
		sampleMap3.put(sample640_3, sampleData3);
		
		Map<Number640, Data> sampleMap4 = new HashMap<Number640, Data>();
		sampleMap4.put(sample640_1, sampleData1);
		sampleMap4.put(sample640_2, sampleData2);
		sampleMap4.put(sample640_3, sampleData3);
		
		Map<Number640, Data> sampleMap5 = new HashMap<Number640, Data>();
		sampleMap5.put(sample640_3, sampleData1);
		sampleMap5.put(sample640_2, sampleData2);
		sampleMap5.put(sample640_1, sampleData3);
		
		// prepare message
		Message m = Utils2.createDummyMessage();
		m.setDataMap(new DataMap(sampleMap1));
		m.setDataMap(new DataMap(sampleMap2));
		m.setDataMap(new DataMap(sampleMap3));
		m.setDataMap(new DataMap(sampleMap4));
		m.setDataMap(new DataMap(sampleMap5));
		m.setDataMap(new DataMap(sampleMap1));
		m.setDataMap(new DataMap(sampleMap2));
		m.setDataMap(new DataMap(sampleMap3));
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessageMapKey640Keys() throws Exception {
		
		// TODO redo for multiple key maps
		
		// create a sample keyMap
		NavigableMap<Number640, Collection<Number160>> keysMap = new TreeMap<Number640, Collection<Number160>>();
		Set<Number160> set = new HashSet<Number160>(1);
		set.add(sample160_1);
		keysMap.put(sample640_1, set);
		
		set = new HashSet<Number160>(2);
		set.add(sample160_2);
		set.add(sample160_3);
		keysMap.put(sample640_2, set);

		set = new HashSet<Number160>(3);
		set.add(sample160_1);
		set.add(sample160_2);
		set.add(sample160_3);
		set.add(sample160_4);
		set.add(sample160_5);
		keysMap.put(sample640_3, set);

		// prepare message
		Message m = Utils2.createDummyMessage();
		m.keyMap640Keys(new KeyMap640Keys(keysMap));
		m.keyMap640Keys(new KeyMap640Keys(keysMap));
		m.keyMap640Keys(new KeyMap640Keys(keysMap));
		m.keyMap640Keys(new KeyMap640Keys(keysMap));
		m.keyMap640Keys(new KeyMap640Keys(keysMap));
		m.keyMap640Keys(new KeyMap640Keys(keysMap));
		m.keyMap640Keys(new KeyMap640Keys(keysMap));
		m.keyMap640Keys(new KeyMap640Keys(keysMap));
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessageSetKey640() throws Exception {
		
		// create sample key collections
		Collection<Number160> sampleCollection1 = new ArrayList<Number160>();
		sampleCollection1.add(sample160_1);
		sampleCollection1.add(sample160_2);
		sampleCollection1.add(sample160_3);
		
		Collection<Number160> sampleCollection2 = new ArrayList<Number160>();
		sampleCollection2.add(sample160_2);
		sampleCollection2.add(sample160_3);
		sampleCollection2.add(sample160_4);
		
		Collection<Number160> sampleCollection3 = new ArrayList<Number160>();
		sampleCollection3.add(sample160_3);
		sampleCollection3.add(sample160_4);
		sampleCollection3.add(sample160_5);
		
		Message m = Utils2.createDummyMessage();
		m.keyCollection(new KeyCollection(sample160_1, sample160_1, sample160_1, sampleCollection1));
		m.keyCollection(new KeyCollection(sample160_2, sample160_2, sample160_2, sampleCollection2));
		m.keyCollection(new KeyCollection(sample160_3, sample160_3, sample160_3, sampleCollection3));
		m.keyCollection(new KeyCollection(sample160_4, sample160_4, sample160_4, sampleCollection1));
		m.keyCollection(new KeyCollection(sample160_5, sample160_5, sample160_5, sampleCollection2));
		m.keyCollection(new KeyCollection(sample160_1, sample160_2, sample160_3, sampleCollection3));
		m.keyCollection(new KeyCollection(sample160_2, sample160_3, sample160_4, sampleCollection1));
		m.keyCollection(new KeyCollection(sample160_3, sample160_4, sample160_5, sampleCollection2));
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessageSetNeighbors() throws Exception {

		// create sample neighbor sets
		PeerAddress sampleAddress1 = new PeerAddress(sample160_1, InetAddress.getByName("192.168.1.1"));
		PeerAddress sampleAddress2 = new PeerAddress(sample160_2, InetAddress.getByName("255.255.255.255"));
		PeerAddress sampleAddress3 = new PeerAddress(sample160_3, InetAddress.getByName("127.0.0.1"));
		PeerAddress sampleAddress4 = new PeerAddress(sample160_4, InetAddress.getByName("0:1:2:3:4:5:6:7"));
		PeerAddress sampleAddress5 = new PeerAddress(sample160_5, InetAddress.getByName("7:6:5:4:3:2:1:0"));
		
		Collection<PeerAddress> sampleNeighbours1 = new ArrayList<PeerAddress>();
		sampleNeighbours1.add(sampleAddress1);
		sampleNeighbours1.add(sampleAddress2);
		sampleNeighbours1.add(sampleAddress3);
		
		Collection<PeerAddress> sampleNeighbours2 = new ArrayList<PeerAddress>();
		sampleNeighbours2.add(sampleAddress2);
		sampleNeighbours2.add(sampleAddress3);
		sampleNeighbours2.add(sampleAddress4);
		
		Collection<PeerAddress> sampleNeighbours3 = new ArrayList<PeerAddress>();
		sampleNeighbours3.add(sampleAddress3);
		sampleNeighbours3.add(sampleAddress4);
		sampleNeighbours3.add(sampleAddress5);
		
		Message m = Utils2.createDummyMessage();
		m.neighborsSet(new NeighborSet(-1, sampleNeighbours1));
		m.neighborsSet(new NeighborSet(-1, sampleNeighbours2));
		m.neighborsSet(new NeighborSet(-1, sampleNeighbours3));
		m.neighborsSet(new NeighborSet(-1, sampleNeighbours1));
		m.neighborsSet(new NeighborSet(-1, sampleNeighbours2));
		m.neighborsSet(new NeighborSet(-1, sampleNeighbours3));
		m.neighborsSet(new NeighborSet(-1, sampleNeighbours1));
		m.neighborsSet(new NeighborSet(-1, sampleNeighbours2));
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessageByteBuffer() throws Exception {
		
		// TODO figure out how messages work exactly
		
		// create sample buffers
		ByteBuf sampleBuf1 = Unpooled.buffer();
		sampleBuf1.writeBytes(sampleBytes1);
		sampleBuf1.writeBytes(sampleBytes1);
		sampleBuf1.writeBytes(sampleBytes1);
		
		/*ByteBuf sampleBuf2 = Unpooled.buffer();
		sampleBuf2.writeBytes(sampleBytes2);
		sampleBuf2.writeBytes(sampleBytes2);
		sampleBuf2.writeBytes(sampleBytes2);
		
		ByteBuf sampleBuf3 = Unpooled.buffer();
		sampleBuf3.writeBytes(sampleBytes3);
		sampleBuf3.writeBytes(sampleBytes3);
		sampleBuf3.writeBytes(sampleBytes3);
		
		ByteBuf sampleBuf4 = Unpooled.buffer();
		sampleBuf4.writeBytes(sampleBytes1);
		sampleBuf4.writeBytes(sampleBytes2);
		sampleBuf4.writeBytes(sampleBytes3);*/
		
		Message m = Utils2.createDummyMessage();
		m.buffer(new Buffer(sampleBuf1));
		/*m.buffer(new Buffer(sampleBuf2));
		m.buffer(new Buffer(sampleBuf3));
		m.buffer(new Buffer(sampleBuf4));
		m.buffer(new Buffer(sampleBuf1));
		m.buffer(new Buffer(sampleBuf2));
		m.buffer(new Buffer(sampleBuf3));
		m.buffer(new Buffer(sampleBuf4));*/
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessageInt() throws Exception {

		Message m = Utils2.createDummyMessage();
		m.intValue(Integer.MIN_VALUE);
		m.intValue(-256);
		m.intValue(-128);
		m.intValue(-1);
		m.intValue(0);
		m.intValue(1);
		m.intValue(128);
		m.intValue(Integer.MAX_VALUE);
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessageLong() throws Exception {

		Message m = Utils2.createDummyMessage();
		m.longValue(Long.MIN_VALUE);
		m.longValue(-256);
		m.longValue(-128);
		m.longValue(-1);
		m.longValue(0);
		m.longValue(1);
		m.longValue(128);
		m.longValue(Long.MAX_VALUE);
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessagePublicKeySignature() throws Exception {
		
		// TODO implement
		Message m = Utils2.createDummyMessage();
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessagePublicKey() throws Exception
	{
		// TODO implement
		Message m = Utils2.createDummyMessage();
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessageSetTrackerData() throws Exception {
		
		// create sample tracker data
		PeerAddress sampleAddress1 = new PeerAddress(sample160_1, InetAddress.getByName("192.168.1.1"));
		PeerAddress sampleAddress2 = new PeerAddress(sample160_2, InetAddress.getByName("255.255.255.255"));
		PeerAddress sampleAddress3 = new PeerAddress(sample160_3, InetAddress.getByName("127.0.0.1"));
		PeerAddress sampleAddress4 = new PeerAddress(sample160_4, InetAddress.getByName("0:1:2:3:4:5:6:7"));
		PeerAddress sampleAddress5 = new PeerAddress(sample160_5, InetAddress.getByName("7:6:5:4:3:2:1:0"));
		
		PeerStatistic sampleStatistic1 = new PeerStatistic(sampleAddress1);
		PeerStatistic sampleStatistic2 = new PeerStatistic(sampleAddress2);
		PeerStatistic sampleStatistic3 = new PeerStatistic(sampleAddress3);
		PeerStatistic sampleStatistic4 = new PeerStatistic(sampleAddress4);
		PeerStatistic sampleStatistic5 = new PeerStatistic(sampleAddress5);
		
		Map<PeerStatistic, Data> sampleMap1 = new HashMap<PeerStatistic, Data>();
		sampleMap1.put(sampleStatistic1, sampleData1);
		sampleMap1.put(sampleStatistic2, sampleData2);
		sampleMap1.put(sampleStatistic3, sampleData3);
		
		Map<PeerStatistic, Data> sampleMap2 = new HashMap<PeerStatistic, Data>();
		sampleMap2.put(sampleStatistic2, sampleData1);
		sampleMap2.put(sampleStatistic3, sampleData2);
		sampleMap2.put(sampleStatistic4, sampleData3);
		
		Map<PeerStatistic, Data> sampleMap3 = new HashMap<PeerStatistic, Data>();
		sampleMap3.put(sampleStatistic3, sampleData1);
		sampleMap3.put(sampleStatistic4, sampleData2);
		sampleMap3.put(sampleStatistic5, sampleData3);
		
		Message m = Utils2.createDummyMessage();
		m.trackerData(new TrackerData(sampleMap1, true));
		m.trackerData(new TrackerData(sampleMap1, false));
		m.trackerData(new TrackerData(sampleMap2, true));
		m.trackerData(new TrackerData(sampleMap2, false));
		m.trackerData(new TrackerData(sampleMap3, true));
		m.trackerData(new TrackerData(sampleMap3, false));
		m.trackerData(new TrackerData(sampleMap1, true));
		m.trackerData(new TrackerData(sampleMap1, false));
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessageBloomFilter() throws Exception {
		
		// create sample bloom filters		
		SimpleBloomFilter<Number160> sampleBf1 = new SimpleBloomFilter<Number160>(2, 5);
		sampleBf1.add(sample160_1);
		
		SimpleBloomFilter<Number160> sampleBf2 = new SimpleBloomFilter<Number160>(2, 5);
		sampleBf2.add(sample160_2);
		sampleBf2.add(sample160_1);
		
		SimpleBloomFilter<Number160> sampleBf3 = new SimpleBloomFilter<Number160>(2, 5);
		sampleBf3.add(sample160_1);
		sampleBf3.add(sample160_2);
		sampleBf3.add(sample160_3);
		
		SimpleBloomFilter<Number160> sampleBf4 = new SimpleBloomFilter<Number160>(2, 5);
		sampleBf4.add(sample160_1);
		sampleBf4.add(sample160_2);
		sampleBf4.add(sample160_3);
		sampleBf4.add(sample160_4);
		
		SimpleBloomFilter<Number160> sampleBf5 = new SimpleBloomFilter<Number160>(2, 5);
		sampleBf5.add(sample160_1);
		sampleBf5.add(sample160_2);
		sampleBf5.add(sample160_3);
		sampleBf5.add(sample160_4);
		sampleBf5.add(sample160_5);
		
		Message m = Utils2.createDummyMessage();
		m.bloomFilter(sampleBf1);
		m.bloomFilter(sampleBf2);
		m.bloomFilter(sampleBf3);
		m.bloomFilter(sampleBf4);
		m.bloomFilter(sampleBf5);
		m.bloomFilter(sampleBf1);
		m.bloomFilter(sampleBf2);
		m.bloomFilter(sampleBf3);
		
		return encodeMessage(m);
	}
	
	public static byte[] encodeMessageMapKey640Byte() throws Exception {
		
		// create sample keymapbytes
		Map<Number640, Byte> sampleMap1 = new HashMap<Number640, Byte>();
		sampleMap1.put(sample640_1, sampleBytes1[0]);
		sampleMap1.put(sample640_2, sampleBytes1[1]);
		sampleMap1.put(sample640_3, sampleBytes1[2]);
		
		Map<Number640, Byte> sampleMap2 = new HashMap<Number640, Byte>();
		sampleMap2.put(sample640_1, sampleBytes1[3]);
		sampleMap2.put(sample640_2, sampleBytes1[4]);
		sampleMap2.put(sample640_3, sampleBytes1[5]);
		
		Map<Number640, Byte> sampleMap3= new HashMap<Number640, Byte>();
		sampleMap3.put(sample640_1, sampleBytes1[6]);
		sampleMap3.put(sample640_2, sampleBytes1[7]);
		sampleMap3.put(sample640_3, sampleBytes1[8]);
		
		Map<Number640, Byte> sampleMap4 = new HashMap<Number640, Byte>();
		sampleMap4.put(sample640_1, sampleBytes1[9]);
		sampleMap4.put(sample640_2, sampleBytes1[10]);
		sampleMap4.put(sample640_3, sampleBytes1[11]);
		
		Map<Number640, Byte> sampleMap5 = new HashMap<Number640, Byte>();
		sampleMap5.put(sample640_1, sampleBytes1[12]);
		sampleMap5.put(sample640_2, sampleBytes1[13]);
		sampleMap5.put(sample640_3, sampleBytes1[14]);
		
		Message m = Utils2.createDummyMessage();
		m.keyMapByte(new KeyMapByte(sampleMap1));
		m.keyMapByte(new KeyMapByte(sampleMap2));
		m.keyMapByte(new KeyMapByte(sampleMap3));
		m.keyMapByte(new KeyMapByte(sampleMap4));
		m.keyMapByte(new KeyMapByte(sampleMap5));
		m.keyMapByte(new KeyMapByte(sampleMap1));
		m.keyMapByte(new KeyMapByte(sampleMap2));
		m.keyMapByte(new KeyMapByte(sampleMap3));
		
		return encodeMessage(m);
	}

	public static byte[] encodeMessageSetPeerSocket() throws Exception {
		
		// create sample peersocketaddresses
		InetAddress sampleAddress1 = InetAddress.getByName("192.168.1.1");
		InetAddress sampleAddress2 = InetAddress.getByName("255.255.255.255");
		InetAddress sampleAddress3 = InetAddress.getByName("127.0.0.1");
		InetAddress sampleAddress4 = InetAddress.getByName("0:1:2:3:4:5:6:7");
		InetAddress sampleAddress5 = InetAddress.getByName("7:6:5:4:3:2:1:0");
		
		
		PeerSocketAddress samplePsa1 = new PeerSocketAddress(sampleAddress1, Short.MIN_VALUE, Short.MIN_VALUE);
		PeerSocketAddress samplePsa2 = new PeerSocketAddress(sampleAddress2, 65536, 65536);
		PeerSocketAddress samplePsa3 = new PeerSocketAddress(sampleAddress3, 1, 1);
		PeerSocketAddress samplePsa4 = new PeerSocketAddress(sampleAddress4, 2, 2);
		PeerSocketAddress samplePsa5 = new PeerSocketAddress(sampleAddress5, 30, 40);
		PeerSocketAddress samplePsa6 = new PeerSocketAddress(sampleAddress1, 88, 88);
		PeerSocketAddress samplePsa7 = new PeerSocketAddress(sampleAddress2, 177, 177);
		PeerSocketAddress samplePsa8 = new PeerSocketAddress(sampleAddress3, 60000, 65000);
		PeerSocketAddress samplePsa9 = new PeerSocketAddress(sampleAddress4, 99, 100);
		PeerSocketAddress samplePsa10 = new PeerSocketAddress(sampleAddress5, 13, 1234);

		Collection<PeerSocketAddress> sampleAddresses1 = new ArrayList<PeerSocketAddress>();
		sampleAddresses1.add(samplePsa1);
		sampleAddresses1.add(samplePsa2);
		sampleAddresses1.add(samplePsa3);
		
		Collection<PeerSocketAddress> sampleAddresses2 = new ArrayList<PeerSocketAddress>();
		sampleAddresses2.add(samplePsa3);
		sampleAddresses2.add(samplePsa4);
		sampleAddresses2.add(samplePsa5);
		
		Collection<PeerSocketAddress> sampleAddresses3 = new ArrayList<PeerSocketAddress>();
		sampleAddresses3.add(samplePsa6);
		sampleAddresses3.add(samplePsa7);
		sampleAddresses3.add(samplePsa8);
		
		Collection<PeerSocketAddress> sampleAddresses4 = new ArrayList<PeerSocketAddress>();
		sampleAddresses4.add(samplePsa9);
		sampleAddresses4.add(samplePsa10);
		sampleAddresses4.add(samplePsa1);
		
		Message m = Utils2.createDummyMessage();
		m.peerSocketAddresses(sampleAddresses1);
		m.peerSocketAddresses(sampleAddresses2);
		m.peerSocketAddresses(sampleAddresses3);
		m.peerSocketAddresses(sampleAddresses4);
		m.peerSocketAddresses(sampleAddresses1);
		m.peerSocketAddresses(sampleAddresses2);
		m.peerSocketAddresses(sampleAddresses3);
		m.peerSocketAddresses(sampleAddresses4);
		
		return encodeMessage(m);
	}
	
	private static byte[] encodeMessage(Message message) throws Exception {
		
		Encoder encoder = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		encoder.write(buf, message, null);
		
		return extractBytes(buf);
	}
	
	/**
	 * Gets the byte array from the provided {@link AlternativeCompositeByteBuf}.
	 * @param buf
	 * @return
	 */
	private static byte[] extractBytes(AlternativeCompositeByteBuf buf) {
		/*ByteBuffer buf2 = ByteBuffer.allocate(buf.nioBuffer().remaining());
		byte[] bytes = new byte[buf.nioBuffer().remaining()];
		buf2.get(bytes);
		return bytes;*/
		
		ByteBuffer buffer = buf.nioBuffer();
		buffer.position(0);
		
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return bytes;
	}
}
