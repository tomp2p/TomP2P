package net.tomp2p;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import net.tomp2p.message.DataMap;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.KeyCollection;
import net.tomp2p.message.KeyMap640Keys;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
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
		Data sampleData1 = new Data(sampleBytes1);
		Data sampleData2 = new Data(sampleBytes2);
		Data sampleData3 = new Data(sampleBytes3);
				
		Map<Number640, Data> sampleMap1 = new HashMap<Number640, Data>();
		sampleMap1.put(sample640_1, sampleData1);
		sampleMap1.put(sample640_1, sampleData2);
		sampleMap1.put(sample640_1, sampleData3);
		
		Map<Number640, Data> sampleMap2 = new HashMap<Number640, Data>();
		sampleMap2.put(sample640_2, sampleData1);
		sampleMap2.put(sample640_2, sampleData2);
		sampleMap2.put(sample640_2, sampleData3);
		
		Map<Number640, Data> sampleMap3 = new HashMap<Number640, Data>();
		sampleMap3.put(sample640_3, sampleData1);
		sampleMap3.put(sample640_3, sampleData2);
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
		
		// create a sample key collections
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
