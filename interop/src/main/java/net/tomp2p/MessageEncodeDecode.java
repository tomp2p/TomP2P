package net.tomp2p;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import net.tomp2p.message.Encoder;
import net.tomp2p.message.KeyMap640Keys;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class MessageEncodeDecode {

	public static byte[] encodeMessageEmpty() throws Exception {

		Message m = Utils2.createDummyMessage();
		
		Encoder encoder = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		encoder.write(buf, m, null);
		
		return extractBytes(buf);
	}
	
	public static byte[] encodeMessageKey() throws Exception {
		
		Message m = Utils2.createDummyMessage();
		m.key(Number160.ZERO);
		m.key(Number160.ONE);
		m.key(Number160.MAX_VALUE);
		m.key(Number160.ZERO);
		m.key(Number160.ONE);
		m.key(Number160.MAX_VALUE);
		m.key(Number160.ZERO);
		m.key(Number160.ONE);
		
		Encoder encoder = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		encoder.write(buf, m, null);
		
		return extractBytes(buf);
	}
	
	public static byte[] encodeMessageMapKey640Keys() throws Exception {
		
		// create a sample KeyMap640Keys object
		byte[] sampleBytes1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
		byte[] sampleBytes2 = new byte[] {19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
		byte[] sampleBytes3 = new byte[Number160.BYTE_ARRAY_SIZE];

		NavigableMap<Number640, Collection<Number160>> keysMap = new TreeMap<Number640, Collection<Number160>>();
		Set<Number160> set = new HashSet<Number160>(1);
		set.add(Number160.MAX_VALUE);
		keysMap.put(Number640.ZERO, set);
		
		set = new HashSet<Number160>(2);
		set.add(Number160.ZERO);
		set.add(Number160.ONE);
		keysMap.put(new Number640(new Number160(sampleBytes1), new Number160(sampleBytes2), new Number160(sampleBytes3), Number160.MAX_VALUE), set);

		set = new HashSet<Number160>(3);
		set.add(new Number160(sampleBytes1));
		set.add(new Number160(sampleBytes2));
		set.add(new Number160(sampleBytes3));
		keysMap.put(new Number640(Number160.MAX_VALUE, new Number160(sampleBytes1), new Number160(sampleBytes2), new Number160(sampleBytes3)), set);

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
		
		Encoder encoder = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		encoder.write(buf, m, null);
		
		return extractBytes(buf);
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
		
		Encoder encoder = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		encoder.write(buf, m, null);
		
		return extractBytes(buf);
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
		
		Encoder encoder = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		encoder.write(buf, m, null);
		
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
