package net.tomp2p;

import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class MessageEncodeDecode {

	public static byte[] encodeMessageEmpty() throws Exception {

		Message m = Utils2.createDummyMessage();
		
		Encoder encoder = new Encoder(null); // TODO signaturefactory?
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		encoder.write(buf, m, null);
		byte[] bytes = buf.nioBuffer().array(); // TODO correct buffer?
		return bytes;
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
		byte[] bytes = buf.nioBuffer().array();
		return bytes;
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
		byte[] bytes = buf.nioBuffer().array();
		return bytes;
	}
}
