package net.tomp2p;

import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class MessageEncodeDecode {

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
		
		Encoder encoder = new Encoder(null); // TODO signaturefactory?

		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		
		encoder.write(buf, m, null);
		
		byte[] bytes = buf.nioBuffer().array(); // TODO correct buffer?
		return bytes;
	}
}
