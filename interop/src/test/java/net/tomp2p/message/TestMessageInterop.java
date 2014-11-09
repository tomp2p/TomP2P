package net.tomp2p.message;

import net.tomp2p.Utils2;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class TestMessageInterop {

	public void testMessageEncode() throws Exception {

		int intVal = 42;

		Message m1 = Utils2.createDummyMessage();
		m1.intValue(intVal);
		
		Encoder encoder = new Encoder(null); // TODO signaturefactory?

		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer(); // TODO what buffer to use?
		
		encoder.write(buf, m1, null);
		
		//byte[] bytes = buf.array();
		//writeToFile(bytes);
		
	}
	
	public void testMessageDecode() throws Exception {
		
	}
}
