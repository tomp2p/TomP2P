package net.tomp2p.message;

import net.tomp2p.TestInteropBase;
import net.tomp2p.Utils2;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

import org.junit.Ignore;
import org.junit.Test;

public class TestMessageInterop extends TestInteropBase {

	@Test
	public void testMessageEncode() throws Exception {

		int intVal = 42;

		Message m1 = Utils2.createDummyMessage();
		m1.intValue(intVal);
		
		Encoder encoder = new Encoder(null); // TODO signaturefactory?

		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer(); // TODO what buffer to use?
		
		encoder.write(buf, m1, null);
		
		byte[] bytes = buf.array();
		writeToFile(bytes);
		
	}
	
	@Ignore
	@Test
	public void testMessageDecode() throws Exception {
		
	}
}
