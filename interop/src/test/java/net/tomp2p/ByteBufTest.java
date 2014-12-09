package net.tomp2p;

import static org.junit.Assert.assertTrue;
import net.tomp2p.message.Message;
import net.tomp2p.message.TestMessage;

import org.junit.Test;

public class ByteBufTest {

	@Test
	public void encodeDecodeTest() throws Exception {

		// create sample data maps
		Message m1 = MessageEncodeDecode.createMessageByteBuffer();
		
		Message m2 = TestMessage.encodeDecode(m1);

		assertTrue(MessageEncodeDecode.checkIsSameList(m1.bufferList(), m2.bufferList()));
	}
	
	@Test
	public void tmpTest() throws Exception {
		
		Message m = MessageEncodeDecode.createMessageByteBuffer();
		
		// encode
		TestMessage.encodeDecode(m);
	}
}
