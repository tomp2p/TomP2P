package net.tomp2p;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message;
import net.tomp2p.message.TestMessage;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

import org.junit.Test;

public class DataTest {

	@Test
	public void encodeDecodeTest() throws Exception {

		// create sample data maps
		Message m1 = MessageEncodeDecode.createMessageMapKey640Data();
		
		Message m2 = TestMessage.encodeDecode(m1);

		assertTrue(MessageEncodeDecode.checkIsSameList(m1.dataMapList(), m2.dataMapList()));
	}
	
	@Test
	public void encodeSampleData() throws Exception {
		
		byte[] sampleBytes2 = new byte[] { 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
		Data data = new Data(sampleBytes2);
		
		Map<Number640, Data> sampleMap1 = new HashMap<Number640, Data>();
		sampleMap1.put(Number640.ZERO, data);
		
		Message m = Utils2.createDummyMessage();
		m.setDataMap(new DataMap(sampleMap1));
		
		// encode
		TestMessage.encodeDecode(m);
	}
}
