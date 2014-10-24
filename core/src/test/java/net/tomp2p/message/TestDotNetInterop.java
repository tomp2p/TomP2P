package net.tomp2p.message;

import java.io.File;
import java.io.FileOutputStream;

import org.junit.Test;

import net.tomp2p.Utils2;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class TestDotNetInterop {

	
	@Test
	public void testEncode()throws Exception {
		
		Message m1 = Utils2.createDummyMessage();
		int integer = 42;
		m1.intValue(integer);
		
		Encoder encoder = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		
		encoder.write(buf, m1, null);
		
		byte[] bytes = buf.array();
		
		File file = new File("tomp2ptest.txt");
		FileOutputStream fos = new FileOutputStream(file);
		try {
			fos.write(bytes);
		}
		finally {
			fos.close();
		}
		
		
		
	}
}
