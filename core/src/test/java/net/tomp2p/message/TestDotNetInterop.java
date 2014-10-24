package net.tomp2p.message;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.junit.Ignore;
import org.junit.Test;

import net.tomp2p.Utils2;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class TestDotNetInterop {

	private final String from = "D:/Desktop/interop/bytes-NET-encoded.txt";
	private final String to = "D:/Desktop/interop/bytes-JAVA-encoded.txt";
	
	@Test
	public void testEncode()throws Exception {
		
		//Message m1 = Utils2.createDummyMessage();
		final int integer = 42;
		//m1.intValue(integer);
		
		//Encoder encoder = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		
		//encoder.write(buf, m1, null);
		
		int value = 128; // 2147483647
		buf.writeInt(value);
		
		//byte[] bytes = new byte[] { -128, 0, 127 };
		byte[] bytes = buf.array();
		
		File file = new File(to);
		FileOutputStream fos = new FileOutputStream(file);
		try {
			fos.write(bytes);
		}
		finally {
			fos.close();
		}
	}
	
	@Ignore
	@Test
	public void testDecode() throws Exception {
		
		FileInputStream fis = new FileInputStream(from);
		
		try {
			byte[] fileContent = new byte[from.length()];
			fis.read(fileContent);
			System.out.println(fileContent.toString());
		}
		finally {
			fis.close();
		}
	}
}
