package net.tomp2p.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import net.tomp2p.storage.AlternativeCompositeByteBuf;

import org.junit.Ignore;
import org.junit.Test;

/***
 * These tests have to be done manually as they exceed the boundary of the Java platform.
 * 
 * @author Christian Lüthold
 *
 */
public class TestDotNetInterop {

	private final String from = "D:/Desktop/interop/bytes-NET-encoded.txt";
	private final String to = "D:/Desktop/interop/bytes-JAVA-encoded.txt";
	
	@Ignore
	@Test
	public void testEncode()throws Exception {
		
		//Message m1 = Utils2.createDummyMessage();
		//final int integer = 42;
		//m1.intValue(integer);
		
		//Encoder encoder = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		
		//encoder.write(buf, m1, null);
		
		final int value = Integer.MAX_VALUE; // 2147483647
		buf.writeInt(value);

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
		byte[] fileContent = new byte[from.length()];
		try {
			fis.read(fileContent);
		}
		finally {
			fis.close();
		}
		
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);
		int value = buf.readInt();
	}
}
