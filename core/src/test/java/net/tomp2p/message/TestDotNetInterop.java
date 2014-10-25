package net.tomp2p.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import net.tomp2p.storage.AlternativeCompositeByteBuf;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/***
 * These tests have to be done manually as they exceed the boundary of the Java platform.
 * 
 * @author Christian Lüthold
 *
 */
public class TestDotNetInterop {

	//private final String from = "D:/Desktop/interop/bytes-NET-encoded.txt";
	//private final String to = "D:/Desktop/interop/bytes-JAVA-encoded.txt";
	private final String from = "C:/Users/Christian/Desktop/interop/bytes-NET-encoded.txt";
	private final String to = "C:/Users/Christian/Desktop/interop/bytes-JAVA-encoded.txt";
	
	@Ignore
	@Test
	public void testEncodeInt()throws Exception {

		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		buf.writeInt(Integer.MIN_VALUE); //-2147483648
		buf.writeInt(0);
		buf.writeInt(Integer.MAX_VALUE); // 2147483647

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
	public void testDecodeInt() throws Exception {
		
		FileInputStream fis = new FileInputStream(from);
		byte[] fileContent = new byte[from.length()];
		try {
			fis.read(fileContent);
		}
		finally {
			fis.close();
		}
		
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);
		
		int minVal = buf.readInt();
		int zero = buf.readInt();
		int maxVal = buf.readInt();
		
		Assert.assertTrue(minVal == Integer.MIN_VALUE);
		Assert.assertTrue(zero == 0);
		Assert.assertTrue(maxVal == Integer.MAX_VALUE);
		
	}
	
	@Ignore
	@Test
	public void testEncodeLong() throws Exception {
		
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		
		buf.writeLong(Long.MIN_VALUE); //-923372036854775808
		buf.writeLong(0);
		buf.writeLong(Long.MAX_VALUE); // 923372036854775807

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
	public void testDecodeLong() throws Exception {
		
		FileInputStream fis = new FileInputStream(from);
		byte[] fileContent = new byte[from.length()];
		try {
			fis.read(fileContent);
		}
		finally {
			fis.close();
		}
		
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);
		
		long minVal = buf.readLong();
		long zero = buf.readLong();
		long maxVal = buf.readLong();
		
		Assert.assertTrue(minVal == Long.MIN_VALUE);
		Assert.assertTrue(zero == 0);
		Assert.assertTrue(maxVal == Long.MAX_VALUE);
	}
	
	@Ignore
	@Test
	public void testEncodeByte() {
		
	}
	
	@Ignore
	@Test
	public void testDecodeByte() {
		
	}
	
	@Ignore
	@Test
	public void testEncodeBytes() {
		
	}
	
	@Ignore
	@Test
	public void testDecodeBytes() {
		
	}
}
