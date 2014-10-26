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

		buf.writeInt(Integer.MIN_VALUE);	//-2147483648
		buf.writeInt(-256);
		buf.writeInt(-255);
		buf.writeInt(-128);
		buf.writeInt(-127);
		buf.writeInt(-1);
		buf.writeInt(0);
		buf.writeInt(1);
		buf.writeInt(127);
		buf.writeInt(128);
		buf.writeInt(255);
		buf.writeInt(256);
		buf.writeInt(Integer.MAX_VALUE);	// 2147483647
		
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
		byte[] fileContent = new byte[13 * 4];
		try {
			fis.read(fileContent);
		}
		finally {
			fis.close();
		}
		
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);
		
		int val1 = buf.readInt();
		int val2 = buf.readInt();
		int val3 = buf.readInt();
		int val4 = buf.readInt();
		int val5 = buf.readInt();
		int val6 = buf.readInt();
		int val7 = buf.readInt();
		int val8 = buf.readInt();
		int val9 = buf.readInt();
		int val10 = buf.readInt();
		int val11 = buf.readInt();
		int val12 = buf.readInt();
		int val13 = buf.readInt();
		
		Assert.assertTrue(val1 == Integer.MIN_VALUE);
		Assert.assertTrue(val2 == -256);
		Assert.assertTrue(val3 == -255);
		Assert.assertTrue(val4 == -128);
		Assert.assertTrue(val5 == -127);
		Assert.assertTrue(val6 == -1);
		Assert.assertTrue(val7 == 0);
		Assert.assertTrue(val8 == 1);
		Assert.assertTrue(val9 == 127);
		Assert.assertTrue(val10 == 128);
		Assert.assertTrue(val11 == 255);
		Assert.assertTrue(val12 == 256);
		Assert.assertTrue(val13 == Integer.MAX_VALUE);
		
	}
	
	@Ignore
	@Test
	public void testEncodeLong() throws Exception {
		
		ByteBuf buf = Unpooled.buffer();
		
		buf.writeLong(Long.MIN_VALUE);	//-923372036854775808
		buf.writeLong(-256);
		buf.writeLong(-255);
		buf.writeLong(-128);
		buf.writeLong(-127);
		buf.writeLong(-1);
		buf.writeLong(0);
		buf.writeLong(1);
		buf.writeLong(127);
		buf.writeLong(128);
		buf.writeLong(255);
		buf.writeLong(256);
		buf.writeLong(Long.MAX_VALUE);	// 923372036854775807
		
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
		byte[] fileContent = new byte[13 * 8];
		try {
			fis.read(fileContent);
		}
		finally {
			fis.close();
		}
		
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);
		
		long val1 = buf.readLong();
		long val2 = buf.readLong();
		long val3 = buf.readLong();
		long val4 = buf.readLong();
		long val5 = buf.readLong();
		long val6 = buf.readLong();
		long val7 = buf.readLong();
		long val8 = buf.readLong();
		long val9 = buf.readLong();
		long val10 = buf.readLong();
		long val11 = buf.readLong();
		long val12 = buf.readLong();
		long val13 = buf.readLong();
		
		Assert.assertTrue(val1 == Long.MIN_VALUE);
		Assert.assertTrue(val2 == (long)-256);
		Assert.assertTrue(val3 == (long)-255);
		Assert.assertTrue(val4 == (long)-128);
		Assert.assertTrue(val5 == (long)-127);
		Assert.assertTrue(val6 == (long)-1);
		Assert.assertTrue(val7 == (long)0);
		Assert.assertTrue(val8 == (long)1);
		Assert.assertTrue(val9 == (long)127);
		Assert.assertTrue(val10 == (long)128);
		Assert.assertTrue(val11 == (long)255);
		Assert.assertTrue(val12 == (long)256);
		Assert.assertTrue(val13 == Long.MAX_VALUE);
	}
	
	@Ignore
	@Test
	public void testEncodeByte() throws Exception {
		
		ByteBuf buf = Unpooled.buffer();
		
		for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) // -128 ... 127
		{
			buf.writeByte(i);			
		}
		
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
	
	@Test
	public void testDecodeByte() throws Exception {
		
		FileInputStream fis = new FileInputStream(from);
		byte[] fileContent = new byte[256];
		try {
			fis.read(fileContent);
		}
		finally {
			fis.close();
		}
		
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);
		
		for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) // -128 ... 127
		{
			byte b = buf.readByte();
			Assert.assertTrue(i == b);
		}
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
