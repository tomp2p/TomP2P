package net.tomp2p;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Assert;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TestClass {

	protected final String from = "C:/Users/Christian/Desktop/interop/bytes-NET-encoded.txt";
	protected final String to = "C:/Users/Christian/Desktop/interop/bytes-JAVA-encoded.txt";
	
	public void testEncodeInt() throws Exception {

		ByteBuf buf = Unpooled.buffer();

		buf.writeInt(Integer.MIN_VALUE); // -2147483648
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
		buf.writeInt(Integer.MAX_VALUE); // 2147483647

		byte[] bytes = buf.array();
		writeToFile(bytes);
	}
	
	public boolean testDecodeInt(String path) throws IOException {

		byte[] fileContent = InteropUtil.readFromFile(path, 13 * 4);
		
		//byte[] fileContent = readFromFile(13 * 4);
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
		
		return false;

	}

	protected void writeToFile(byte[] bytes) throws IOException {

		File file = new File(to);
		FileOutputStream fos = new FileOutputStream(file);
		try {
			fos.write(bytes);
		} finally {
			fos.close();
		}
	}
	
	protected byte[] readFromFile(int fileSize) throws IOException {
		
		FileInputStream fis = new FileInputStream(from);
		byte[] fileContent = new byte[fileSize];
		try {
			fis.read(fileContent);
		}
		finally {
			fis.close();
		}
		
		return fileContent;
	}
}
