package net.tomp2p;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

/***
 * These tests check the binary encoding/decoding of data types between Java and .NET.
 * 
 * @author Christian Lüthold
 *
 */
public class DotNetEncodeDecodeTest {

	public static void testEncodeInt() throws Exception {

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

		//byte[] bytes = buf.array();
		// InteropUtil.writeToFile(bytes);
	}

	public static void testEncodeLong() throws IOException {
	
		ByteBuf buf = Unpooled.buffer();
	
		buf.writeLong(Long.MIN_VALUE); // -923372036854775808
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
		buf.writeLong(Long.MAX_VALUE); // 923372036854775807
	
		//byte[] bytes = buf.array();
		// writeToFile(bytes);
	}

	public static void testEncodeByte() throws Exception {
	
		ByteBuf buf = Unpooled.buffer();
	
		for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) // -128 ... 127
		{
			buf.writeByte(i);
		}
	
		//byte[] bytes = buf.array();
		// writeToFile(bytes);
	}

	public static void testEncodeBytes() throws Exception {
	
		ByteBuf buf = Unpooled.buffer();
	
		byte[] byteArray = new byte[256];
		for (int i = 0, b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; i++, b++) {
			byteArray[i] = (byte) b;
		}
	
		buf.writeBytes(byteArray);
	
		//byte[] bytes = buf.array();
		//writeToFile(bytes);
	}

	public static boolean testDecodeInt(String argument) throws IOException {

		byte[] fileContent = InteropUtil.readFromFile(argument, 13 * 4);
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

		boolean t1 = val1 == Integer.MIN_VALUE;
		boolean t2 = val2 == -256;
		boolean t3 = val3 == -255;
		boolean t4 = val4 == -128;
		boolean t5 = val5 == -127;
		boolean t6 = val6 == -1;
		boolean t7 = val7 == 0;
		boolean t8 = val8 == 1;
		boolean t9 = val9 == 127;
		boolean t10 = val10 == 128;
		boolean t11 = val11 == 255;
		boolean t12 = val12 == 256;
		boolean t13 = val13 == Integer.MAX_VALUE;

		return t1 && t2 && t3 && t4 && t5 && t6 && t7 && t8 && t9 && t10 && t11 && t12 && t13;
	}

	public static boolean testDecodeLong(String argument) throws Exception {

		byte[] fileContent = InteropUtil.readFromFile(argument, 13 * 8);
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

		boolean t1 = val1 == Long.MIN_VALUE;
		boolean t2 = val2 == (long) -256;
		boolean t3 = val3 == (long) -255;
		boolean t4 = val4 == (long) -128;
		boolean t5 = val5 == (long) -127;
		boolean t6 = val6 == (long) -1;
		boolean t7 = val7 == (long) 0;
		boolean t8 = val8 == (long) 1;
		boolean t9 = val9 == (long) 127;
		boolean t10 = val10 == (long) 128;
		boolean t11 = val11 == (long) 255;
		boolean t12 = val12 == (long) 256;
		boolean t13 = val13 == Long.MAX_VALUE;
		
		return t1 && t2 && t3 && t4 && t5 && t6 && t7 && t8 && t9 && t10 && t11 && t12 && t13;
	}

	public static boolean testDecodeByte(String argument) throws Exception {

		byte[] fileContent = InteropUtil.readFromFile(argument, 256);
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);

		boolean result = false;
		for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) // -128 ... 127
		{
			byte b = buf.readByte();
			result = result && i == b;
		}
		
		return result;
	}

	public static boolean testDecodeBytes(String argument) throws Exception {

		byte[] fileContent = InteropUtil.readFromFile(argument, 256);
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);

		byte[] byteArray = new byte[256];
		buf.readBytes(byteArray);

		boolean result = false;
		for (int i = 0, b = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++, b++) {
			result = result && b == byteArray[i];
		}
		
		return result;
	}
}
