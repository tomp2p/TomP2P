package net.tomp2p;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import net.tomp2p.storage.AlternativeCompositeByteBuf;

/***
 * This class generates/checks the binary encoding/decoding of data types between Java and .NET.
 * 
 * @author Christian Lüthold
 *
 */
public class DotNetEncodeDecode {

	public static byte[] encodeByte() throws Exception {
	
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();

		for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) // -128 ... 127
		{
			buf.writeByte(i);
		}
	
		return InteropUtil.extractBytes(buf);
	}

	public static byte[] encodeBytes() throws Exception {
	
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
	
		byte[] byteArray = new byte[256];
		for (int i = 0, b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; i++, b++) {
			byteArray[i] = (byte) b;
		}
	
		buf.writeBytes(byteArray);
	
		return InteropUtil.extractBytes(buf);
	}

	public static byte[] encodeShort() throws Exception {

		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		
		buf.writeShort(Short.MIN_VALUE); //-32768
		buf.writeShort(-256);
		buf.writeShort(-255);
		buf.writeShort(-128);
		buf.writeShort(-127);
		buf.writeShort(-1);
		buf.writeShort(0);
		buf.writeShort(1);
		buf.writeShort(127);
		buf.writeShort(128);
		buf.writeShort(255);
		buf.writeShort(256);
		buf.writeShort(Short.MAX_VALUE); // 32,767

		return InteropUtil.extractBytes(buf);
	}
	
	public static byte[] encodeInt() throws Exception {

		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		
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

		return InteropUtil.extractBytes(buf);
	}

	public static byte[] encodeLong() throws IOException {

		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		
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

		return InteropUtil.extractBytes(buf);
	}

	public static byte[] testDecodeByte(String argument) throws Exception {
	
		byte[] fileContent = InteropUtil.readFromFile(argument);
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);
	
		boolean result = true;
		for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) // -128 ... 127
		{
			byte b = buf.readByte();
			result = result && (i == b);
		}
	
		return new byte[] { result ? (byte) 1 : (byte) 0 };
	}

	public static byte[] testDecodeBytes(String argument) throws Exception {
	
		byte[] fileContent = InteropUtil.readFromFile(argument);
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);
	
		byte[] byteArray = new byte[256];
		buf.readBytes(byteArray);
	
		boolean result = true;
		for (int i = 0, b = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++, b++) { // -128 ... 127
			result = result && (b == byteArray[i]);
		}
	
		return new byte[] { result ? (byte) 1 : (byte) 0 };
	}
	
	public static byte[] testDecodeShort(String argument) throws IOException {

		byte[] fileContent = InteropUtil.readFromFile(argument);
		ByteBuf buf = Unpooled.copiedBuffer(fileContent);

		short val1 = buf.readShort();
		short val2 = buf.readShort();
		short val3 = buf.readShort();
		short val4 = buf.readShort();
		short val5 = buf.readShort();
		short val6 = buf.readShort();
		short val7 = buf.readShort();
		short val8 = buf.readShort();
		short val9 = buf.readShort();
		short val10 = buf.readShort();
		short val11 = buf.readShort();
		short val12 = buf.readShort();
		short val13 = buf.readShort();

		boolean t1 = val1 == Short.MIN_VALUE;
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
		boolean t13 = val13 == Short.MAX_VALUE;

		boolean result = t1 && t2 && t3 && t4 && t5 && t6 && t7 && t8 && t9 && t10 && t11 && t12 && t13;
		return new byte[] { result ? (byte) 1 : (byte) 0 };
	}

	public static byte[] testDecodeInt(String argument) throws IOException {

		byte[] fileContent = InteropUtil.readFromFile(argument);
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

		boolean result = t1 && t2 && t3 && t4 && t5 && t6 && t7 && t8 && t9 && t10 && t11 && t12 && t13;
		return new byte[] { result ? (byte) 1 : (byte) 0 };
	}

	public static byte[] testDecodeLong(String argument) throws Exception {

		byte[] fileContent = InteropUtil.readFromFile(argument);
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

		boolean result = t1 && t2 && t3 && t4 && t5 && t6 && t7 && t8 && t9 && t10 && t11 && t12 && t13;
		return new byte[] { result ? (byte) 1 : (byte) 0 };
	}
}
