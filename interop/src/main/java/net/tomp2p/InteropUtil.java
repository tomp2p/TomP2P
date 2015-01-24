package net.tomp2p;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.tomp2p.storage.AlternativeCompositeByteBuf;

import org.apache.commons.io.IOUtils;

public class InteropUtil {

	private static final String TmpDir = "C:/Users/Christian/Desktop/interop/";
	//private static final String TmpDir = "D:/Desktop/interop/";

	public static byte[] readFromFile(String argument) throws IOException {

		byte[] buffer = null;
		String path = String.format("%s%s-in.txt", TmpDir, argument);

		FileInputStream fis = new FileInputStream(path);
		buffer = IOUtils.toByteArray(fis);

		return buffer;
	}

	public static void writeToFile(String argument, byte[] bytes) throws IOException {

		String path = String.format("%s%s-out.txt", TmpDir, argument);

		File file = new File(path);
		FileOutputStream fos = new FileOutputStream(file);
		try {
			fos.write(bytes);
		} finally {
			fos.close();
		}
	}
		
	/**
	 * Gets the byte array from the provided {@link AlternativeCompositeByteBuf}.
	 * 
	 * @param buf
	 * @return
	 */
	public static byte[] extractBytes(AlternativeCompositeByteBuf buf) {
		/*
		 * ByteBuffer buf2 = ByteBuffer.allocate(buf.nioBuffer().remaining());
		 * byte[] bytes = new byte[buf.nioBuffer().remaining()];
		 * buf2.get(bytes);
		 * return bytes;
		 */

		ByteBuffer buffer = buf.nioBuffer();
		buffer.position(0);

		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return bytes;
	}
}