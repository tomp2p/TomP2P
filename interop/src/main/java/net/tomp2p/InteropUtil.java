package net.tomp2p;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

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
}