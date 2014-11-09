package net.tomp2p;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class InteropUtil {

	private static final String TmpDir = "C:/Users/Christian/Desktop/interop/";
	
	public static byte[] readFromFile(String argument, int fileSize) throws IOException {
		
		String path = String.format("%s%s-in.txt", TmpDir, argument);
		
		FileInputStream fis = new FileInputStream(path);
		byte[] buffer = new byte[fileSize];
		try {
			fis.read(buffer);
		}
		finally {
			fis.close();
		}
		
		return buffer;
	}

	public static void writeToFile(String argument, byte[] bytes) throws IOException {
		
		String path = String.format("%s%s-out.txt", TmpDir, argument);
		
		File file = new File(path);
		FileOutputStream fos = new FileOutputStream(file);
		try {
			fos.write(bytes);
		}
		finally {
			fos.close();
		}
	}
}
