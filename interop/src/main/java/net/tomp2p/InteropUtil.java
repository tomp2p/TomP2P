package net.tomp2p;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class InteropUtil {

	private static final String TmpDir1 = "C:/Users/Christian/Desktop/interop/";
	private static final String TmpDir2 = "D:/Desktop/interop/";
	private static String directory = TmpDir1;
	
	public static byte[] readFromFile(String argument, int fileSize) throws IOException {
		
		byte[] buffer = null;
		try {
			String path = String.format("%s%s-in.txt", directory, argument);
			
			FileInputStream fis = new FileInputStream(path);
			buffer = new byte[fileSize];
			try {
				fis.read(buffer);
			}
			finally {
				fis.close();
			}
		} catch (IOException ex) {
			directory = TmpDir2;
			readFromFile(argument, fileSize);
		}
		return buffer;
	}

	public static void writeToFile(String argument, byte[] bytes) throws IOException {
		
		try {
			String path = String.format("%s%s-out.txt", directory, argument);
			
			File file = new File(path);
			FileOutputStream fos = new FileOutputStream(file);
			try {
				fos.write(bytes);
			}
			finally {
				fos.close();
			}
		} catch (IOException ex) {
			directory = TmpDir2;
			writeToFile(argument, bytes);
		}
	}
}
