package net.tomp2p;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;

public class InteropUtil {

	private static final String TmpDir1 = "C:/Users/Christian/Desktop/interop/";
	private static final String TmpDir2 = "D:/Desktop/interop/";
	private static String directory = TmpDir1;
	
	public static byte[] readFromFile(String argument) throws IOException {
		
		byte[] buffer = null;
		try {
			String path = String.format("%s%s-in.txt", directory, argument);
			
			FileInputStream fis = new FileInputStream(path);
			buffer = IOUtils.toByteArray(fis);
			
		} catch (IOException ex) {
			directory = TmpDir2;
			readFromFile(argument);
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