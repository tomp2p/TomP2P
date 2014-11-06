package net.tomp2p;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public abstract class TestInteropBase {

	// specify a local file path where the platforms can interchange their bytes
	
	//protected final String from = "D:/Desktop/interop/bytes-NET-encoded.txt";
	//protected final String to = "D:/Desktop/interop/bytes-JAVA-encoded.txt";
	protected final String from = "C:/Users/Christian/Desktop/interop/bytes-NET-encoded.txt";
	protected final String to = "C:/Users/Christian/Desktop/interop/bytes-JAVA-encoded.txt";
	
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
	
	protected void writeToFile(byte[] bytes) throws IOException {
		
		File file = new File(to);
		FileOutputStream fos = new FileOutputStream(file);
		try {
			fos.write(bytes);
		}
		finally {
			fos.close();
		}
	}
	
}
