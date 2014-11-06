package net.tomp2p;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TestClass {

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

	protected void writeToFile(byte[] bytes) throws IOException {

		File file = new File(to);
		FileOutputStream fos = new FileOutputStream(file);
		try {
			fos.write(bytes);
		} finally {
			fos.close();
		}
	}
}
