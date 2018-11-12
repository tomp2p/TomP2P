package net.tomp2p.connection;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChannelUtils {
	private static final Logger LOG = LoggerFactory.getLogger(ChannelUtils.class);

	public static byte[] convert2(ByteBuffer buf) {
		byte[] arr = new byte[buf.remaining()];
		buf.get(arr);
		return arr;
	}
}
