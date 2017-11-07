package net.tomp2p.connection;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;

public class ChannelUtils {
	public static ByteBuffer convert(ByteBuf buf) {
    	if(buf.nioBufferCount() == 0) {
    		return ByteBuffer.allocate(0);
    	} else if (buf.nioBufferCount() == 1) {
    		return buf.nioBuffer();
    	} else {
    		final byte[] bytes = new byte[buf.readableBytes()];
    		buf.getBytes(buf.readerIndex(), bytes);
    	    return ByteBuffer.wrap(bytes);
    	}
    }
}
