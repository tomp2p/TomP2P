package net.tomp2p.utils;

import java.io.IOException;
import java.io.InputStream;

import org.jboss.netty.buffer.ChannelBuffer;

public class MultiByteBufferInputStream extends InputStream
{
	private final ChannelBuffer channelBuffer;
	public MultiByteBufferInputStream(ChannelBuffer channelBuffer)
	{
		this.channelBuffer = channelBuffer;
	}

	@Override
	public int read() throws IOException
	{
		if(!channelBuffer.readable())
		{
			return -1;
		}
		return channelBuffer.readUnsignedByte();
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException
	{
		int len2 = channelBuffer.readableBytes();
		int read = Math.min(len, len2);
		channelBuffer.readBytes(b, off, read);
		return read;
	}
}
