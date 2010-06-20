package net.tomp2p.message;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

public class IntermediateMessage
{
	final private List<ChannelBuffer> buffers;
	final private Message message;

	public IntermediateMessage(Message message, List<ChannelBuffer> buffers)
	{
		this.buffers=buffers;
		this.message = message;
	}

	public List<ChannelBuffer> getBuffers()
	{
		return buffers;
	}
	
	@Override
	public String toString()
	{
		return message.toString();
	}
}
