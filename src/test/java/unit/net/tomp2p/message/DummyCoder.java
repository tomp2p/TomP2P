package net.tomp2p.message;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import net.tomp2p.message.IntermediateMessage;
import net.tomp2p.message.Message;
import net.tomp2p.message.TomP2PDecoderTCP;
import net.tomp2p.message.TomP2PEncoderStage1;
import net.tomp2p.message.TomP2PEncoderStage2;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.CompositeChannelBuffer;

public class DummyCoder 
{
	final private SocketAddress sock = new InetSocketAddress(2000);
	final private DummyChannel dc = new DummyChannel(sock);
	final private DummyChannelHandlerContext dchc = new DummyChannelHandlerContext(dc);
	
	public ChannelBuffer encode(Message m1) throws Exception
	{
		IntermediateMessage im = (IntermediateMessage) new TomP2PEncoderStage1().encode(dchc, dc, m1);
		ChannelBuffer buffer = (ChannelBuffer)new TomP2PEncoderStage2().encode(dchc, dc, im);
		// flatten first
		List<ChannelBuffer> tmp=((CompositeChannelBuffer)buffer).decompose(0, buffer.capacity());
		// decode
		ChannelBuffer flat=ChannelBuffers.dynamicBuffer();
		for(ChannelBuffer buff:tmp)
			flat.writeBytes(buff);
		return flat;
	}
	
	public Message decode(ChannelBuffer buffer) throws Exception
	{
		return (Message)new TomP2PDecoderTCP().decode(null, dc, buffer);
	}
}
