/*
 * Copyright 2009 Thomas Bocek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.tomp2p.message;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The decoder first decodes the header. If this has been successful, then if
 * there is payload, then the payload is decoded. The TomP2P decoder can be used
 * to decode several messages in the same session as long as they are
 * sequential. This class is not thread safe.
 * 
 * @author Thomas Bocek
 * 
 */
public class TomP2PDecoderTCP extends FrameDecoder
{
	final private static Logger logger = LoggerFactory.getLogger(TomP2PDecoderTCP.class);
	final private int maxMessageSize;
	// This var keeps the state of the decoding. If null, header not complete,
	// if not null, header is complete
	private volatile Message message = null;

	public TomP2PDecoderTCP()
	{
		this(Integer.MAX_VALUE);
	}

	public TomP2PDecoderTCP(int maxMessageSize)
	{
		this.maxMessageSize = maxMessageSize;
	}

	@Override
	protected Object decode(final ChannelHandlerContext ctx, final Channel channel,
			final ChannelBuffer buffer) throws Exception
	{
		buffer.markReaderIndex();
		//System.err.println("got decoder"+buffer.readableBytes()+" "+channel.isOpen());
		if (message == null && buffer.readableBytes() >= MessageCodec.HEADER_SIZE)
		{
			final SocketAddress sa = channel.getRemoteAddress();
			// System.err.println("tcp decoder"+sa);
			message = MessageCodec.decodeHeader(buffer, ((InetSocketAddress) sa).getAddress());
			if (message.getContentLength() + MessageCodec.HEADER_SIZE > maxMessageSize)
				throw new DecoderException("Messag too large :"
						+ (message.getContentLength() + MessageCodec.HEADER_SIZE)
						+ " allowed are: " + maxMessageSize);
			if(logger.isDebugEnabled())
				logger.debug("got header in decoder "+message);
		}
		else if (message != null)
			buffer.readerIndex(MessageCodec.HEADER_SIZE);
		if (message != null && message.getContentLength() == 0)
		{
			return cleanupAndReturnMessage();
		}
		else if (message != null && buffer.readableBytes() >= message.getContentLength())
		{
			MessageCodec.decodePayload(message.getContentType1(), buffer, message);
			MessageCodec.decodePayload(message.getContentType2(), buffer, message);
			MessageCodec.decodePayload(message.getContentType3(), buffer, message);
			MessageCodec.decodePayload(message.getContentType4(), buffer, message);
			return cleanupAndReturnMessage();
		}
		buffer.resetReaderIndex();
		return null;
	}

	@Override
	public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e)
			throws Exception
	{
		if (logger.isDebugEnabled())
			e.getCause().printStackTrace();
		if(e.getCause().getMessage()!=null && !e.getCause().getMessage().equals("Connection reset by peer"))
		{
			ctx.sendUpstream(e);
		}
	}

	/**
	 * After successfully reception of the message, we need to set message to
	 * null in order to reuse the connection. If connection is closed, then this
	 * cleanup would not be necessary as the annotation
	 * ChannelPipelineCoverage("one") creates a new TomP2PDecoder. Thus, we do
	 * not need to cleanup in the close() or disconnect() method.
	 * 
	 * @return The message, which reference has been set to null
	 */
	private Message cleanupAndReturnMessage()
	{
		final Message tmp = message;
		message = null;
		// set finished time at the end since the sender starts its timer after
		// sending the last packet
		if(logger.isDebugEnabled())
			logger.debug("cleanupAndReturnMessage "+tmp);
		tmp.setTCP();
		tmp.finished();
		return tmp;
	}
}
