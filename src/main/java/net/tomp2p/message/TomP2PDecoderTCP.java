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
import java.security.Signature;

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
	private volatile byte[] rawHeader = new byte[MessageCodec.HEADER_SIZE];
	private volatile Signature signature = null;
	private volatile int step = 0;

	public TomP2PDecoderTCP()
	{
		this(Integer.MAX_VALUE);
	}

	public TomP2PDecoderTCP(int maxMessageSize)
	{
		this.maxMessageSize = maxMessageSize;
	}

	@Override
	protected Object decode(final ChannelHandlerContext ctx, final Channel channel, final ChannelBuffer buffer)
			throws Exception
	{
		if (buffer.readableBytes() > maxMessageSize)
		{
			throw new DecoderException("Message size larger than " + maxMessageSize);
		}
		// read header if possible and not already read
		if (message == null && buffer.readableBytes() >= MessageCodec.HEADER_SIZE)
		{
			// in case we want to check the signature, we need to keep the
			// header for a while
			buffer.getBytes(buffer.readerIndex(), rawHeader);
			final SocketAddress sa = channel.getRemoteAddress();
			message = MessageCodec.decodeHeader(buffer, ((InetSocketAddress) sa).getAddress());
			if (logger.isDebugEnabled())
				logger.debug("got header in decoder " + message);
			// set that we did not read data, otherwise it gets lost.
			if (!message.hasContent())
			{
				return cleanupAndReturnMessage();
			}
		}
		// go for the content. Since we don't have the length anymore, we decide
		// on the fly when we are finished
		if (message != null && message.hasContent())
		{
			int readerIndex = buffer.readerIndex();
			if (step == 0 && !MessageCodec.decodePayload(message.getContentType1(), buffer, message))
			{
				buffer.readerIndex(readerIndex);
				return null;
			}
			else if (step == 0)
			{
				step++;
				if (message.isHintSign())
				{
					signature = Signature.getInstance("SHA1withDSA");
					signature.initVerify(message.getPublicKey());
					signature.update(rawHeader);
					int read = buffer.readerIndex() - readerIndex;
					signature.update(buffer.array(), buffer.arrayOffset() + readerIndex, read);
				}
				readerIndex = buffer.readerIndex();
			}

			if (step == 1 && !MessageCodec.decodePayload(message.getContentType2(), buffer, message))
			{
				buffer.readerIndex(readerIndex);
				return null;
			}
			else if (step == 1)
			{
				step++;
				if (signature != null)
				{
					int read = buffer.readerIndex() - readerIndex;
					signature.update(buffer.array(), buffer.arrayOffset() + readerIndex, read);
				}
				readerIndex = buffer.readerIndex();
			}

			if (step == 2 && !MessageCodec.decodePayload(message.getContentType3(), buffer, message))
			{
				buffer.readerIndex(readerIndex);
				return null;
			}
			else if (step == 2)
			{
				step++;
				if (signature != null)
				{
					int read = buffer.readerIndex() - readerIndex;
					signature.update(buffer.array(), buffer.arrayOffset() + readerIndex, read);
				}
				readerIndex = buffer.readerIndex();
			}
			
			if (step == 3 && !MessageCodec.decodePayload(message.getContentType4(), buffer, message))
			{
				buffer.readerIndex(readerIndex);
				return null;
			}
			else if (step == 3)
			{
				step++;
				if (signature != null)
				{
					int read = buffer.readerIndex() - readerIndex;
					signature.update(buffer.array(), buffer.arrayOffset() + readerIndex, read);
				}
				readerIndex = buffer.readerIndex();
			}

			if (step == 4 && signature != null && !MessageCodec.decodeSignature(signature, message, buffer))
			{
				buffer.readerIndex(readerIndex);
				return null;
			}
			return cleanupAndReturnMessage();
		}
		else
		{
			return null;
		}
	}

	@Override
	public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) throws Exception
	{
		if (logger.isDebugEnabled())
			e.getCause().printStackTrace();
		if (e.getCause().getMessage() == null)
		{
			e.getCause().printStackTrace();
			ctx.sendUpstream(e);
			return;
		}
		if (e.getCause().getMessage().equals("Connection reset by peer"))
		{
			ctx.sendUpstream(e);
			return;
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
		step = 0;
		signature = null;
		// set finished time at the end since the sender starts its timer after
		// sending the last packet
		if (logger.isDebugEnabled())
			logger.debug("cleanupAndReturnMessage " + tmp);
		tmp.setTCP();
		tmp.finished();
		return tmp;
	}

}
