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
import static org.jboss.netty.channel.Channels.fireMessageReceived;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ChannelHandler.Sharable;

@Sharable
public class TomP2PDecoderUDP implements ChannelUpstreamHandler
{
	@Override
	public void handleUpstream(final ChannelHandlerContext ctx, final ChannelEvent evt)
			throws Exception
	{
		if (!(evt instanceof MessageEvent))
		{
			ctx.sendUpstream(evt);
			return;
		}
		final MessageEvent e = (MessageEvent) evt;
		final Object originalMessage = e.getMessage();
		final Object decodedMessage = decode(ctx, e.getChannel(), originalMessage, e
				.getRemoteAddress());
		if (originalMessage == decodedMessage)
			ctx.sendUpstream(evt);
		else
			fireMessageReceived(ctx, decodedMessage, e.getRemoteAddress());
	}

	private Object decode(final ChannelHandlerContext ctx, final Channel channel, final Object obj,
			final SocketAddress socketAddress) throws Exception
	{
		if (!(obj instanceof ChannelBuffer))
			return obj;
		final ChannelBuffer buffer = (ChannelBuffer) obj;
		if (buffer.readableBytes() >= MessageCodec.HEADER_SIZE)
		{
			final Message message = MessageCodec.decodeHeader(buffer,
					((InetSocketAddress) socketAddress).getAddress());
			//System.err.println("udp decoder"+socketAddress);
			// set finished time before, as the sender already started its timer
			message.setUDP();
			message.finished();
			if (message.getContentLength() > 0)
			{
				if (buffer.readableBytes() >= message.getContentLength())
				{
					MessageCodec.decodePayload(message.getContentType1(), buffer, message);
					MessageCodec.decodePayload(message.getContentType2(), buffer, message);
					MessageCodec.decodePayload(message.getContentType3(), buffer, message);
					MessageCodec.decodePayload(message.getContentType4(), buffer, message);
				}
				else
				{
					Channels.fireExceptionCaught(ctx, new DecoderException(
							"did not get all the data expected " + message.getContentLength()
									+ " got " + buffer.readableBytes()));
				}
			}
			return message;
		}
		return obj;
	}
}
