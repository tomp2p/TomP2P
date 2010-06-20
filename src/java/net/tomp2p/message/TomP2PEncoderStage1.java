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

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

@Sharable
public class TomP2PEncoderStage1 extends OneToOneEncoder
{
	@Override
	protected Object encode(final ChannelHandlerContext ctx, final Channel channel, final Object msg)
			throws Exception
	{
		if (!(msg instanceof Message))
			return msg;
		final Message message = (Message) msg;
		final List<ChannelBuffer> buffers = new ArrayList<ChannelBuffer>();
		MessageCodec.encodePayload(message,buffers);
		// we need to call this before the header to find out the payload length
		final ChannelBuffer headerBuffer= ChannelBuffers.buffer(MessageCodec.HEADER_SIZE);
		MessageCodec.encodeHeader(headerBuffer, message);
		//1 is the header, 2 from the security part
		buffers.add(0,headerBuffer);
		MessageCodec.encodeSecurity(message, buffers);
		return new IntermediateMessage(message, buffers);
	}
}