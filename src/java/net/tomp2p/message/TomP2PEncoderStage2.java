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

import java.nio.ByteOrder;

import org.jboss.netty.buffer.CompositeChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

@Sharable
public class TomP2PEncoderStage2 extends OneToOneEncoder
{
	@Override
	protected Object encode(final ChannelHandlerContext ctx, final Channel channel, final Object msg)
			throws Exception
	{
		if (!(msg instanceof IntermediateMessage))
			return msg;
		final IntermediateMessage message = (IntermediateMessage) msg;
		//we created the buffer ourselfs, so we can pick the first one
		ByteOrder order=message.getBuffers().get(0).order();
		return new CompositeChannelBuffer(order, message.getBuffers());
	}
}
