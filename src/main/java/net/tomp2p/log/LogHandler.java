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
package net.tomp2p.log;
import net.tomp2p.message.Message;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

@Deprecated
public class LogHandler extends SimpleChannelHandler
{
	final private PeerLogger peerLogger;

	public LogHandler(final PeerLogger statSender)
	{
		this.peerLogger = statSender;
	}

	@Override
	public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
			throws Exception
	{
		peerLogger.sendLog("rcv", info(e.getMessage()));
		ctx.sendUpstream(e);
	}

	@Override
	public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e)
			throws Exception
	{
		peerLogger.sendLog("snd", info(e.getMessage()));
		ctx.sendDownstream(e);
	}

	private String info(final Object message)
	{
		return message instanceof Message ? message.toString() : "n/a for ".concat(message
				.getClass().toString());
	}
}
