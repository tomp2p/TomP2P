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
package net.tomp2p.connection;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Measures the number of outgoing and incoming packets. This is used to test
 * the performance. The logger is set to debug and will only output if the msg/s
 * is larger than 1. To enable the performance, set in tomp2plog.properties
 * "net.tomp2p.connection.PerformanceFilter.level = FINE"
 * 
 * @author Thomas Bocek
 */
@Sharable
public class PerformanceFilter extends SimpleChannelHandler
{
	final private static Logger logger = LoggerFactory.getLogger(PerformanceFilter.class);
	private static AtomicLong startSend = new AtomicLong(System.currentTimeMillis());
	private static AtomicLong startReceive = new AtomicLong(System.currentTimeMillis());
	private static AtomicLong messagesCountReceive = new AtomicLong(0);
	private static AtomicLong messagesCountSend = new AtomicLong(0);

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
	{
		messagesCountReceive.incrementAndGet();
		final long time = System.currentTimeMillis() - startReceive.get();
		if (time > 1000)
		{
			double throughput = messagesCountReceive.doubleValue() / (time / 1000d);
			if (throughput > 1 && logger.isDebugEnabled())
			{
				logger.debug("Incoming throughput="
						+ throughput + "msg/s");
			}
			startReceive.set(System.currentTimeMillis());
			messagesCountReceive.set(0);
		}
		ctx.sendUpstream(e);
	}

	@Override
	public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception
	{
		messagesCountSend.incrementAndGet();
		long time = System.currentTimeMillis() - startSend.get();
		if (time > 1000)
		{
			double throughput = messagesCountSend.doubleValue() / (time / 1000d);
			if (throughput > 1 && logger.isDebugEnabled())
			{
				logger.debug("[Outgoing throughput="
						+ throughput + "msg/s");
			}
			startSend.set(System.currentTimeMillis());
			messagesCountSend.set(0);
		}
		ctx.sendDownstream(e);
	}
}