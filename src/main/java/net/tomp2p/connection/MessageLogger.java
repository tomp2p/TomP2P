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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.zip.GZIPOutputStream;

import net.tomp2p.message.Message;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class MessageLogger implements ChannelUpstreamHandler
{
	final private static Logger logger = LoggerFactory.getLogger(MessageLogger.class);
	final private PrintWriter pw;
	final private GZIPOutputStream gz;
	private boolean open = false;

	public MessageLogger(File messageLogger) throws FileNotFoundException, IOException
	{
		this.gz = new GZIPOutputStream(new FileOutputStream(messageLogger));
		this.pw = new PrintWriter(gz);
		open = true;
	}
	
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception
	{
		if (e instanceof MessageEvent)
			messageReceived((MessageEvent) e);
		ctx.sendUpstream(e);
	}

	public void customMessage(String customMessage)
	{
		synchronized (pw)
		{
			if (open)
			{
				pw.println("C:".concat(customMessage));
				pw.flush();
			}
		}
	}

	private void messageReceived(MessageEvent e) throws Exception
	{
		synchronized (pw)
		{
			if (open && e.getMessage() instanceof Message)
			{
				pw.println("R:".concat(e.getMessage().toString()));
				pw.flush();
			}
		}
	}

	public void close()
	{
		synchronized (pw)
		{
			pw.flush();
			try
			{
				gz.finish();
				gz.close();
			}
			catch (IOException e)
			{
				// try hard, otherwise we cannot do anything...
				logger.error(e.toString());
				e.printStackTrace();
			}
			pw.close();
			open = false;
		}
	}
}
