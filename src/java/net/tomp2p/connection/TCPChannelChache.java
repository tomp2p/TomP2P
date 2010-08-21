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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.Number160;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPChannelChache
{
	final public static String DEFAULT_CHANNEL_NAME = "any";
	final private static Logger logger = LoggerFactory.getLogger(TCPChannelChache.class);
	final private Map<Identifier, ChannelFuture> cache = new HashMap<Identifier, ChannelFuture>();
	final private ConnectionCollector connectionCollector;
	final private Timer timer;
	final private ChannelGroup channelGroup;
	private boolean running = true;
	private DispatcherRequest dispatcherRequest;

	public TCPChannelChache(ConnectionCollector connectionCollector, Timer timer,
			ChannelGroup channelGroup)
	{
		this.connectionCollector = connectionCollector;
		this.timer = timer;
		this.channelGroup = channelGroup;
	}

	public void addChannel(Number160 recipientID, Number160 senderID, InetAddress inetAddress, Channel channel)
	{
		Identifier identifier = new Identifier(recipientID, senderID, inetAddress, DEFAULT_CHANNEL_NAME);
		ChannelFuture future = new DefaultChannelFuture(channel, false);
		future.setSuccess();
		synchronized (cache)
		{
			if (!cache.containsKey(identifier) && running)
			{
				if(logger.isDebugEnabled())
					logger.debug("add to TCP cache (add) "+identifier+ "/"+channel);
				cache.put(identifier, future);
			}
		}
	}

	public ChannelFuture getChannel(Number160 recipientID, Number160 senderID, InetSocketAddress recipientAddress,
			ChannelHandler timeoutHandler, FutureResponse futureResponse, int connectTimeoutMillis,
			int tcpIdleTimeoutMillis) throws InterruptedException
	{
		return getChannel(recipientID, senderID, recipientAddress, DEFAULT_CHANNEL_NAME, timeoutHandler,
				futureResponse, connectTimeoutMillis, tcpIdleTimeoutMillis);
	}

	public ChannelFuture getChannel(Number160 recipientID, Number160 senderID, InetSocketAddress recipientAddress,
			String channelName, ChannelHandler timeoutHandler, FutureResponse futureResponse,
			int connectTimeoutMillis, int tcpIdleTimeoutMillis) throws InterruptedException
	{
		final Identifier identifier = new Identifier(recipientID, senderID, recipientAddress.getAddress(),
				channelName);
		synchronized (cache)
		{
			ChannelFuture future = cache.get(identifier);
			if (future != null)
			{
				if (logger.isDebugEnabled())
					logger.debug("reuse connection " + future.getChannel());
				if(future.getChannel()!=null)
				{
				  IdleStateHandler timeoutHandlerOld=(IdleStateHandler) future.getChannel().getPipeline().get("timeout");
				  timeoutHandlerOld.reset();
				  if(!future.getChannel().isOpen())
				    return createNewChannel(recipientID, recipientAddress, timeoutHandler,
	                                  connectTimeoutMillis, tcpIdleTimeoutMillis, identifier);
				}
				
				return future;
			}
			else
			{
				return createNewChannel(recipientID, recipientAddress, timeoutHandler,
                                  connectTimeoutMillis, tcpIdleTimeoutMillis, identifier);
			}
		}
	}

  private ChannelFuture createNewChannel(Number160 recipientID,
      InetSocketAddress recipientAddress,
      ChannelHandler timeoutHandler,
      int connectTimeoutMillis,
      int tcpIdleTimeoutMillis,
      final Identifier identifier) throws InterruptedException {
    ChannelFuture future;
    if (logger.isDebugEnabled())
    	logger.debug("no cached channel found, create one to " + recipientID + ", "
    			+ recipientAddress);
    // create channel
    DispatcherReply dispatcherReply = new DispatcherReply(timer, tcpIdleTimeoutMillis,
    		getDispatcherRequest(), channelGroup);
    future = connectionCollector.channelTCP(timeoutHandler, dispatcherReply,
    		recipientAddress, connectTimeoutMillis, this);
    if(future==null)
      return null;
    if (logger.isDebugEnabled())
      logger.debug("created channel "+future.getChannel());
    future.getChannel().getCloseFuture().addListener(new ChannelFutureListener()
    {
    	@Override
    	public void operationComplete(ChannelFuture future) throws Exception
    	{
    		synchronized (cache)
    		{
    		  if(logger.isDebugEnabled())
    		      logger.debug("connection was closed, so remove "+future.getChannel());
    		  cache.remove(identifier);
    		}
    	}
    });
    if(logger.isDebugEnabled())
    	logger.debug("add to TCP cache (get) "+identifier);
    cache.put(identifier, future);
    return future;
  }

	public boolean expireCache()
	{
		synchronized (cache)
		{
			for (ChannelFuture future : cache.values())
			{
				Channel channel = future.getChannel();
				DispatcherReply dispatcherReply = (DispatcherReply) channel.getPipeline().get(
						"reply");
				// TODO: search for the longest idle one...
				if (!dispatcherReply.isWaiting())
				{
				    if(logger.isDebugEnabled())
		                          logger.debug("expire channel");	
				      channel.close();
				return true;
				}
			}
			if(logger.isDebugEnabled())
			  logger.debug("could not expire any channel");
		}
		return false;
	}

	/*
	 * public void shutdown() { synchronized (cache) { running = false; for
	 * (ChannelFuture future : cache.values()) future.getChannel().close();
	 * cache.clear(); } }
	 */
	public void setDispatcherRequest(DispatcherRequest dispatcherRequest)
	{
		this.dispatcherRequest = dispatcherRequest;
	}

	public DispatcherRequest getDispatcherRequest()
	{
		return dispatcherRequest;
	}
	private static class Identifier
	{
		final private Number160 recipientId;
		final private Number160 senderId;
		final private InetAddress inetAddress;
		final private String channelName;
		final private Number160 both;

		public Identifier(Number160 recipientId, Number160 senderId, InetAddress inetAddress, String channelName)
		{
			this.recipientId = recipientId;
			this.senderId=senderId;
			this.inetAddress = inetAddress;
			this.channelName = channelName;
			this.both=senderId.xor(recipientId);
		}

		@Override
		public int hashCode()
		{
			return both.hashCode() ^ inetAddress.hashCode() ^ channelName.hashCode();
		}

		@Override
		public boolean equals(Object obj)
		{
			if (!(obj instanceof Identifier))
				return false;
			Identifier i = (Identifier) obj;
			return i.both.equals(both) && i.inetAddress.equals(inetAddress)
					&& i.channelName.equals(channelName);
		}

		@Override
		public String toString()
		{
			StringBuilder sb = new StringBuilder("recipientID:");
			sb.append(recipientId).append(",senderID:").append(senderId).append(",inet:").append(inetAddress).append(",name:").append(
					channelName);
			return sb.toString();
		}
	}
}
