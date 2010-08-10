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
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.tomp2p.futures.FutureResponse;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPChannelChache
{
	final private static Logger logger = LoggerFactory.getLogger(TCPChannelChache.class);
	private Map<InetSocketAddress, Channel> cache = new HashMap<InetSocketAddress, Channel>();
	private Set<InetSocketAddress> active = new HashSet<InetSocketAddress>();
	private Map<InetSocketAddress, Thread> tt = new HashMap<InetSocketAddress, Thread>();

	/**
	 * Add an open channel to the cache.
	 * 
	 * @param createSocketTCP The identifier
	 * @param channel The channel to be cached
	 * @param futureResponse The reference for the release
	 * @return True if channel could be added, which means, that we need to
	 *         release the channel if not used anymore. False otherwise
	 */
	public void addAndAcquire(final InetSocketAddress createSocketTCP, Channel channel,
			FutureResponse futureResponse)
	{
		synchronized (cache)
		{
			if (cache.containsKey(createSocketTCP))
			{
				if (logger.isDebugEnabled())
					logger
							.debug("could not add channel to cache, since there is already a cached channel for "
									+ createSocketTCP);
				return;
			}
			cache.put(createSocketTCP, channel);
			tt.put(createSocketTCP, Thread.currentThread());
			// we are using it right now, so mark as active
			active.add(createSocketTCP);
			// remove from cache if someone closes the connection
			channel.getCloseFuture().addListener(new ChannelFutureListener()
			{
				@Override
				public void operationComplete(ChannelFuture future) throws Exception
				{
					synchronized (cache)
					{
						if (logger.isDebugEnabled())
							logger.debug("remove from cache, since connection closed "
									+ createSocketTCP);
						cache.remove(createSocketTCP);
						active.remove(createSocketTCP);
					}
				}
			});
			futureResponse.prepareRelease(createSocketTCP, this);
		}
	}

	/**
	 * Try to acquire a cached channel. This operation may block if the
	 * connection is active
	 * 
	 * @param createSocketTCP The identifier
	 * @param futureResponse The reference for the release
	 * @return The cached channel or null, if no cached channel exists
	 * @throws InterruptedException
	 */
	public Channel acquire(InetSocketAddress createSocketTCP, FutureResponse futureResponse)
			throws InterruptedException
	{
		synchronized (cache)
		{
			Channel channel = cache.get(createSocketTCP);
			if (channel != null)
			{
				// we have a cached channel! lets see if its busy
			        long start=System.currentTimeMillis();
				while (active.contains(createSocketTCP))
				{
					cache.wait(500);
					if(System.currentTimeMillis()-start>5000)
					{
					  //channel still busy...
					  Thread t=tt.get(createSocketTCP);
					  System.err.println("current channel is used here: ");
					  for(StackTraceElement ste:t.getStackTrace())
					  {
					    System.err.println(ste.getClassName()+", "+ste.getMethodName()+", "+ste.getLineNumber());
					  }
					}
				}
				// in the meantime, the channel may have been removed
				if(!cache.containsKey(createSocketTCP))
					return null;
				if (logger.isDebugEnabled())
					logger.debug("acquire cached channel " + createSocketTCP);
				active.add(createSocketTCP);
				futureResponse.prepareRelease(createSocketTCP, this);
				return channel;
			}
			else
			{
				if (logger.isDebugEnabled())
					logger.debug("no cached channel found " + createSocketTCP);
				return null;
			}
		}
	}

	/**
	 * If the connection is done, release the channel to the cache
	 * 
	 * @param createSocketTCP The identifier
	 * @return True if the channel could be released
	 */
	public boolean release(InetSocketAddress createSocketTCP)
	{
		synchronized (cache)
		{
			if (logger.isDebugEnabled())
				logger.debug("release " + createSocketTCP);
			boolean result = active.remove(createSocketTCP);
			if (!result)
				logger.warn("could not release channel " + createSocketTCP);
			cache.notifyAll();
			return result;
		}
	}

	/**
	 * Iterate over cached connections and close the ones not being used. In
	 * future there a least recently used would make sense.
	 */
	public void expireCache()
	{
		synchronized (cache)
		{
			Channel closeme = null;
			for (Map.Entry<InetSocketAddress, Channel> entry : cache.entrySet())
			{
				if (!active.contains(entry.getKey()))
					closeme = entry.getValue();
			}
			if (closeme != null)
			{
				logger.debug("closing expired channel");
			        closeme.close();
			}
		}
	}
}
