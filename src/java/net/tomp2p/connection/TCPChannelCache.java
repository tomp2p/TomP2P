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
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.FutureResponse;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPChannelCache
{
	final private static Logger logger = LoggerFactory.getLogger(TCPChannelCache.class);

	private static final long	CONNECTION_KEEP_ALIVE_MILLIS = 5000L;
	final static AtomicInteger lastGivenId = new AtomicInteger();
	
	private Map<InetSocketAddress, Set<CachedChannel>> cache = new HashMap<InetSocketAddress, Set<CachedChannel>>();
	
	private class CachedChannel {
		final Channel channel;
		final int id;	//for debugging only (relating cached channels)
		private boolean active = true;
		private long inactiveSince;
		
		CachedChannel (final Channel channel) {
			this.channel = channel;
			this.id = lastGivenId.getAndIncrement();
		}
		
		void setActive(final boolean active) {
			if (!active)
				this.inactiveSince = Calendar.getInstance().getTimeInMillis();
			this.active = active;
		}
		boolean isActive() {
			return this.active;
		}
		long getInactiveForMillis() {
			return Calendar.getInstance().getTimeInMillis() - this.inactiveSince;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof CachedChannel)
				return channel.equals(((CachedChannel)obj).channel);
			else if (obj instanceof Channel) 
				return channel.equals((Channel)obj);
			else
				return false;
		}
	}
	
	/**
	 * Add an open channel to the cache.
	 * 
	 * @param createSocketTCP The identifier
	 * @param channel The channel to be cached
	 * @param futureResponse The reference for the release
	 * @return True if channel could be added, which means, that we need to
	 *         release the channel if not used anymore. False otherwise
	 */
	public void addAndAcquire(final InetSocketAddress createSocketTCP, final Channel channel,
			FutureResponse futureResponse)
	{
		synchronized (cache)
		{
			//adds to cache
			this.addToCache(createSocketTCP, channel);
			
			// remove from cache if someone closes the connection
			channel.getCloseFuture().addListener(new ChannelFutureListener()
			{
				@Override
				public void operationComplete(ChannelFuture future) throws Exception
				{
					synchronized (cache)
					{
						Set<CachedChannel> cachedChannels = cache.get(createSocketTCP); 
						if (cachedChannels!=null) {
							
							if (logger.isDebugEnabled())
								for (CachedChannel cachedChannel : cachedChannels)
									if (cachedChannel.equals(channel))
										logger.debug("remove from cache, since connection closed " + createSocketTCP + " ["+cachedChannel.id+"]");
							
							cachedChannels.remove(channel);
						}
						if (cachedChannels.isEmpty())
							cache.remove(createSocketTCP);
					}
				}
			});
			futureResponse.prepareRelease(createSocketTCP, this, channel);
		}
	}

	/**
	 * Try to acquire a cached channel.
	 * If all existing channels are busy, a new one is created and returned. 
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
			CachedChannel freeCachedChannel = null;
			
			while (freeCachedChannel==null) {
				
				freeCachedChannel = this.getFreeChannelFromCache(createSocketTCP);
				if (freeCachedChannel != null)
				{
					if (!freeCachedChannel.channel.isOpen()) {
						if (logger.isDebugEnabled())
							logger.debug("removing expired channel " + createSocketTCP + " ["+freeCachedChannel.id+"]");
	
						freeCachedChannel.channel.close();  //TODO not sure this is necessary, since its not open, but I felt that I should free resources
						this.cache.get(createSocketTCP).remove(freeCachedChannel);
						continue;
					}
	
					if (logger.isDebugEnabled())
						logger.debug("acquire cached channel " + createSocketTCP + " ["+freeCachedChannel.id+"]");
	
					freeCachedChannel.setActive(true);
					futureResponse.prepareRelease(createSocketTCP, this, freeCachedChannel.channel);
					
					return freeCachedChannel.channel;
				}
				else
				{
					if (logger.isDebugEnabled())
						logger.debug("no free cached channel found " + createSocketTCP);
					
					return null;
				}
			}
		}
		return null;
	}

	/**
	 * If the connection is done, release the channel to the cache
	 * 
	 * @param createSocketTCP The identifier
	 * @param channel 
	 * @return True if the channel could be released
	 */
	public boolean release(final InetSocketAddress createSocketTCP, final Channel channel)
	{
		synchronized (cache)
		{
			Set<CachedChannel> cachedChannels = cache.get(createSocketTCP); 
			if (cachedChannels!=null)
				for (CachedChannel cachedChannel : cachedChannels)
					if (cachedChannel.channel.equals(channel)) {
						if (logger.isDebugEnabled())
							logger.debug("release " + createSocketTCP + " ["+cachedChannel.id+"]");

						cachedChannel.setActive(false);
						return true;
					}
			
			logger.warn("could not release channel " + createSocketTCP);
			return false;
		}
	}

	/**
	 * Iterate over cached connections and close the ones not being used that have expired.
	 */
	public void expireCache()
	{
		synchronized (cache)
		{
			for (Entry<InetSocketAddress, Set<CachedChannel>> entry : cache.entrySet())
			{
				Iterator<CachedChannel> cachedChannelIterator = entry.getValue().iterator();  
				while (cachedChannelIterator.hasNext()) {
					CachedChannel cachedChannel = cachedChannelIterator.next();
					
					if (!cachedChannel.isActive() && cachedChannel.getInactiveForMillis() > CONNECTION_KEEP_ALIVE_MILLIS) {
						
						logger.debug("closing expired channel ["+cachedChannel.id+"]");
						
						cachedChannel.channel.close();
						cachedChannelIterator.remove();
					}
				}
			}
		}
	}
	
	private void addToCache(final InetSocketAddress createSocketTCP, final Channel channel) {
		synchronized (cache)
		{
			Set<CachedChannel> cachedChannels = cache.get(createSocketTCP); 
			if (cachedChannels==null) {
				cachedChannels = new HashSet<CachedChannel>();
				cache.put(createSocketTCP, cachedChannels);
			}
			CachedChannel cachedChannel = new CachedChannel(channel);
			cachedChannels.add(cachedChannel);
			
			logger.debug("added new cachedChannel "+createSocketTCP+" ["+cachedChannel.id+"]");

		}
	}
	
	private CachedChannel getFreeChannelFromCache(final InetSocketAddress createSocketTCP) {
		synchronized (cache)
		{
			Set<CachedChannel> cachedChannels = cache.get(createSocketTCP); 
			if (cachedChannels==null)
				return null;
			
			for (CachedChannel cachedChannel : cachedChannels)
				if (!cachedChannel.isActive())
					return cachedChannel;
			
			return null;
		}
	}
	
	
}
