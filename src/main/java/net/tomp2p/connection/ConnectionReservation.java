/*
 * Copyright 2011 Thomas Bocek
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.p2p.Statistics;
import net.tomp2p.utils.Utils;

import org.jboss.netty.channel.ChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The connection pool limits the connection used in the application. If too
 * many connections are open, the application has to wait until a connection is
 * closed.
 * 
 * @author Thomas Bocek
 */
public class ConnectionReservation
{
	final private static Logger logger = LoggerFactory.getLogger(ConnectionReservation.class);
	// semaphore for connections that are created, used only once, then
	// destroyed
	final private Semaphore semaphoreCreating;
	// semaphore for connections that live longer
	final private Semaphore semaphoreOpen;
	final private ChannelFactory tcpClientChannelFactory;
	final private ChannelFactory udpChannelFactory;
	final private MessageLogger messageLoggerFilter;
	final private int maxPermitsCreating;
	final private int maxPermitsOpen;
	final private Statistics statistics;
	// keeps the information about acquired channel and which semaphore has been used
	final private Map<ChannelCreator, Semaphore> activeChannelCreators = new ConcurrentHashMap<ChannelCreator, Semaphore>();
	// used for synchronization
	final private AtomicInteger counter = new AtomicInteger(0);
	private volatile boolean shutdown = false;

	public ConnectionReservation(ChannelFactory tcpClientChannelFactory,
			ChannelFactory udpChannelFactory, ConnectionConfigurationBean configuration,
			MessageLogger messageLoggerFilter, Statistics statistics)
	{
		this.tcpClientChannelFactory = tcpClientChannelFactory;
		this.udpChannelFactory = udpChannelFactory;
		this.maxPermitsCreating = configuration.getMaxCreating();
		this.maxPermitsOpen = configuration.getMaxOpenConnection();
		this.semaphoreCreating = new Semaphore(maxPermitsCreating);
		this.semaphoreOpen = new Semaphore(maxPermitsOpen);
		this.messageLoggerFilter = messageLoggerFilter;
		this.statistics = statistics;
	}

	/**
	 * Reserves connections. If a reservation of 5 connection is made, then 5
	 * parallel connections can be made. Until the connections are released, the
	 * connections can be closed and reopened. The reservation reserves
	 * connections that are created and released immediately.
	 * 
	 * @param permits The number of connections to be reserved
	 * @return The channel creator object that can be used to create the
	 *         channels. Returns null if something went wrong (shutdown,
	 *         interrupt)
	 */
	public ChannelCreator reserve(final int permits)
	{
		return reserve(permits, false);
	}

	/**
	 * Reserves connections. If a reservation of 5 connection is made, then 5
	 * parallel connections can be made. Until the connections are released, the
	 * connections can be closed and reopened.
	 * 
	 * @param permits The number of connections to be reserved
	 * @param keepAliveAndReuse If the connection should stay open (TCP) for
	 *        later reuse.
	 * @return The channel creator object that can be used to create the
	 *         channels. Returns null if something went wrong (shutdown,
	 *         interrupt)
	 */
	public ChannelCreator reserve(final int permits, final boolean keepAliveAndReuse)
	{
//		if (Thread.currentThread().getName().startsWith(ConnectionHandler.THREAD_NAME))
//		{
//			logger.warn("we are blocking in a thread that could cause a deadlock: "
//					+ Thread.currentThread().getName());
//			throw new RuntimeException("[Thread: " + Thread.currentThread().getName() + "] cannot block here");
//		}
		if(counter.incrementAndGet()<0)
		{
			logger.warn("Cannot acquire " + permits + " connections, shutting down");
			return null;
		}
		try
		{	
			boolean acquired = acquire(keepAliveAndReuse ? semaphoreOpen : semaphoreCreating,
					permits);
			if (acquired)
			{
				ChannelCreator channelCreator = new ChannelCreator(permits, statistics,
						messageLoggerFilter, tcpClientChannelFactory, udpChannelFactory,
						keepAliveAndReuse);
				activeChannelCreators.put(channelCreator, keepAliveAndReuse ? semaphoreOpen
						: semaphoreCreating);
				return channelCreator;
			}
			else
			{
				logger.warn("Cannot acquire " + permits + " connections");
				return null;
			}
		}
		finally
		{
			counter.decrementAndGet();
		}
	}

	/**
	 * Acquires connections. Waits in a loop. If a connection is ready, the
	 * wait() gets notified and the acquire() is tried again.
	 * 
	 * @param semaphore The semaphore to get the permits from,
	 * @param permits The number of permits
	 * @return True if the permits could be acquired
	 */
	private boolean acquire(Semaphore semaphore, int permits)
	{
		boolean acquired = false;
		while (!acquired && !shutdown)
		{
			try
			{
				acquired = semaphore.tryAcquire(permits);
				if (!acquired)
				{
					if (logger.isDebugEnabled())
					{
						logger.debug("cannot acquire " + permits + ", in total we have "
								+ maxPermitsCreating + "/" + maxPermitsOpen
								+ ", but now we have "
								+ semaphore.availablePermits());
					}
					synchronized (semaphore)
					{
						semaphore.wait(250);
					}
				}
				else
				{
					if (logger.isDebugEnabled())
					{
						logger.debug("acquired " + permits + ", in total we have "
								+ maxPermitsCreating + "/" + maxPermitsOpen
								+ ", but now we have "
								+ semaphore.availablePermits());
					}
				}
			}
			catch (InterruptedException e)
			{
				return false;
			}
		}
		return acquired;
	}

	/**
	 * Release a channelcreator. The permits will be returned so that they can
	 * be used again.
	 * 
	 * @param channelCreator The channelcreator that is not used anymore (or at
	 *        least partially)
	 * @param permits The number of permits to release
	 */
	public void release(ChannelCreator channelCreator, int permits)
	{
		channelCreator.release(permits);
		Semaphore semaphore = activeChannelCreators.get(channelCreator);
		semaphore.release(permits);
		if (channelCreator.hasNoPermits())
		{
			activeChannelCreators.remove(channelCreator);
			if (logger.isDebugEnabled())
			{
				logger.debug("full release ("+permits+"), we can remove the channelcreator from the list "+semaphore.availablePermits());
			}
		}
		else
		{
			if (logger.isDebugEnabled())
			{
				logger.debug("partial release ("+permits+"), we cannot remove the channelcreator from the list "+semaphore.availablePermits());
			}
		}
		if (logger.isDebugEnabled())
		{
			logger.debug("released " + channelCreator.getPermits() + ", in total we have "
					+ maxPermitsCreating + "/"
					+ maxPermitsOpen + ", now we have " + semaphore.availablePermits());
		}
		synchronized (semaphore)
		{
			semaphore.notifyAll();
		}
	}

	/**
	 * Release a channelcreator. The permits will be returned so that they can
	 * be used again.
	 * 
	 * @param channelCreator. The channelcreator that is not used anymore
	 */
	public void release(ChannelCreator channelCreator)
	{
		release(channelCreator, channelCreator.getPermits());
	}

	/**
	 * Close all open connections and prevent creating new ones.
	 */
	public void shutdown()
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Shutdown");
		}
		// wait until all reserev() calls know that we are shutting down.
		shutdown = true;
		while(counter.compareAndSet(0, Integer.MIN_VALUE))
		{
			synchronized (semaphoreCreating)
			{
				semaphoreCreating.notifyAll();
			}
			synchronized (semaphoreOpen)
			{
				semaphoreOpen.notifyAll();
			}
		}
		//this needs synchronized, otherwise a thread may add to the list a value, then we fail here
		synchronized (activeChannelCreators)
		{
			for (ChannelCreator channelCreator : activeChannelCreators.keySet())
			{
				//this makes them faster to call ConnectionReservation.release
				channelCreator.shutdown();
			}
			//TODO:DBX debug why it hangs here
			while(activeChannelCreators.size()!=0)
			{
				Utils.sleep(500);
			}
		}
		//make sure we wait until all connections are finished.
		
		semaphoreCreating.acquireUninterruptibly(maxPermitsCreating);
		semaphoreOpen.acquireUninterruptibly(maxPermitsOpen);
	}
}