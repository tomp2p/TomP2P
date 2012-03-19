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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureRunnable;
import net.tomp2p.p2p.Configuration;
import net.tomp2p.p2p.Statistics;

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
	final private Map<ChannelCreator, Semaphore> activeChannelCreators = new HashMap<ChannelCreator, Semaphore>();
	//final private Map<Long, Boolean> threads = new ConcurrentHashMap<Long, Boolean>();
	final private Map<ChannelCreator, StackTraceElement[]> debug = new ConcurrentHashMap<ChannelCreator, StackTraceElement[]>();
	// used for synchronization
	final private AtomicInteger counter = new AtomicInteger(0);
	final private Scheduler scheduler;
	private volatile Reservation reservation = new DefaultReservation();

	public ConnectionReservation(ChannelFactory tcpClientChannelFactory,
			ChannelFactory udpChannelFactory, Configuration configuration,
			MessageLogger messageLoggerFilter, Statistics statistics, Scheduler scheduler)
	{
		this.tcpClientChannelFactory = tcpClientChannelFactory;
		this.udpChannelFactory = udpChannelFactory;
		this.maxPermitsCreating = configuration.getMaxCreating();
		this.maxPermitsOpen = configuration.getMaxOpenConnection();
		this.semaphoreCreating = new Semaphore(maxPermitsCreating);
		this.semaphoreOpen = new Semaphore(maxPermitsOpen);
		this.messageLoggerFilter = messageLoggerFilter;
		this.statistics = statistics;
		this.scheduler = scheduler;
	}
	
	/**
	 * Reserves connections. If a reservation of 5 connection is made, then 5
	 * parallel connections can be made. Until the connections are released, the
	 * connections can be closed and reopened. The reservation reserves
	 * connections that are created and released immediately.
	 * 
	 * @param permits The number of connections to be reserved
	 * @return The future channel creator object that can be used to create the
	 *         channels. Returns null if something went wrong (shutdown,
	 *         interrupt)
	 */
	public FutureChannelCreator reserve(final int permits)
	{
		return reserve(permits, false, "default");
	}

	/**
	 * Reserves connections. If a reservation of 5 connection is made, then 5
	 * parallel connections can be made. Until the connections are released, the
	 * connections can be closed and reopened. The reservation reserves
	 * connections that are created and released immediately.
	 * 
	 * @param permits The number of connections to be reserved
	 * @param name The name of the ChannelCreator, used for easier debugging
	 * @return The future channel creator object that can be used to create the
	 *         channels. Returns null if something went wrong (shutdown,
	 *         interrupt)
	 */
	public FutureChannelCreator reserve(final int permits, final String name)
	{
		return reserve(permits, false, name);
	}
	
	/**
	 * Reserves connections. If a reservation of 5 connection is made, then 5
	 * parallel connections can be made. Until the connections are released, the
	 * connections can be closed and reopened.
	 * 
	 * @param permits The number of connections to be reserved
	 * @param keepAliveAndReuse If the connection should stay open (TCP) for
	 *        later reuse.
	 * @return The future channel creator object that can be used to create the
	 *         channels. Returns null if something went wrong (shutdown,
	 *         interrupt)
	 */
	public FutureChannelCreator reserve(final int permits, final boolean keepAliveAndReuse)
	{
		return reserve(permits, keepAliveAndReuse, "default");
	}

	/**
	 * Reserves connections. If a reservation of 5 connection is made, then 5
	 * parallel connections can be made. Until the connections are released, the
	 * connections can be closed and reopened. Depending on the thread, this may block or not.
	 * 
	 * @param permits The number of connections to be reserved
	 * @param keepAliveAndReuse If the connection should stay open (TCP) for
	 *        later reuse.
	 * @param name The name of the ChannelCreator, used for easier debugging
	 * @return The future channel creator object that can be used to create the
	 *         channels. Returns null if something went wrong (shutdown,
	 *         interrupt)
	 */
	public FutureChannelCreator reserve(final int permits, final boolean keepAliveAndReuse, final String name)
	{
		final FutureChannelCreator futureChannelCreator = new FutureChannelCreator();
		getReservation().runDeadLockProof(scheduler, new FutureRunnable()
		{
			@Override
			public void run()
			{
				ChannelCreator channelCreator = reserve0(permits, keepAliveAndReuse, name);
				if(channelCreator != null)
				{
					futureChannelCreator.reserved(channelCreator);
				}
				else
				{
					futureChannelCreator.setFailed("could not reserve connection, most likely we shutdown");
				}
			}

			@Override
			public void failed(String reason)
			{
				futureChannelCreator.setFailed(reason);
			}
		});
		return futureChannelCreator;
	}
	
	/**
	 * Acquires connections. Waits in a loop. If a connection is ready, the
	 * wait() gets notified and the acquire() is tried again.
	 * 
	 * @param semaphore The semaphore to get the permits from,
	 * @param permits The number of permits
	 * @return True if the permits could be acquired
	 */
	private ChannelCreator reserve0(final int permits, final boolean keepAliveAndReuse, String name)
	{
		if(counter.incrementAndGet()<0)
		{
			logger.warn("Cannot acquire " + permits + " connections, shutting down");
			return null;
		}
		try
		{	
			getReservation().prepareDeadLockCheck();
			boolean acquired = getReservation().acquire(keepAliveAndReuse ? semaphoreOpen : semaphoreCreating,
					permits);
			if (acquired)
			{
				ChannelCreator channelCreator = new ChannelCreator(permits, statistics,
						messageLoggerFilter, tcpClientChannelFactory, udpChannelFactory,
						keepAliveAndReuse, name, Thread.currentThread().getId(), scheduler);
				if(logger.isDebugEnabled())
				{
					logger.debug("created channels for Thread "+ Thread.currentThread().getName()+"/"+Thread.currentThread().getId());
					//we do a bit of debugging here, since this is a dangerous place here that may deadlock upon shutdown
					debug.put(channelCreator, Thread.currentThread().getStackTrace());
				}
				synchronized (activeChannelCreators)
				{
					activeChannelCreators.put(channelCreator, keepAliveAndReuse ? semaphoreOpen
							: semaphoreCreating);
				}
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
	 * Release a channelcreator. The permits will be returned so that they can
	 * be used again.
	 * 
	 * @param channelCreator The channelcreator that is not used anymore (or at
	 *        least partially)
	 * @param permits The number of permits to release
	 */
	public void release(ChannelCreator channelCreator, int permits)
	{
		final Semaphore semaphore;
		final boolean hasNoPermits;
		// needs to be in a sync block, othrewise we see a remove first, then a
		// get which returns null -> bang null pointer
		synchronized (activeChannelCreators)
		{
			hasNoPermits = channelCreator.release(permits);
			if (hasNoPermits)
			{
				semaphore = activeChannelCreators.remove(channelCreator);
				activeChannelCreators.notifyAll();
			}
			else
			{
				semaphore = activeChannelCreators.get(channelCreator);
			}
		}
		semaphore.release(permits);
		synchronized (semaphore)
		{
			semaphore.notifyAll();
		}
		if (hasNoPermits)
		{
			getReservation().removeDeadLockCheck(channelCreator.getCreatorThread());
			if (logger.isDebugEnabled())
			{
				logger.debug("full release ("+permits+"), we can remove the channelcreator from the list "+semaphore.availablePermits()+", which was created from thread: " + channelCreator.getCreatorThread() +" / " + channelCreator.getName());
				debug.remove(channelCreator);
			}
		}
		else
		{
			if (logger.isDebugEnabled())
			{
				logger.debug("partial release ("+permits+"), we cannot remove the channelcreator from the list "+semaphore.availablePermits()+", which was created from thread: "+channelCreator.getCreatorThread());
			}
		}
		if (logger.isDebugEnabled())
		{
			logger.debug("released " + channelCreator.getPermits() + ", in total we have "
					+ maxPermitsCreating + "/"
					+ maxPermitsOpen + ", now we have " + semaphore.availablePermits());
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
	 * Close all open connections and prevent creating new ones. This operations
	 * blocks until everything is finished
	 */
	public void shutdown()
	{
		scheduler.shutdown();
		if (logger.isDebugEnabled())
		{
			logger.debug("Shutdown");
		}
		// wait until all reserve() calls know that we are shutting down.
		getReservation().shutdown();
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
		Collection<ChannelCreator> allCreators;
		synchronized (activeChannelCreators)
		{
			allCreators = new ArrayList<ChannelCreator>(activeChannelCreators.size());
			allCreators.addAll(activeChannelCreators.keySet());
		}
		for (ChannelCreator channelCreator : allCreators)
		{
			//this makes them faster to call ConnectionReservation.release
			channelCreator.shutdown();
		}
		synchronized (activeChannelCreators)
		{
			while(activeChannelCreators.size() != 0)
			{
				try
				{
					activeChannelCreators.wait(500);
				}
				catch (InterruptedException e)
				{
					//ignore
					Thread.currentThread().interrupt();
				}
			}
		}
		//make sure we wait until all connections are finished.
		semaphoreCreating.acquireUninterruptibly(maxPermitsCreating);
		semaphoreOpen.acquireUninterruptibly(maxPermitsOpen);
	}

	public Reservation getReservation()
	{
		return reservation;
	}

	public void setReservation(Reservation reservation)
	{
		this.reservation = reservation;
	}
}