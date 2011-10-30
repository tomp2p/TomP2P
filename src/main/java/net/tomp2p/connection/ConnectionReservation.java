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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
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
	private AtomicBoolean shutdown = new AtomicBoolean(false);
	final private ChannelGroup channelsTCP = new DefaultChannelGroup("TomP2P ConnectionPool TCP");
	final private ChannelGroup channelsUDP = new DefaultChannelGroup("TomP2P ConnectionPool UDP");
	final private Semaphore semaphoreCreating;
	final private Semaphore semaphoreOpen;
	final private static Logger logger = LoggerFactory.getLogger(ConnectionReservation.class);
	final private ChannelFactory tcpClientChannelFactory;
	final private ChannelFactory udpChannelFactory;
	final private MessageLogger messageLoggerFilter;
	final private int maxPermitsCreating;
	final private int maxPermitsOpen;
	final private ConcurrentMap<PeerConnection, Boolean> permanentConnections = new ConcurrentHashMap<PeerConnection, Boolean>();

	public ConnectionReservation(ChannelFactory tcpClientChannelFactory,
			ChannelFactory udpChannelFactory, ConnectionConfiguration configuration,
			//ExecutionHandler executionHandlerSender,
			 MessageLogger messageLoggerFilter)
	{
		this.tcpClientChannelFactory = tcpClientChannelFactory;
		this.udpChannelFactory = udpChannelFactory;
		this.maxPermitsCreating = configuration.getMaxCreating();
		this.maxPermitsOpen = configuration.getMaxOpenConnection();
		this.semaphoreCreating = new Semaphore(maxPermitsCreating);
		this.semaphoreOpen = new Semaphore(maxPermitsOpen);
		//this.semaphoreCreate = new Semaphore(20);
		this.messageLoggerFilter = messageLoggerFilter;
	}
	
	public ChannelCreator reserve(final int permits) throws ChannelException
	{
		return reserve(permits, false);
	}
	
	public ChannelCreator reserve(final int permits, final boolean keepAliveAndReuse) throws ChannelException
	{
		if(shutdown.get())
			return null;
		if (Thread.currentThread().getName().startsWith(ConnectionHandler.THREAD_NAME))
		{
			logger.warn("we are blocking in a thread that could cause a deadlock: " + Thread.currentThread().getName());
			throw new RuntimeException("cannot block here");
		}
		boolean acquired = false;
		//int counter = 0;
		while(!acquired && !shutdown.get())
		{
			acquired=semaphoreCreating.tryAcquire(permits) && semaphoreOpen.tryAcquire(permits);
			if (!acquired)
			{
				if(logger.isDebugEnabled())
				{
					logger.debug("cannot acquire "+ permits+", in total we have "+maxPermitsCreating+"/"+maxPermitsOpen+", but now we have "+semaphoreCreating.availablePermits());
				}
				/*if(counter % 40 ==0)
				{
					logger.warn("cannot acquire "+ permits+" for 10sec, in total we have "+maxPermitsCreating+"/"+maxPermitsOpen+", but now we have "+semaphoreCreating.availablePermits());
				}*/
				synchronized (semaphoreCreating)
				{
					try 
					{
						semaphoreCreating.wait(250);
						//counter++;
					} 
					catch (InterruptedException e) 
					{
						// maybe we need to exit due to shutdown
					}
				}
			}	
		}
		if(shutdown.get()) 
		{
			if(acquired) 
			{
				semaphoreCreating.release(permits);
				semaphoreOpen.release(permits);
			}
			return null;
		}
		//by now we have acquired the permits
		ChannelCreator channelCreator = new ChannelCreator(channelsTCP, channelsUDP, permits, messageLoggerFilter, tcpClientChannelFactory, udpChannelFactory, shutdown, this, keepAliveAndReuse);
		return channelCreator;
	}
	
	public void releaseCreating(int permits)
	{
		semaphoreCreating.release(permits);
		synchronized (semaphoreCreating)
		{
			semaphoreCreating.notifyAll();
		}
	}
	
	public void releaseOpen(int permits)
	{
		semaphoreOpen.release(permits);
	}
	
	public void release(int permits)
	{
		semaphoreCreating.release(permits);
		semaphoreOpen.release(permits);
		if(logger.isDebugEnabled())
		{
			logger.debug("released "+ permits+", in total we have "+maxPermitsCreating+"/"+maxPermitsOpen+", now we have "+semaphoreCreating.availablePermits());
		}
		synchronized (semaphoreCreating)
		{
			semaphoreCreating.notifyAll();
		}
	}

	/**
	 * Close all open connections and prevent creating new ones.
	 */
	public void shutdown()
	{
		shutdown.set(true);
		for(PeerConnection peerConnection:permanentConnections.keySet())
		{
			peerConnection.close();
		}
		synchronized (semaphoreCreating)
		{
			//when we shutdown, we dont care about the connection count.
			semaphoreCreating.notifyAll();
		}
		//futureGroup.awaitUninterruptibly();
		channelsTCP.close().awaitUninterruptibly();
		channelsUDP.close().awaitUninterruptibly();
	}

	public void addPeerConnection(PeerConnection peerConnection) 
	{
		permanentConnections.put(peerConnection, Boolean.TRUE);	
	}
	
	public void removePeerConnection(PeerConnection peerConnection) 
	{
		permanentConnections.remove(peerConnection);
	}
	
}