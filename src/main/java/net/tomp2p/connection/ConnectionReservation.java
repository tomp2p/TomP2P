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
	final private Semaphore semaphore;
	final private static Logger logger = LoggerFactory.getLogger(ConnectionReservation.class);
	final private ChannelFactory tcpClientChannelFactory;
	final private ChannelFactory udpChannelFactory;
	final private MessageLogger messageLoggerFilter;
	final private int maxPermits;

	public ConnectionReservation(ChannelFactory tcpClientChannelFactory,
			ChannelFactory udpChannelFactory, ConnectionConfiguration configuration,
			//ExecutionHandler executionHandlerSender,
			 MessageLogger messageLoggerFilter)
	{
		this.tcpClientChannelFactory = tcpClientChannelFactory;
		this.udpChannelFactory = udpChannelFactory;
		this.maxPermits = configuration.getMaxOutgoingTCP() + configuration.getMaxOutgoingUDP();
		this.semaphore = new Semaphore(maxPermits);
		this.messageLoggerFilter = messageLoggerFilter;
	}
	
	public ChannelCreator reserve(final int permits) throws ChannelException
	{
		if(shutdown.get())
			return null;
		if (Thread.currentThread().getName().startsWith(ConnectionHandler.THREAD_NAME))
		{
			logger.warn("we are blocking in a thread that could cause a deadlock: " + Thread.currentThread().getName());
			throw new RuntimeException("cannot block here");
		}
		boolean acquired = false;
		while(!acquired && !shutdown.get())
		{
			acquired=semaphore.tryAcquire(permits);
			if (!acquired)
			{
				logger.warn("cannot acquire "+ permits+", in total we have "+maxPermits+", but now we have "+semaphore.availablePermits());
				synchronized (semaphore)
				{
					try {
						semaphore.wait(250);
					} catch (InterruptedException e) {
						// maybe we need to exit due to shutdown
					}
				}
			}	
		}
		if(shutdown.get()) {
			if(acquired) {
				semaphore.release(permits);
			}
			return null;
		}
		//by now we have acquired the permits
		ChannelCreator channelCreator = new ChannelCreator(channelsTCP, channelsUDP, permits, messageLoggerFilter, tcpClientChannelFactory, udpChannelFactory, shutdown, this);
		return channelCreator;
	}
	
	public void release(int permits)
	{
		semaphore.release(permits);
		synchronized (semaphore)
		{
			semaphore.notifyAll();
		}
	}

	/**
	 * Close all open connections and prevent creating new ones.
	 */
	public void shutdown()
	{
		shutdown.set(true);
		synchronized (semaphore)
		{
			semaphore.notifyAll();
		}
		channelsTCP.close().awaitUninterruptibly();
		channelsUDP.close().awaitUninterruptibly();
	}
}
