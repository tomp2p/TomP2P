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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultReservation implements Reservation
{
	final private static Logger logger = LoggerFactory.getLogger(DefaultReservation.class);
	private volatile boolean shutdown = false;
	
	/* (non-Javadoc)
	 * @see net.tomp2p.connection.Reservation#shutdown()
	 */
	@Override
	public void shutdown()
	{
		shutdown = true;
	}
	/* (non-Javadoc)
	 * @see net.tomp2p.connection.Reservation#acquire(java.util.concurrent.Semaphore, int)
	 */
	@Override
	public boolean acquire(Semaphore semaphore, int permits)
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
						logger.debug("cannot acquire " + permits + ", now we have "
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
						logger.debug("acquired " + permits + ", now we have "
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
}
