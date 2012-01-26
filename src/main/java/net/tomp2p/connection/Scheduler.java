/*
 * Copyright 2012 Thomas Bocek
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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.FutureRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scheduler
{
	private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
	private static final int NR_THREADS = Runtime.getRuntime().availableProcessors() + 1;
	private static final int WARNING_THRESHOLD = 10000;
	private final LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
	private final ExecutorService executor = new ThreadPoolExecutor(NR_THREADS, NR_THREADS, 0L,
			TimeUnit.MILLISECONDS, queue, new MyThreadFactory());

	public void addQueue(FutureRunnable futureRunnable)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("we are called from a TCP netty thread, so send this in an other thread "
					+ Thread.currentThread().getName()+". The queue size is: "+queue.size());
		}
		if(queue.size() > WARNING_THRESHOLD)
		{
			if(logger.isInfoEnabled())
			{
				logger.info("slow down, we have a huge backlog!");
			}
		}
		if(executor.isShutdown())
		{
			futureRunnable.failed("shutting down");
			return;
		}
		executor.execute(futureRunnable);
	}
	
	public void shutdown()
	{
		List<Runnable> runners = executor.shutdownNow();
		for (Runnable runner : runners)
		{
			FutureRunnable futureRunnable = (FutureRunnable) runner;
			futureRunnable.failed("Shutting down...");
		}
	}
	
	private class MyThreadFactory implements ThreadFactory 
	{
		private int nr = 0;
		public Thread newThread(Runnable r) 
		{
		     Thread t = new Thread(r);
		     t.setName("scheduler-"+nr);
		     nr++;
		     return t;
		 }
	}
}
