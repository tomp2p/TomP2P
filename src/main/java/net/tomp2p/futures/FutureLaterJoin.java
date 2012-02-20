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

package net.tomp2p.futures;
import java.util.ArrayList;
import java.util.List;

/**
 * FutureLaterJoin is similar to FutureLateJoin. The main difference is that
 * with this class you don't need to specify all the futures in advance and you
 * don't need to specify how many futures you excpect. Once you are done, just
 * call done().
 * 
 * @author Thomas Bocek
 * 
 * @param <K>
 */
public class FutureLaterJoin<K extends BaseFuture> extends BaseFutureImpl implements BaseFuture
{
	final private List<K> futuresDone;
	private int nrMaxFutures = Integer.MAX_VALUE;
	private int minSuccess = Integer.MAX_VALUE;
	private K lastSuceessFuture;
	private int successCount = 0;
	private int futureCount = 0;

	/**
	 * Create this future.
	 */
	public FutureLaterJoin()
	{
		this.futuresDone = new ArrayList<K>(nrMaxFutures);
	}

	/**
	 * Add a future when ready. This is why its called FutureLateJoin, since you
	 * can add futures later on.
	 * 
	 * @param future The future to be added.
	 * @return True if the future was added to the futurelist, false if the
	 *         latejoin future is already finished and the future was not added.
	 */
	public boolean add(final K future)
	{
		synchronized (lock)
		{
			if (completed)
			{
				return false;
			}
			futureCount ++;
			future.addListener(new BaseFutureAdapter<K>()
			{
				@Override
				public void operationComplete(K future) throws Exception
				{
					boolean done = false;
					synchronized (lock)
					{
						if (!completed)
						{
							if (future.isSuccess())
							{
								successCount++;
								lastSuceessFuture = future;
							}
							futuresDone.add(future);
							done = checkDone();
						}
					}
					if (done)
					{
						notifyListerenrs();
					}
				}
			});
			return true;
		}
	}
	
	/**
	 * If no more futures are added, done() must be called to evaluate the
	 * results. If the futures already finished by the time done() is called,
	 * the listeners are called from this thread. Otherwise the notify may be
	 * called from a different thread.
	 */
	public void done()
	{
		done(futureCount);
	}
	
	/**
	 * If no more futures are added, done() must be called to evaluate the
	 * results. If the futures already finished by the time done() is called,
	 * the listeners are called from this thread. Otherwise the notify may be
	 * called from a different thread.
	 * 
	 * @param minSuccess The number of minimum futures that needs to be
	 *        successful to consider this future as a success.
	 */
	public void done(int minSuccess)
	{
		boolean done = false;
		synchronized (lock)
		{
			this.nrMaxFutures = futureCount;
			this.minSuccess = minSuccess;
			done = checkDone();
		}
		if (done)
		{
			notifyListerenrs();
		}
	}

	/**
	 * Check if the can set this future to done
	 * 
	 * @return True if we are done.
	 */
	private boolean checkDone()
	{
		if (futuresDone.size() >= nrMaxFutures || successCount >= minSuccess)
		{
			if(!setCompletedAndNotify())
			{
				return false;
			}
			boolean isSuccess = successCount >= minSuccess;
			type = isSuccess ? FutureType.OK : FutureType.FAILED;
			reason = isSuccess ? "Minimal number of futures received" : 
				"Minimal number of futures *not* received ("+successCount+" of "+minSuccess+" reached)";
			return true;
		}
		return false;
	}

	/**
	 * Returns the finished futures.
	 * 
	 * @return All the futures that are done.
	 */
	public List<K> getFuturesDone()
	{
		synchronized (lock)
		{
			return futuresDone;
		}
	}
	
	/**
	 * @return the last successful finished future.
	 */
	public K getLastSuceessFuture()
	{
		synchronized (lock)
		{
			return lastSuceessFuture;
		}
	}
}
