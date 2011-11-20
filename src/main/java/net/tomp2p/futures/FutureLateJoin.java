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
 * FutureLateJoin is similar to FutureForkJoin. The main difference is that with
 * this class you don't need to specify all the futures in advance. You can just
 * tell how many futures you expect and add them later on.
 * 
 * @author Thomas Bocek
 * 
 * @param <K>
 */
public class FutureLateJoin<K extends BaseFuture> extends BaseFutureImpl implements BaseFuture
{
	final private int nrMaxFutures;
	final private int minSuccess;
	final private List<K> futuresDone;
	private int successCount = 0;

	/**
	 * Create this future and set the minSuccess to the number of expected
	 * futures.
	 * 
	 * @param nrMaxFutures The number of expected futures.
	 */
	public FutureLateJoin(int nrMaxFutures)
	{
		this(nrMaxFutures, nrMaxFutures);
	}

	/**
	 * Create this future.
	 * 
	 * @param nrMaxFutures The number of expected futures.
	 * @param minSuccess The number of expected successful futures.
	 */
	public FutureLateJoin(int nrMaxFutures, int minSuccess)
	{
		this.nrMaxFutures = nrMaxFutures;
		this.minSuccess = minSuccess;
		this.futuresDone = new ArrayList<K>(nrMaxFutures);
	}

	/**
	 * Add a future when ready. This is why its called FutureLateJoin, since you
	 * can add futures later on.
	 * 
	 * @param future The future to be added.
	 */
	public void add(final K future)
	{
		future.addListener(new BaseFutureAdapter<K>()
		{
			@Override
			public void operationComplete(K future) throws Exception
			{
				boolean done;
				synchronized (lock)
				{
					if (future.isSuccess())
					{
						successCount++;
					}
					done = checkDone(future);
				}
				if (done)
				{
					notifyListerenrs();
				}
			}
		});
	}

	/**
	 * Check if the can set this future to done
	 * 
	 * @param future The latest finished future
	 * @return True if we are done.
	 */
	private boolean checkDone(K future)
	{
		boolean done = false;
		if (!completed)
		{
			futuresDone.add(future);
			if (futuresDone.size() == nrMaxFutures)
			{
				done = setCompletedAndNotify();
				boolean isSuccess = successCount >= minSuccess;
				type = isSuccess ? FutureType.OK : FutureType.FAILED;
				reason = isSuccess ? "All Futures Ok" : "At least one future failed";
			}
		}
		return done;
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
}
