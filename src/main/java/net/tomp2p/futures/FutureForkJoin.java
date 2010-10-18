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

public class FutureForkJoin<K extends BaseFuture> extends BaseFutureImpl implements BaseFuture
{
	private final K[] forks;
	private final int nrFutures;
	private final int nrFinishFuturesSuccess;
	private final boolean cancelFuturesOnFinish;
	// all these values are accessed within synchronized blocks
	private K last;
	private final List<K> intermediateFutures;
	private int counter = 0;
	private int successCounter = 0;
	volatile private boolean completedJoin = false;

	public FutureForkJoin(K... forks)
	{
		this(forks.length, false, forks);
	}

	/**
	 * 
	 * @param futureNr Is the number of non-null futures. Its fail fast.
	 * @param nrFinishFuturesSuccess
	 * @param forks
	 */
	public FutureForkJoin(int nrFinishFuturesSuccess, boolean cancelFuturesOnFinish, K... forks)
	{
		this.nrFinishFuturesSuccess = nrFinishFuturesSuccess;
		this.forks = forks;
		this.cancelFuturesOnFinish = cancelFuturesOnFinish;
		// the futures array may have null entries, so count first.
		nrFutures = forks.length;
		intermediateFutures = new ArrayList<K>(this.nrFutures - 1);
		if (this.nrFutures <= 0)
			setFailed("We have no futures: " + this.nrFutures);
		else
			join();
	}

	private void join()
	{
		final int len = forks.length;
		for (int i = 0; i < len; i++)
		{
			if (completedJoin)
				return;
			final int index = i;
			if (forks[index] != null)
			{
				forks[index].addListener(new BaseFutureAdapter<K>()
				{
					@Override
					public void operationComplete(final K future) throws Exception
					{
						evaluate(future, index);
					}
				});
			}
			else
			{
				boolean notifyNow = false;
				synchronized (lock)
				{
					if (completed)
						return;
					if (++counter >= nrFutures)
						notifyNow = setFinish(null, FutureType.FAILED);
				}
				if (notifyNow)
				{
					notifyListerenrs();
					cancelAll();
					return;
				}
			}
		}
	}

	private void evaluate(K finished, int index)
	{
		boolean notifyNow = false;
		synchronized (lock)
		{
			if (completed)
				return;
			forks[index] = null;
			if (finished.isSuccess() && ++successCounter >= nrFinishFuturesSuccess)
				notifyNow = setFinish(finished, FutureType.OK);
			else if (++counter >= nrFutures)
				notifyNow = setFinish(finished, FutureType.FAILED);
			else
				intermediateFutures.add(finished);
		}
		if (notifyNow)
		{
			notifyListerenrs();
			cancelAll();
		}
	}

	private void cancelAll()
	{
		if (cancelFuturesOnFinish)
		{
			for (K future : forks)
			{
				if (future != null)
					future.cancel();
			}
		}
	}

	private boolean setFinish(K last, FutureType type)
	{
		if (!setCompletedAndNotify())
			return false;
		this.completedJoin = true;
		this.last = last;
		this.type = type;
		return true;
	}

	@Override
	public String getFailedReason()
	{
		synchronized (lock)
		{
			StringBuilder sb = new StringBuilder("FutureMulti:").append(reason).append(", type:")
					.append(type);
			if (last != null)
				sb.append(", last:").append(last.getFailedReason()).append("rest:");
			for (K k : getAll())
			{
				if (k != null)
					sb.append(",").append(k.getFailedReason());
			}
			return sb.toString();
		}
	}

	public K getLast()
	{
		synchronized (lock)
		{
			return last;
		}
	}

	/*
	 * public int getLastIndex() { synchronized (lock) { return lastIndex; } }
	 */
	/**
	 * This is only safe to access is this future has completed!
	 * 
	 * @return The list of finished futures indexes before complete has been
	 *         called. This does not include the last future index, which can be
	 *         accessed using getLast().
	 */
	public List<K> getIntermediateFutures()
	{
		synchronized (lock)
		{
			if (!completed)
				throw new RuntimeException(
						"Cannot access intermediateIndexes if fork/join not finished");
			return intermediateFutures;
		}
	}

	public K[] getAll()
	{
		synchronized (lock)
		{
			return forks;
		}
	}

	public int getSuccessCounter()
	{
		synchronized (lock)
		{
			return successCounter;
		}
	}
}
