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

import net.tomp2p.connection.ConnectionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base for all BaseFuture implementations. Be aware of possible deadlocks.
 * Never await from a listener. This class is heavily inspired by MINA and
 * Netty.
 * 
 * @author Thomas Bocek
 * 
 */
public abstract class BaseFutureImpl implements BaseFuture
{
	final private static Logger logger = LoggerFactory.getLogger(BaseFutureImpl.class);
	// Listeners that gets notified if the future finished
	final private List<BaseFutureListener<? extends BaseFuture>> listeners =
			new ArrayList<BaseFutureListener<? extends BaseFuture>>(1);
	// While a future is running, the process may add cancellations for faster
	// cancel operations, e.g. cancel connection attempt
	final protected List<Cancellable> cancellables = new ArrayList<Cancellable>(1);
	final protected Object lock;
	// set the ready flag if operation completed
	protected boolean completed = false;
	// by default false, change in case of success. An unfinished operation is
	// always set to failed
	protected FutureType type = FutureType.INIT;
	protected String reason = "unknown";

	public BaseFutureImpl()
	{
		this.lock = this;
	}

	@Override
	public BaseFuture await() throws InterruptedException
	{
		synchronized (lock)
		{
			checkDeadlock();
			while (!completed)
			{
				lock.wait();
			}
			return this;
		}
	}

	@Override
	public BaseFuture awaitUninterruptibly()
	{
		synchronized (lock)
		{
			checkDeadlock();
			while (!completed)
			{
				try
				{
					lock.wait();
				}
				catch (final InterruptedException e)
				{}
			}
			return this;
		}
	}

	@Override
	public boolean await(final long timeoutMillis) throws InterruptedException
	{
		return await0(timeoutMillis, true);
	}

	@Override
	public boolean awaitUninterruptibly(final long timeoutMillis)
	{
		try
		{
			return await0(timeoutMillis, false);
		}
		catch (final InterruptedException e)
		{
			throw new RuntimeException("This should never ever happen.");
		}
	}

	/**
	 * Internal await operation that also checks for potential deadlocks.
	 * 
	 * @param timeoutMillis The time to wait
	 * @param interrupt Flag to indicate if the method can throw an
	 *        InterruptedException
	 * @return True if this future has finished in timeoutMillis time, false
	 *         otherwise
	 * @throws InterruptedException If the flag interrupt is true and this
	 *         thread has been interrupted.
	 */
	private boolean await0(final long timeoutMillis, final boolean interrupt)
			throws InterruptedException
	{
		final long startTime = (timeoutMillis <= 0) ? 0 : System.currentTimeMillis();
		long waitTime = timeoutMillis;
		synchronized (lock)
		{
			if (completed)
			{
				return completed;
			}
			else if (waitTime <= 0)
			{
				return completed;
			}
			checkDeadlock();
			while (true)
			{
				try
				{
					lock.wait(waitTime);
				}
				catch (final InterruptedException e)
				{
					if (interrupt)
					{
						throw e;
					}
				}
				if (completed)
				{
					return true;
				}
				else
				{
					waitTime = timeoutMillis - (System.currentTimeMillis() - startTime);
					if (waitTime <= 0)
					{
						return completed;
					}
				}
			}
		}
	}

	@Override
	public boolean isCompleted()
	{
		synchronized (lock)
		{
			return completed;
		}
	}

	@Override
	public boolean isSuccess()
	{
		synchronized (lock)
		{
			return completed && (type == FutureType.OK);
		}
	}

	@Override
	public boolean isFailed()
	{
		synchronized (lock)
		{
			// failed means failed or canceled
			return completed && (type != FutureType.OK);
		}
	}

	@Override
	public void setFailed(final String reason)
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
			{
				return;
			}
			if (logger.isWarnEnabled())
			{
				logger.warn("set failed reason: " + reason);
			}
			this.reason = reason;
			this.type = FutureType.FAILED;
		}
		notifyListerenrs();
	}

	@Override
	public String getFailedReason()
	{
		StringBuffer sb = new StringBuffer("BaseFuture Status=(isComplete:");
		synchronized (lock)
		{
			sb.append(completed);
			sb.append(", reason:");
			sb.append(reason);
			sb.append(", type:");
			sb.append(type);
			sb.append(")");
			return sb.toString();
		}
	}

	@Override
	public FutureType getType()
	{
		synchronized (lock)
		{
			return type;
		}
	}

	/**
	 * Make sure that the calling method has synchronized (lock)
	 * 
	 * @return True if notified. It will notify if completed is not set yet.
	 */
	protected boolean setCompletedAndNotify()
	{
		if (!completed)
		{
			completed = true;
			lock.notifyAll();
			return true;
		}
		else
		{
			return false;
		}
	}

	@Override
	public BaseFuture addListener(final BaseFutureListener<? extends BaseFuture> listener)
	{
		boolean notifyNow = false;
		synchronized (lock)
		{
			if (completed)
			{
				notifyNow = true;
			}
			else
			{
				listeners.add(listener);
			}
		}
		// called only once
		if (notifyNow)
		{
			callOperationComplete(listener);
		}
		return this;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void callOperationComplete(final BaseFutureListener listener)
	{
		try
		{
			listener.operationComplete(this);
		}
		catch (final Exception e)
		{
			try
			{
				listener.exceptionCaught(e);
			}
			catch (final Exception e1)
			{
				if (logger.isErrorEnabled())
				{
					logger.error("Unexcpected exception in exceptionCaught()", e1);
				}
				e1.printStackTrace();
			}
		}
	}

	/**
	 * If we block from a Netty thread, then the Netty thread won't receive any
	 * IO operations, thus make the user aware of this situation.
	 */
	private void checkDeadlock()
	{
		String currentName = Thread.currentThread().getName();
		if (currentName.startsWith(ConnectionHandler.THREAD_NAME))
		{
			throw new IllegalStateException("await*() in Netty I/O thread causes a dead lock or "
					+ "sudden performance drop. Use addListener() instead or "
					+ "call await*() from a different thread.");
		}
	}

	/**
	 * Always call this from outside synchronized(lock)!
	 */
	protected void notifyListerenrs()
	{
		// if this is synchronized, it will deadlock, so do not lock this!
		// There won't be any visibility problem or concurrent modification
		// because 'ready' flag will be checked against both addListener and
		// removeListener calls.
		for (final BaseFutureListener<? extends BaseFuture> listener : listeners)
		{
			callOperationComplete(listener);
		}
		listeners.clear();
		cancellables.clear();
		// all events are one time events. It cannot happen that you get
		// notified twice
	}

	@Override
	public BaseFuture removeListener(final BaseFutureListener<? extends BaseFuture> listener)
	{
		synchronized (lock)
		{
			if (!completed)
			{
				listeners.remove(listener);
			}
		}
		return this;
	}

	@Override
	public BaseFuture addCancellation(final Cancellable cancellable)
	{
		synchronized (lock)
		{
			if (!completed)
			{
				cancellables.add(cancellable);
			}
		}
		return this;
	}

	@Override
	public BaseFuture removeCancellation(final Cancellable cancellable)
	{
		synchronized (lock)
		{
			if (!completed)
			{
				cancellables.remove(cancellable);
			}
		}
		return this;
	}

	@Override
	public void cancel()
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
			{
				return;
			}
			this.type = FutureType.CANCEL;
			this.reason = "canceled";
		}
		// only run once
		for (final Cancellable cancellable : cancellables)
		{
			cancellable.cancel();
		}
		notifyListerenrs();
	}
}