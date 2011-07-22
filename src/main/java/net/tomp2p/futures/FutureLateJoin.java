package net.tomp2p.futures;
import java.util.ArrayList;
import java.util.List;

public class FutureLateJoin<K extends BaseFuture> extends BaseFutureImpl implements BaseFuture
{
	final int nrMaxFutures;
	final int minSuccess;
	final List<K> futuresDone;
	private int successCount = 0;
	
	public FutureLateJoin(int nrMaxFutures)
	{
		this(nrMaxFutures, 0);
	}

	public FutureLateJoin(int nrMaxFutures, int minSuccess)
	{
		this.nrMaxFutures = nrMaxFutures;
		this.minSuccess = minSuccess;
		this.futuresDone = new ArrayList<K>(nrMaxFutures);
	}

	public void add(final K future)
	{
		future.addListener(new BaseFutureListener<K>()
		{
			@Override
			public void exceptionCaught(Throwable t) throws Exception
			{
				boolean done;
				synchronized (lock)
				{
					done = checkDone(future);
				}
				if (done) 
					notifyListerenrs();
			}

			@Override
			public void operationComplete(K future) throws Exception
			{
				boolean done;
				synchronized (lock)
				{
					if (future.isSuccess())
						successCount++;
					done = checkDone(future);
				}
				if (done)
					notifyListerenrs();
			}
		});
	}

	private boolean checkDone(K future)
	{
		boolean done = false;
		if (!completed)
		{
			futuresDone.add(future);
			if (futuresDone.size() == nrMaxFutures)
			{
				done = setCompletedAndNotify();
				boolean isSuccess = nrMaxFutures >= successCount;
				type = isSuccess ? FutureType.OK : FutureType.FAILED;
				reason = isSuccess ? "All Futures Ok" : "At least one future failed";
			}
		}
		return done;
	}
	
	public List<K> getFuturesDone()
	{
		synchronized (lock)
		{
			return futuresDone;
		}
	}
}
