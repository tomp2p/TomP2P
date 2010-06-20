package net.tomp2p.futures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class FutureLateJoin<K extends BaseFuture> extends BaseFutureImpl implements BaseFuture
{
	final int nrMaxFutures;
	final List<K> futures = new ArrayList<K>();
	final AtomicBoolean success = new AtomicBoolean(true);

	public FutureLateJoin(int nrMaxFutures)
	{
		this.nrMaxFutures = nrMaxFutures;
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
					success.set(false);
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
					if (future.isFailed())
						success.set(false);
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
			futures.add(future);
			if (futures.size() == nrMaxFutures)
			{
				done = setCompletedAndNotify();
				//in this future, this cannot be false because we check with iscompleted before
				assert(done);
				type = success.get() ? FutureType.OK : FutureType.FAILED;
				reason = success.get() ? "All Futures Ok" : "At least one future failed";
			}
		}
		return done;
	}
	
	public List<K> getFutures()
	{
		synchronized (lock)
		{
			return futures;
		}
	}
}
