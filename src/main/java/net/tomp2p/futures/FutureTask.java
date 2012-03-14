package net.tomp2p.futures;

import java.util.ArrayList;
import java.util.List;

import net.tomp2p.mapreduce.FutureAsyncTask;

public class FutureTask extends BaseFutureImpl
{
	final private List<FutureAsyncTask> requests = new ArrayList<FutureAsyncTask>();

	/**
	 * Adds all requests that have been created for the DHT operations. Those
	 * were created after the routing process.
	 * 
	 * @param futureResponse The futurRepsonse that has been created
	 */
	public void addRequests(FutureAsyncTask futureResponse)
	{
		synchronized (lock)
		{
			requests.add(futureResponse);
		}
	}
}
