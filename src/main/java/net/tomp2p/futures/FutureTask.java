package net.tomp2p.futures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.tomp2p.mapreduce.FutureAsyncTask;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class FutureTask extends BaseFutureImpl
{
	final private List<FutureAsyncTask> requests = new ArrayList<FutureAsyncTask>();
	final private Map<PeerAddress, Map<Number160, Data>> dataMap = new HashMap<PeerAddress, Map<Number160,Data>>();
	private int resultSize = 0;
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

	public void setDone()
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
			{
				return;
			}
			this.type = resultSize > 0 ? FutureType.OK: FutureType.FAILED;
		}
		notifyListerenrs();
	}

	public void setProgress(FutureAsyncTask futureAsyncTask)
	{
		synchronized (lock)
		{
			resultSize += futureAsyncTask.getDataMap().size();
			PeerAddress peerAddress = futureAsyncTask.getRemotePeer();
			Map<Number160, Data> tmp = dataMap.get(peerAddress);
			if(tmp == null)
			{
				tmp = new HashMap<Number160, Data>();
				dataMap.put(peerAddress, tmp);
			}
			tmp.putAll(futureAsyncTask.getDataMap());
		}
	}
}