package net.tomp2p.mapreduce;

import java.util.Map;

import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class FutureAsyncTask extends BaseFutureImpl
{
	private final PeerAddress remotePeer;
	private Map<Number160, Data> dataMap;
	
	public FutureAsyncTask(PeerAddress remotePeer)
	{
		this.remotePeer = remotePeer;
	}
	
	public void setDataMap(Map<Number160, Data> dataMap)
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
			{
				return;
			}
			this.dataMap = dataMap;
			this.type = FutureType.OK;
		}
		notifyListerenrs();
	}
	
	public Map<Number160, Data> getDataMap()
	{
		synchronized (lock)
		{
			return dataMap;
		}
	}

	public PeerAddress getRemotePeer()
	{
		return remotePeer;
	}
}