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

package net.tomp2p.futures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

/**
 * This future keeps track of one or more tasks send to remote peers.
 * 
 * @author Thomas Bocek
 */
public class FutureTask extends BaseFutureImpl
{
	final private List<FutureAsyncTask> requests = new ArrayList<FutureAsyncTask>();
	final private Map<PeerAddress, Map<Number160, Data>> dataMap = new HashMap<PeerAddress, Map<Number160,Data>>();
	private int resultSuccess = 0;
	
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

	/**
	 * Finishes the future. Set the future to success if at least one of the
	 * future was a success.
	 */
	public void setDone()
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
			{
				return;
			}
			this.type = resultSuccess > 0 ? FutureType.OK: FutureType.FAILED;
		}
		notifyListerenrs();
	}

	/**
	 * This is called for intermediate results. Whenever a
	 * {@link FutureAsyncTask} is ready, update the result data.
	 * 
	 * @param futureAsyncTask The future that has finished
	 */
	public void setProgress(FutureAsyncTask futureAsyncTask)
	{
		synchronized (lock)
		{
			if(futureAsyncTask.isSuccess())
			{
				resultSuccess++;
			}
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