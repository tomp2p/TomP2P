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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import net.tomp2p.p2p.EvaluatingSchemeTracker;
import net.tomp2p.p2p.VotingSchemeTracker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;


public class FutureTracker extends BaseFutureImpl
{
	final private EvaluatingSchemeTracker evaluatingSchemeTracker;
	final private SimpleBloomFilter<Number160> knownPeers;
	private volatile FutureCreate<BaseFuture> futureCreate;
	private Set<PeerAddress> potentialTrackers;
	private Set<PeerAddress> directTrackers;
	private Map<PeerAddress, Map<PeerAddress, Data>> peersOnTracker;
	//
	private ScheduledFuture<?> scheduledFuture;
	private List<ScheduledFuture<?>> scheduledFutures;
	private boolean cancelSchedule = false;

	public FutureTracker()
	{
		this(new VotingSchemeTracker(), null);
	}

	public FutureTracker(EvaluatingSchemeTracker evaluatingSchemeTracker, SimpleBloomFilter<Number160> knownPeers)
	{
		this.evaluatingSchemeTracker = evaluatingSchemeTracker;
		this.knownPeers=knownPeers;
	}

	public void setFutureCreate(FutureCreate<BaseFuture> futureCreate)
	{
		if (futureCreate == null)
			return;
		this.futureCreate = futureCreate;
	}

	public void repeated(BaseFuture future)
	{
		if (futureCreate != null)
			futureCreate.repeated(future);
	}

	public void setTrackers(Set<PeerAddress> potentialTrackers, Set<PeerAddress> directTrackers,
			Map<PeerAddress, Map<PeerAddress, Data>> peersOnTracker)
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
				return;
			this.potentialTrackers = potentialTrackers;
			this.directTrackers = directTrackers;
			this.peersOnTracker = peersOnTracker;
			this.type = ((potentialTrackers.size() == 0) && (directTrackers.size() == 0))
					? BaseFuture.FutureType.FAILED : BaseFuture.FutureType.OK;
		}
		notifyListerenrs();
	}

	public Set<PeerAddress> getPotentialTrackers()
	{
		synchronized (lock)
		{
			return potentialTrackers;
		}
	}

	public Set<PeerAddress> getDirectTrackers()
	{
		synchronized (lock)
		{
			return directTrackers;
		}
	}

	public Map<PeerAddress, Map<PeerAddress, Data>> getRawPeersOnTracker()
	{
		synchronized (lock)
		{
			return peersOnTracker;
		}
	}

	public Set<PeerAddress> getPeersOnTracker()
	{
		synchronized (lock)
		{
			return peersOnTracker.keySet();
		}
	}

	public Map<PeerAddress, Data> getTrackers()
	{
		synchronized (lock)
		{
			return evaluatingSchemeTracker.evaluateSingle(peersOnTracker);
		}
	}

	public Map<PeerAddress, Set<Data>> getCumulativeTrackers()
	{
		synchronized (lock)
		{
			return evaluatingSchemeTracker.evaluate(peersOnTracker);
		}
	}
	
	public SimpleBloomFilter<Number160> getKnownPeers()
	{
		synchronized (lock)
		{
			return knownPeers;
		}
	}

	public void setScheduledFuture(ScheduledFuture<?> scheduledFuture,
			List<ScheduledFuture<?>> scheduledFutures)
	{
		synchronized (lock)
		{
			this.scheduledFuture = scheduledFuture;
			this.scheduledFutures = scheduledFutures;
			if (cancelSchedule == true)
				cancel();
		}
	}

	@Override
	public void cancel()
	{
		synchronized (lock)
		{
			cancelSchedule = true;
			if (scheduledFuture != null)
				scheduledFuture.cancel(false);
			if (scheduledFutures != null)
				scheduledFutures.remove(scheduledFuture);
		}
		super.cancel();
	}
}
