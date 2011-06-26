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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import net.tomp2p.p2p.EvaluatingSchemeTracker;
import net.tomp2p.p2p.VotingSchemeTracker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.TrackerData;

/**
 * This class holds the object for future results from the tracker get() and
 * add(). FutureTracker can fail, if the search did not return any results.
 */
public class FutureTracker extends BaseFutureImpl
{
	final private EvaluatingSchemeTracker evaluatingSchemeTracker;
	final private Set<Number160> knownPeers;
	private volatile FutureCreate<BaseFuture> futureCreate;
	private Set<PeerAddress> potentialTrackers;
	private Set<PeerAddress> directTrackers;
	private Map<PeerAddress, Collection<TrackerData>> peersOnTracker;
	//
	private ScheduledFuture<?> scheduledFuture;
	private List<ScheduledFuture<?>> scheduledFutures;
	private boolean cancelSchedule = false;

	public FutureTracker()
	{
		this(new VotingSchemeTracker(), null);
	}

	public FutureTracker(EvaluatingSchemeTracker evaluatingSchemeTracker, Set<Number160> knownPeers)
	{
		this.evaluatingSchemeTracker = evaluatingSchemeTracker;
		this.knownPeers = knownPeers;
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
			Map<PeerAddress, Collection<TrackerData>> peersOnTracker)
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
				return;
			this.potentialTrackers = potentialTrackers;
			this.directTrackers = directTrackers;
			this.peersOnTracker = peersOnTracker;
			this.type = ((potentialTrackers.size() == 0) && (directTrackers.size() == 0)) ? BaseFuture.FutureType.FAILED
					: BaseFuture.FutureType.OK;
			if (this.type == BaseFuture.FutureType.FAILED)
			{
				this.reason = "we did not find anything, are you sure you are serching for the right tracker?";
			}
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

	public Map<PeerAddress, Collection<TrackerData>> getRawPeersOnTracker()
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

	public Collection<TrackerData> getTrackers()
	{
		synchronized (lock)
		{
			return evaluatingSchemeTracker.evaluateSingle(peersOnTracker);
		}
	}

	public Set<Number160> getKnownPeers()
	{
		synchronized (lock)
		{
			return knownPeers;
		}
	}

	public void setScheduledFuture(ScheduledFuture<?> scheduledFuture, List<ScheduledFuture<?>> scheduledFutures)
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
