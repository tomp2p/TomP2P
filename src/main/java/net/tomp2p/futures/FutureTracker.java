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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.tomp2p.p2p.EvaluatingSchemeTracker;
import net.tomp2p.p2p.VotingSchemeTracker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.TrackerData;

/**
 * This class holds the object for future results from the tracker get() and
 * add(). FutureTracker can fail, if the search did not return any results.
 * 
 * @author Thomas Bocek
 */
public class FutureTracker extends BaseFutureImpl<FutureTracker> implements FutureCleanup
{
	// since we receive results from multiple peers, we need to summarize them
	final private EvaluatingSchemeTracker evaluatingSchemeTracker;
	// a set of know peers that we don't want in the result set.
	final private Set<Number160> knownPeers;
	// keeps track of futures that are based on this future
	final private FutureCreate<BaseFuture> futureCreate;
	final private List<Cancellable> cleanup = new ArrayList<Cancellable>(1);
	// results
	private Set<PeerAddress> potentialTrackers;
	private Set<PeerAddress> directTrackers;
	private Map<PeerAddress, Collection<TrackerData>> peersOnTracker;
	
	public FutureTracker()
	{
		this(null);
	}

	/**
	 * Create a future object for storing
	 * 
	 * @param futureCreate Keeps track of futures that are based on this future
	 */
	public FutureTracker(FutureCreate<BaseFuture> futureCreate)
	{
		this(new VotingSchemeTracker(), null, futureCreate);
	}

	/**
	 * Create a future object for retrieving.
	 * 
	 * @param evaluatingSchemeTracker Since we receive results from multiple
	 *        peers, we need to summarize them
	 * @param knownPeers A set of know peers that we don't want in the result
	 *        set.
	 */
	public FutureTracker(EvaluatingSchemeTracker evaluatingSchemeTracker, Set<Number160> knownPeers)
	{
		this(new VotingSchemeTracker(), knownPeers, null);
	}

	/**
	 * Sets all the values for this future object.
	 * 
	 * @param evaluatingSchemeTracker Since we receive results from multiple
	 *        peers, we need to summarize them
	 * @param knownPeers A set of know peers that we don't want in the result
	 *        set.
	 * @param futureCreate Keeps track of futures that are based on this future
	 */
	private FutureTracker(EvaluatingSchemeTracker evaluatingSchemeTracker,
			Set<Number160> knownPeers, FutureCreate<BaseFuture> futureCreate)
	{
		this.evaluatingSchemeTracker = evaluatingSchemeTracker;
		this.knownPeers = knownPeers;
		this.futureCreate = futureCreate;
		self(this);
	}

	/**
	 * Called if a future is created based on this future.
	 * 
	 * @param future The newly created future
	 */
	public void repeated(BaseFuture future)
	{
		if (futureCreate != null)
			futureCreate.repeated(future);
	}

	/**
	 * Set the result of the tracker process.
	 * 
	 * @param potentialTrackers The trackers that are close to the key, also
	 *        containing the direct trackers.
	 * @param directTrackers Those peers that are close and reported to have the
	 *        key.
	 * @param peersOnTracker The data from the trackers.
	 */
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
			this.type = ((potentialTrackers.size() == 0) && (directTrackers.size() == 0))
					? BaseFuture.FutureType.FAILED
					: BaseFuture.FutureType.OK;
			if (this.type == BaseFuture.FutureType.FAILED)
			{
				this.reason = "we did not find anything, are you sure you are serching for the right tracker?";
			}
		}
		notifyListerenrs();
	}

	/**
	 * @return The trackers that are close to the key, also containing the
	 *         direct trackers.
	 */
	public Set<PeerAddress> getPotentialTrackers()
	{
		synchronized (lock)
		{
			return potentialTrackers;
		}
	}

	/**
	 * @return Those peers that are close and reported to have the key.
	 */
	public Set<PeerAddress> getDirectTrackers()
	{
		synchronized (lock)
		{
			return directTrackers;
		}
	}

	/**
	 * @return the raw data, which means all the data the trackers reported.
	 */
	public Map<PeerAddress, Collection<TrackerData>> getRawPeersOnTracker()
	{
		synchronized (lock)
		{
			return peersOnTracker;
		}
	}

	/**
	 * @return The peer address that send back data.
	 */
	public Set<PeerAddress> getPeersOnTracker()
	{
		synchronized (lock)
		{
			return peersOnTracker.keySet();
		}
	}

	/**
	 * @return The list of peers which we already have in our result set.
	 */
	public Set<Number160> getKnownPeers()
	{
		synchronized (lock)
		{
			return knownPeers;
		}
	}

	/**
	 * Evaluates the data from the trackers. Since we receive multiple results,
	 * we evaluate them before we give the data to the user. If the user wants
	 * to see the raw data, use {@link #getRawPeersOnTracker()}.
	 * 
	 * @return The data from the trackers.
	 */
	public Collection<TrackerData> getTrackers()
	{
		synchronized (lock)
		{
			return evaluatingSchemeTracker.evaluateSingle(peersOnTracker);
		}
	}

	public void addCleanup(Cancellable cancellable)
	{
		synchronized (lock)
		{
			cleanup.add(cancellable);
		}
	}

	public void shutdown()
	{
		// Even though, this future is completed, there may be tasks than can be
		// canceled due to scheduled futures attached to this event.
		synchronized (lock)
		{
			for (final Cancellable cancellable : cleanup)
			{
				cancellable.cancel();
			}
		}
	}
}
