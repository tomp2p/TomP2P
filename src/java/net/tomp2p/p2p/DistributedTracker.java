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
package net.tomp2p.p2p;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.message.Message.Command;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.TrackerRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DistributedTracker
{
	final private static Logger logger = LoggerFactory.getLogger(DistributedTracker.class);
	// in theory if you have a perfect distribution then you can have up to
	// (direct_hits*max_trackers)+max_trackers^4 entries, which can be used. For
	// 30 as max_trackers and 5 direct hits, this results in (30*5)+30^4=810150.
	// 810150 is enough for a tracker I think.
	final private static int MAX_FULL_TRACKERS = 4;
	final private Routing routing;
	final private PeerBean peerBean;
	final private TrackerRPC trackerRPC;

	public DistributedTracker(PeerBean peerBean, Routing routing, TrackerRPC trackerRPC)
	{
		this.routing = routing;
		this.trackerRPC = trackerRPC;
		this.peerBean = peerBean;
	}

	public FutureTracker getFromTracker(final Number160 locationKey, final Number160 domainKey,
			RoutingConfiguration routingConfiguration,
			final TrackerConfiguration trackerConfiguration, final boolean expectAttachement,
			EvaluatingSchemeTracker evaluatingScheme, final boolean signMessage)
	{
		final FutureTracker futureTracker = new FutureTracker(evaluatingScheme);
		final FutureRouting futureRouting = createRouting(locationKey, domainKey, null,
				routingConfiguration, trackerConfiguration, true);
		futureRouting.addListener(new BaseFutureAdapter<FutureRouting>()
		{
			@Override
			public void operationComplete(FutureRouting future) throws Exception
			{
				if (futureRouting.isSuccess())
				{
					if (logger.isDebugEnabled())
						logger.debug("found direct hits for tracker get: "
								+ futureRouting.getDirectHits());
					loop(locationKey, domainKey, futureRouting.getDirectHits(),
							trackerConfiguration, futureTracker, true, new Operation()
							{
								@Override
								public FutureResponse create(PeerAddress remoteNode)
								{
									if (logger.isDebugEnabled())
										logger.debug("tracker get: " + remoteNode + " location="
												+ locationKey);
									return trackerRPC.getFromTracker(remoteNode, locationKey,
											domainKey, expectAttachement, signMessage);
								}
							});
				}
				else
				{
					futureTracker.setFailed("routing failed");
				}
			}
		});
		return futureTracker;
	}

	public FutureTracker addToTracker(final Number160 locationKey, final Number160 domainKey,
			final Data attachement, RoutingConfiguration routingConfiguration,
			final TrackerConfiguration trackerConfiguration, final boolean signMessage,
			final FutureCreate<FutureTracker> futureCreate)
	{
		final FutureTracker futureTracker = new FutureTracker();
		futureTracker.setFutureCreate(futureCreate);
		final FutureRouting futureRouting = createRouting(locationKey, domainKey, null,
				routingConfiguration, trackerConfiguration, false);
		futureRouting.addListener(new BaseFutureAdapter<FutureRouting>()
		{
			@Override
			public void operationComplete(FutureRouting future) throws Exception
			{
				if (futureRouting.isSuccess())
				{
					if (logger.isDebugEnabled())
						logger.debug("found potential hits for tracker add: "
								+ futureRouting.getPotentialHits());
					loop(locationKey, domainKey, futureRouting.getPotentialHits(),
							trackerConfiguration, futureTracker, false, new Operation()
							{
								@Override
								public FutureResponse create(PeerAddress remoteNode)
								{
									if (logger.isDebugEnabled())
										logger.debug("tracker add: " + remoteNode + " location="
												+ locationKey);
									return trackerRPC.addToTracker(remoteNode, locationKey,
											domainKey, attachement, signMessage);
								}
							});
				}
				else
				{
					futureTracker.setFailed("routing failed");
				}
			}
		});
		return futureTracker;
	}

	private void loop(Number160 locationKey, final Number160 domainKey,
			SortedSet<PeerAddress> queueToAsk, TrackerConfiguration trackerConfiguration,
			FutureTracker futureTracker, boolean cancelOnFinish, Operation operation)
	{
		FutureResponse[] futureResponses = new FutureResponse[trackerConfiguration.getParallel()];
		// make pollfirst not equal for all peers
		SortedSet<PeerAddress> secondaryQueue = new TreeSet<PeerAddress>(peerBean.getPeerMap()
				.createPeerComparator());
		loopRec(queueToAsk, secondaryQueue, new HashSet<PeerAddress>(),
				new HashMap<PeerAddress, Map<PeerAddress, Data>>(), operation, trackerConfiguration
						.getParallel(), new AtomicInteger(0), trackerConfiguration.getMaxFailure(),
				new AtomicInteger(0), queueToAsk.size() + MAX_FULL_TRACKERS, new AtomicInteger(0),
				trackerConfiguration.getAtLeastSucessfulRequestes(), trackerConfiguration
						.getAtLeastTrackers(), futureResponses, futureTracker, cancelOnFinish);
	}

	private void loopRec(final SortedSet<PeerAddress> queueToAsk,
			final SortedSet<PeerAddress> secondaryQueue, final Set<PeerAddress> alreadyAsked,
			final Map<PeerAddress, Map<PeerAddress, Data>> peerOnTracker,
			final Operation operation, final int parallel, final AtomicInteger nrFailures,
			final int maxFailures, final AtomicInteger trackerFull, final int maxTrackerFull,
			final AtomicInteger successfulRequests, final int atLeastSuccessfullRequests,
			final int atLeastPeersOnTrackers, final FutureResponse[] futureResponses,
			final FutureTracker futureTracker, final boolean cancelOnFinish)
	{
		int active = 0;
		for (int i = 0; i < parallel; i++)
		{
			if (futureResponses[i] == null)
			{
				// TODO: make this more smart
				PeerAddress next = Utils.pollFirst(queueToAsk);
				if (next == null)
					next = Utils.pollFirst(secondaryQueue);
				// PeerAddress next = queueToAsk.pollFirst();
				if (next != null)
				{
					alreadyAsked.add(next);
					active++;
					futureResponses[i] = operation.create(next);
				}
			}
			else if (futureResponses[i] != null)
				active++;
		}
		if (active == 0)
		{
			queueToAsk.addAll(secondaryQueue);
			futureTracker.setTrackers(queueToAsk, alreadyAsked, peerOnTracker);
			Routing.cancel(cancelOnFinish, parallel, futureResponses);
			return;
		}
		FutureForkJoin<FutureResponse> fp = new FutureForkJoin<FutureResponse>(1, false,
				futureResponses);
		fp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>()
		{
			@Override
			public void operationComplete(FutureForkJoin<FutureResponse> future) throws Exception
			{
				boolean finished = false;
				FutureResponse futureResponse = future.getLast();
				// success if we could add the tracker, but also if the tracker
				// is full and sent a denied message
				boolean success = futureResponse != null && futureResponse.getResponse() != null
						&& future.getLast().getResponse().isNotOk();
				if (future.isSuccess() || success)
				{
					Map<Number160, Data> newDataMap = futureResponse.getResponse()
							.getDataMap();
					mergeC(secondaryQueue, newDataMap.values(), alreadyAsked);
					merge(peerOnTracker, newDataMap, futureResponse.getRequest().getRecipient());
					int successRequests = success ? successfulRequests.get() : successfulRequests
							.incrementAndGet();
					finished = evaluate(peerOnTracker, successRequests, atLeastSuccessfullRequests,
							atLeastPeersOnTrackers);
					if (!finished && success)
					{
						logger.debug("this tracker was full");
						finished = trackerFull.incrementAndGet() >= maxTrackerFull;
					}
				}
				else
				{
					logger.debug("no success " + future.getFailedReason());
					finished = nrFailures.incrementAndGet() > maxFailures;
				}
				if (finished)
				{
					queueToAsk.addAll(secondaryQueue);
					futureTracker.setTrackers(queueToAsk, alreadyAsked, peerOnTracker);
					Routing.cancel(cancelOnFinish, parallel, futureResponses);
				}
				else
				{
					loopRec(queueToAsk, secondaryQueue, alreadyAsked, peerOnTracker, operation,
							parallel, nrFailures, maxFailures, trackerFull, maxTrackerFull,
							successfulRequests, atLeastSuccessfullRequests, atLeastPeersOnTrackers,
							futureResponses, futureTracker, cancelOnFinish);
				}
			}
		});
	}

	private boolean evaluate(Map<?, ?> peerOnTracker, int successfulRequests,
			int atLeastSuccessfulRequests, int atLeastTrackers)
	{
		return successfulRequests >= atLeastSuccessfulRequests
				&& peerOnTracker.size() >= atLeastTrackers;
	}

	private FutureRouting createRouting(Number160 locationKey, Number160 domainKey,
			Set<Number160> contentKeys, RoutingConfiguration routingConfiguration,
			TrackerConfiguration trackerConfiguration, boolean isDigest)
	{
		return routing.route(locationKey, domainKey, contentKeys, Command.NEIGHBORS_TRACKER,
				routingConfiguration.getDirectHits(), routingConfiguration
						.getMaxNoNewInfo(trackerConfiguration.getAtLeastSucessfulRequestes()),
				routingConfiguration.getMaxFailures(), routingConfiguration.getParallel(), isDigest);
	}

	static boolean evaluateInformation(Collection<PeerAddress> newNeighbors,
			final SortedSet<PeerAddress> queueToAsk, final Set<PeerAddress> alreadyAsked,
			final AtomicInteger noNewInfo, int maxNoNewInfo)
	{
		boolean newInformation = merge(queueToAsk, newNeighbors, alreadyAsked);
		if (newInformation)
		{
			noNewInfo.set(0);
			return false;
		}
		else
			return noNewInfo.incrementAndGet() >= maxNoNewInfo;
	}

	static void merge(Map<PeerAddress, Map<PeerAddress, Data>> peerOnTracker,
			Map<Number160, Data> newDataMap, PeerAddress reporter)
	{
		for (Data data : newDataMap.values())
		{
			PeerAddress peer=data.getPeerAddress();
			Map<PeerAddress, Data> peerOnTrackerEntry = peerOnTracker.get(peer);
			if (peerOnTrackerEntry == null)
			{
				peerOnTrackerEntry = new HashMap<PeerAddress, Data>();
				peerOnTracker.put(peer, peerOnTrackerEntry);
			}
			peerOnTrackerEntry.put(reporter, data);
		}
	}
	
	static boolean mergeC(Collection<PeerAddress> queueToAsk, Collection<Data> newData,
			Set<PeerAddress> alreadyAsked)
	{
		Collection<PeerAddress> newNeighbors=new HashSet<PeerAddress>();
		for(Data data:newData)
			newNeighbors.add(data.getPeerAddress());
		return merge(queueToAsk, newNeighbors, alreadyAsked);
	}

	static boolean merge(Collection<PeerAddress> queueToAsk, Collection<PeerAddress> newNeighbors,
			Set<PeerAddress> alreadyAsked)
	{
		// result will be small...
		final Set<PeerAddress> result = new HashSet<PeerAddress>();
		Utils.difference(newNeighbors, alreadyAsked, result);
		if (result.size() == 0)
			return false;
		int size1 = queueToAsk.size();
		queueToAsk.addAll(result);
		int size2 = queueToAsk.size();
		return size1 < size2;
	}
	public interface Operation
	{
		public abstract FutureResponse create(PeerAddress address);
	}
}
