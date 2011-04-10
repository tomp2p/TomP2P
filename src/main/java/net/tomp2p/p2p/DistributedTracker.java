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
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.message.MessageCodec;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.PeerExchangeRPC;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.rpc.TrackerData;
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
	// final private static int MAX_FULL_TRACKERS = 4;

	final private DistributedRouting routing;
	final private PeerBean peerBean;
	final private TrackerRPC trackerRPC;
	final private PeerExchangeRPC peerExchangeRPC;
	final private Random rnd;
	final private Number160 stableRandom;

	public DistributedTracker(PeerBean peerBean, DistributedRouting routing, TrackerRPC trackerRPC,
			PeerExchangeRPC peerExchangeRPC)
	{
		this.routing = routing;
		this.trackerRPC = trackerRPC;
		this.peerExchangeRPC = peerExchangeRPC;
		this.peerBean = peerBean;
		this.rnd = new Random(peerBean.getServerPeerAddress().getID().hashCode());
		this.stableRandom = new Number160(rnd);

	}

	public FutureTracker getFromTracker(final Number160 locationKey, final Number160 domainKey,
			RoutingConfiguration routingConfiguration, final TrackerConfiguration trackerConfiguration,
			final boolean expectAttachement, EvaluatingSchemeTracker evaluatingScheme, final boolean signMessage,
			final boolean useSecondaryTrackers, final SimpleBloomFilter<Number160> knownPeers)
	{
		final FutureTracker futureTracker = new FutureTracker(evaluatingScheme, knownPeers);
		if (useSecondaryTrackers)
		{
			SortedSet<PeerAddress> queueToAsk = peerBean.getTrackerStorage().getSelectionSecondary(locationKey,
					domainKey, trackerConfiguration.getMaxPrimaryTrackers());
			startLoop(locationKey, domainKey, trackerConfiguration, expectAttachement, signMessage, knownPeers,
					futureTracker, queueToAsk);
		}
		else
		{
			final FutureRouting futureRouting = createRouting(locationKey, domainKey, null, routingConfiguration,
					trackerConfiguration, true);
			// final Number160 searchCloseTo=new Number160(rnd);
			futureRouting.addListener(new BaseFutureAdapter<FutureRouting>()
			{
				@Override
				public void operationComplete(FutureRouting future) throws Exception
				{
					if (futureRouting.isSuccess())
					{
						if (logger.isDebugEnabled())
							logger.debug("found direct hits for tracker get: " + futureRouting.getDirectHits());
						startLoop(locationKey, domainKey, trackerConfiguration, expectAttachement, signMessage,
								knownPeers, futureTracker, futureRouting.getDirectHits());
					}
					else
					{
						futureTracker.setFailed("routing failed");
					}
				}

			});
		}
		return futureTracker;
	}

	private void startLoop(final Number160 locationKey, final Number160 domainKey,
			final TrackerConfiguration trackerConfiguration, final boolean expectAttachement,
			final boolean signMessage, final SimpleBloomFilter<Number160> knownPeers,
			final FutureTracker futureTracker, final SortedSet<PeerAddress> queueToAsk)
	{
		loop(locationKey, domainKey, queueToAsk, trackerConfiguration, futureTracker, true, knownPeers, new Operation()
		{
			@Override
			public FutureResponse create(PeerAddress remoteNode, boolean primary)
			{
				if (logger.isDebugEnabled())
					logger.debug("tracker get: " + remoteNode + " location=" + locationKey + " ");
				FutureResponse futureResponse = trackerRPC.getFromTracker(remoteNode, locationKey, domainKey,
						expectAttachement, signMessage, knownPeers);
				if (logger.isDebugEnabled())
				{
					futureResponse.addListener(new BaseFutureAdapter<FutureResponse>()
					{
						@Override
						public void operationComplete(FutureResponse future) throws Exception
						{
							logger.debug("found the following peers: " + future.getResponse().getDataMap().keySet());
						}
					});
				}
				return futureResponse;
			}
		});
	}

	public FutureForkJoin<FutureResponse> startExchange(final Number160 locationKey, final Number160 domainKey)
	{
		TrackerData trackerData = peerBean.getTrackerStorage().getSelection(locationKey, domainKey, peerBean.getTrackerStorage().getTrackerSize(), null);
		SortedMap<Number480, Data> peers = trackerData.getPeerDataMap();
		FutureResponse[] futureResponses = new FutureResponse[peers.size()];
		int i = 0;
		for (Data data : peers.values())
		{
			futureResponses[i++] = peerExchangeRPC.peerExchange(data.getPeerAddress(), locationKey, domainKey);
		}
		return new FutureForkJoin<FutureResponse>(futureResponses);
	}

	public FutureTracker addToTracker(final Number160 locationKey, final Number160 domainKey, final Data attachement,
			RoutingConfiguration routingConfiguration, final TrackerConfiguration trackerConfiguration,
			final boolean signMessage, final FutureCreate<BaseFuture> futureCreate,
			final SimpleBloomFilter<Number160> knownPeers)
	{
		final FutureTracker futureTracker = new FutureTracker();
		futureTracker.setFutureCreate(futureCreate);
		final FutureRouting futureRouting = createRouting(locationKey, domainKey, null, routingConfiguration,
				trackerConfiguration, false);
		futureRouting.addListener(new BaseFutureAdapter<FutureRouting>()
		{
			@Override
			public void operationComplete(FutureRouting future) throws Exception
			{
				if (futureRouting.isSuccess())
				{
					if (logger.isDebugEnabled())
						logger.debug("found potential hits for tracker add: " + futureRouting.getPotentialHits());
					loop(locationKey, domainKey, futureRouting.getPotentialHits(), trackerConfiguration, futureTracker,
							false, knownPeers, new Operation()
							{
								@Override
								public FutureResponse create(PeerAddress remoteNode, boolean primary)
								{
									if (logger.isDebugEnabled())
										logger.debug("tracker add: " + remoteNode + " location=" + locationKey);
									return trackerRPC.addToTracker(remoteNode, locationKey, domainKey, attachement,
											signMessage, primary, knownPeers);
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

	private void loop(Number160 locationKey, final Number160 domainKey, SortedSet<PeerAddress> queueToAsk,
			TrackerConfiguration trackerConfiguration, FutureTracker futureTracker, boolean isGet,
			final SimpleBloomFilter<Number160> knownPeers, Operation operation)
	{
		FutureResponse[] futureResponses = new FutureResponse[trackerConfiguration.getParallel()];
		SortedSet<PeerAddress> secondaryQueue = new TreeSet<PeerAddress>(peerBean.getPeerMap().createPeerComparator(
				stableRandom));
		loopRec(locationKey, domainKey, queueToAsk, secondaryQueue, new HashSet<PeerAddress>(),
				new HashMap<PeerAddress, Map<PeerAddress, Data>>(), operation, trackerConfiguration.getParallel(),
				new AtomicInteger(0), trackerConfiguration.getMaxFailure(), new AtomicInteger(0),
				trackerConfiguration.getMaxFullTrackers(), new AtomicInteger(0),
				trackerConfiguration.getAtLeastSucessfulRequestes(), trackerConfiguration.getAtLeastEntriesFromTrackers(),
				futureResponses, futureTracker, knownPeers, isGet);
	}

	private void loopRec(final Number160 locationKey, final Number160 domainKey,
			final SortedSet<PeerAddress> queueToAsk, final SortedSet<PeerAddress> secondaryQueue,
			final Set<PeerAddress> alreadyAsked, final Map<PeerAddress, Map<PeerAddress, Data>> peerOnTracker,
			final Operation operation, final int parallel, final AtomicInteger nrFailures, final int maxFailures,
			final AtomicInteger trackerFull, final int maxTrackerFull, final AtomicInteger successfulRequests,
			final int atLeastSuccessfullRequests, final int atLeastEntriesFromTrackers,
			final FutureResponse[] futureResponses, final FutureTracker futureTracker,
			final SimpleBloomFilter<Number160> knownPeers, final boolean isGet)
	{
		// if its a get, we cancel connections, because we have what we want.
		// For a put, we want to store on many peers, thus there we don't
		// cancel.
		final boolean cancelOnFinish = isGet;
		if (logger.isDebugEnabled())
			logger.debug("we can ask " + queueToAsk.size());
		int active = 0;
		for (int i = 0; i < parallel; i++)
		{
			if (futureResponses[i] == null)
			{
				// TODO: make this more smart
				boolean primary = true;
				PeerAddress next;
				if (isGet)
					next = Utils.pollRandom(queueToAsk, rnd);
				else
					next = Utils.pollFirst(queueToAsk);
				if (next == null)
				{
					if (isGet)
						next = Utils.pollRandom(secondaryQueue, rnd);
					else
						next = Utils.pollFirst(secondaryQueue);

					primary = false;
				}
				// PeerAddress next = queueToAsk.pollFirst();
				if (next != null)
				{
					alreadyAsked.add(next);
					active++;
					futureResponses[i] = operation.create(next, primary);
				}
			}
			else if (futureResponses[i] != null)
				active++;
		}
		if (active == 0)
		{
			queueToAsk.addAll(secondaryQueue);
			if (logger.isDebugEnabled())
				logger.debug("we finished1, we asked " + alreadyAsked.size() + ", but we could ask "
						+ queueToAsk.size() + " more nodes " + alreadyAsked);
			futureTracker.setTrackers(queueToAsk, alreadyAsked, peerOnTracker);
			DistributedRouting.cancel(cancelOnFinish, parallel, futureResponses);
			return;
		}
		FutureForkJoin<FutureResponse> fp = new FutureForkJoin<FutureResponse>(1, false, futureResponses);
		fp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>()
		{
			@Override
			public void operationComplete(FutureForkJoin<FutureResponse> future) throws Exception
			{
				boolean finished = false;
				FutureResponse futureResponse = future.getLast();
				// success if we could add the tracker, but also if the tracker
				// is full and sent a denied message
				boolean isFull = futureResponse != null && futureResponse.getResponse() != null
						&& futureResponse.getResponse().getType() == Type.DENIED;
				boolean isPartial = futureResponse != null && futureResponse.getResponse() != null
						&& futureResponse.getResponse().getType() == Type.PARTIALLY_OK;
				if (future.isSuccess() || isFull)
				{
					Map<Number160, Data> newDataMap = futureResponse.getResponse().getDataMap();
					mergeC(secondaryQueue, newDataMap.values(), alreadyAsked);
					merge(peerOnTracker, newDataMap, futureResponse.getRequest().getRecipient(), knownPeers);
					// regardless if we search for a tracker or add something to
					// a tracker, we know that this tracker is alive and
					// serving, so add it to the primary list
					Data data = new Data(MessageCodec.EMPTY_BYTE_ARRAY, futureResponse.getRequest().getRecipient());
					peerBean.getTrackerStorage().put(locationKey, domainKey, null, data);
					int successRequests = isFull ? successfulRequests.get() : successfulRequests.incrementAndGet();
					finished = evaluate(peerOnTracker, successRequests, atLeastSuccessfullRequests,
							atLeastEntriesFromTrackers, isGet);
					// if peer reported that he can provide more data, we keep
					// the peer in the list
					if (!finished && isPartial)
					{
						if (logger.isDebugEnabled())
							logger.debug("partial: " + futureResponse.getRequest().getRecipient());
						secondaryQueue.add(futureResponse.getRequest().getRecipient());
					}
					if (!finished && isFull)
					{
						if (logger.isDebugEnabled())
							logger.debug("tracker reported to be full. Check if finished due to full trackers.");
						finished = trackerFull.incrementAndGet() >= maxTrackerFull;
					}
				}
				else
				{
					if (logger.isDebugEnabled())
						logger.debug("no success " + future.getFailedReason());
					finished = nrFailures.incrementAndGet() > maxFailures;
				}
				// check if done, or continune looping
				if (finished)
				{
					queueToAsk.addAll(secondaryQueue);
					if (logger.isDebugEnabled())
						logger.debug("we finished2, we asked " + alreadyAsked.size() + ", but we could ask "
								+ queueToAsk.size() + " more nodes (" + successfulRequests + "/"
								+ atLeastSuccessfullRequests + ")");
					futureTracker.setTrackers(queueToAsk, alreadyAsked, peerOnTracker);
					DistributedRouting.cancel(cancelOnFinish, parallel, futureResponses);
				}
				else
				{
					loopRec(locationKey, domainKey, queueToAsk, secondaryQueue, alreadyAsked, peerOnTracker, operation,
							parallel, nrFailures, maxFailures, trackerFull, maxTrackerFull, successfulRequests,
							atLeastSuccessfullRequests, atLeastEntriesFromTrackers, futureResponses, futureTracker,
							knownPeers, isGet);
				}
			}
		});
	}

	private boolean evaluate(Map<?, ?> peerOnTracker, int successfulRequests, int atLeastSuccessfulRequests,
			int atLeastEntriesFromTrackers, boolean isGet)
	{
		if (isGet)
			return successfulRequests >= atLeastSuccessfulRequests || peerOnTracker.size() >= atLeastEntriesFromTrackers;
		else
			return successfulRequests >= atLeastSuccessfulRequests;
	}

	private FutureRouting createRouting(Number160 locationKey, Number160 domainKey, Set<Number160> contentKeys,
			RoutingConfiguration routingConfiguration, TrackerConfiguration trackerConfiguration, boolean isDigest)
	{
		return routing.route(locationKey, domainKey, contentKeys, Command.NEIGHBORS_TRACKER,
				routingConfiguration.getDirectHits(),
				routingConfiguration.getMaxNoNewInfo(trackerConfiguration.getMaxPrimaryTrackers()),
				routingConfiguration.getMaxFailures(), routingConfiguration.getMaxSuccess(),
				routingConfiguration.getParallel(), isDigest);
	}

	static boolean evaluateInformation(Collection<PeerAddress> newNeighbors, final SortedSet<PeerAddress> queueToAsk,
			final Set<PeerAddress> alreadyAsked, final AtomicInteger noNewInfo, int maxNoNewInfo)
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

	static void merge(Map<PeerAddress, Map<PeerAddress, Data>> peerOnTracker, Map<Number160, Data> newDataMap,
			PeerAddress reporter, SimpleBloomFilter<Number160> knownPeers)
	{
		knownPeers.add(reporter.getID());
		for (Data data : newDataMap.values())
		{
			PeerAddress peer = data.getPeerAddress();
			knownPeers.add(peer.getID());
			Map<PeerAddress, Data> peerOnTrackerEntry = peerOnTracker.get(peer);
			if (peerOnTrackerEntry == null)
			{
				peerOnTrackerEntry = new HashMap<PeerAddress, Data>();
				peerOnTracker.put(peer, peerOnTrackerEntry);
			}
			peerOnTrackerEntry.put(reporter, data);
		}
	}

	static boolean mergeC(Collection<PeerAddress> queueToAsk, Collection<Data> newData, Set<PeerAddress> alreadyAsked)
	{
		Collection<PeerAddress> newNeighbors = new HashSet<PeerAddress>();
		for (Data data : newData)
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
		public abstract FutureResponse create(PeerAddress address, boolean primary);
	}
}
