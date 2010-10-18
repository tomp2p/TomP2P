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
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureForkedBroadcast;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles routing of nodes to other nodes
 * 
 * @author Thomas Bocek
 * 
 */
public class Routing
{
	final private static Logger logger = LoggerFactory.getLogger(Routing.class);
	final private NeighborRPC neighbors;
	final private PeerBean peerBean;

	public Routing(PeerBean peerBean, NeighborRPC neighbors)
	{
		this.neighbors = neighbors;
		this.peerBean = peerBean;
	}

	/**
	 * Bootstraps to the given remoteNode, i.e. looking for near and far nodes
	 * 
	 * @param remoteNode the node to which bootstrap should be performed to
	 * @param maxNoNewInfo number of nodes asked without new information to stop
	 *        at
	 * @param maxFailures number of failures to stop at
	 * @param parallel number of routing requests performed concurrently
	 * @return a FutureMulti object containing FutureRouting objects
	 */
	public FutureBootstrap bootstrap(final Collection<PeerAddress> peerAddresses, int maxNoNewInfo,
			int maxFailures, int parallel, boolean forceSocket)
	{
		// search close peers
		logger.debug("broadcast to " + peerAddresses);
		FutureRouting futureRouting1 = routing(peerAddresses, peerBean.getServerPeerAddress()
				.getID(), null, null, 0, maxNoNewInfo, maxFailures, parallel,
				Command.NEIGHBORS_STORAGE, false, forceSocket);
		// search far peers
		FutureRouting futureRouting2 = routing(peerAddresses, peerBean.getServerPeerAddress()
				.getID().xor(Number160.MAX_VALUE), null, null, 0, maxNoNewInfo, maxFailures,
				parallel, Command.NEIGHBORS_STORAGE, false, forceSocket);
		return new FutureForkedBroadcast(peerAddresses, futureRouting1, futureRouting2);
	}

	/**
	 * Looks for a route to the given locationKey
	 * 
	 * @param locationKey the node a route should be found to
	 * @param domainKey the domain of the network the current node and
	 *        locationKey is in
	 * @param contentKeys keys of the content to search for. Only used if you
	 *        perform a get
	 * @param maxDirectHits number of direct hits to stop at
	 * @param maxNoNewInfo number of nodes asked without new information to stop
	 *        at
	 * @param maxFailures number of failures to stop at
	 * @param parallel number of routing requests performed concurrently
	 * @return a FutureRouting object, is set to complete if the route has been
	 *         found
	 */
	public FutureRouting route(final Number160 locationKey, final Number160 domainKey,
			final Collection<Number160> contentKeys, Command command, int maxDirectHits,
			int maxNoNewInfo, int maxFailures, int parallel, boolean isDigest)
	{
		return route(locationKey, domainKey, contentKeys, command, maxDirectHits, maxNoNewInfo,
				maxFailures, parallel, isDigest, false);
	}

	FutureRouting route(final Number160 locationKey, final Number160 domainKey,
			final Collection<Number160> contentKeys, Command command, int maxDirectHits,
			int maxNoNewInfo, int maxFailures, int parallel,  boolean isDigest, boolean forceSocket)
	{
		// for bad distribution, use large NO_NEW_INFORMATION
		Collection<PeerAddress> startPeers = peerBean.getPeerMap().closePeers(locationKey,
				parallel * 2);
		return routing(startPeers, locationKey, domainKey, contentKeys, maxDirectHits,
				maxNoNewInfo, maxFailures, parallel, command, isDigest, forceSocket);
	}

	/**
	 * Looks for a route to the given locationKey
	 * 
	 * @param peerAddresses nodes which should be asked first for a route
	 * @param locationKey the node a route should be found to
	 * @param domainKey the domain of the network the current node and
	 *        locationKey is in
	 * @param contentKeys nodes which we got from another node
	 * @param maxDirectHits number of direct hits to stop at
	 * @param maxNoNewInfo number of nodes asked without new information to stop
	 *        at
	 * @param maxFailures number of failures to stop at
	 * @param parallel number of routing requests performed concurrently
	 * @return a FutureRouting object, is set to complete if the route has been
	 *         found
	 */
	private FutureRouting routing(Collection<PeerAddress> peerAddresses, Number160 locationKey,
			final Number160 domainKey, final Collection<Number160> contentKeys, int maxDirectHits,
			int maxNoNewInfo, int maxFailures, final int parallel, Command command,
			boolean isDigest, boolean forceSocket)
	{
		if (peerAddresses == null)
			throw new IllegalArgumentException("you need to specify some nodes");
		if (locationKey == null)
			throw new IllegalArgumentException("location key cannot be null");
		final FutureResponse[] futureResponses = new FutureResponse[parallel];
		final FutureRouting futureRouting = new FutureRouting();
		//
		final Comparator<PeerAddress> comparator = peerBean.getPeerMap().createPeerComparator(
				locationKey);
		final SortedSet<PeerAddress> queueToAsk = new TreeSet<PeerAddress>(comparator);
		// we can reuse the comparator
		final SortedSet<PeerAddress> alreadyAsked = new TreeSet<PeerAddress>(comparator);
		// as presented by Kazuyuki Shudo at AIMS 2009, its better to ask random
		// peers with the data than ask peers that ar ordered by distance ->
		// this balances load.
		final SortedSet<PeerAddress> directHits = new TreeSet<PeerAddress>(peerBean.getPeerMap()
				.createPeerComparator());
		final SortedSet<PeerAddress> potentialHits = new TreeSet<PeerAddress>(comparator);
		// fill initially
		queueToAsk.addAll(peerAddresses);
		alreadyAsked.add(peerBean.getServerPeerAddress());
		potentialHits.add(peerBean.getServerPeerAddress());
		//domainkey can be null if we bootstrap
		if (command == Command.NEIGHBORS_STORAGE && domainKey!=null)
		{
			DigestInfo digestInfo = Utils.digest(peerBean.getStorage(), locationKey, domainKey, contentKeys);
			if (digestInfo.getSize() > 0)
				directHits.add(peerBean.getServerPeerAddress());
		}
		else if (command == Command.NEIGHBORS_TRACKER)
		{
			DigestInfo digestInfo = Utils.digest(peerBean.getTrackerStorage(), locationKey, domainKey, contentKeys);
			if (digestInfo.getSize() > 0)
				directHits.add(peerBean.getServerPeerAddress());
		}
		if (peerAddresses.size() == 0)
		{
			futureRouting.setNeighbors(directHits, potentialHits);
		}
		else
		{
			routingRec(futureResponses, futureRouting, queueToAsk, alreadyAsked, directHits,
					potentialHits, new AtomicInteger(0), new AtomicInteger(0), maxDirectHits,
					maxNoNewInfo, maxFailures, parallel, locationKey, domainKey, contentKeys, true,
					command, isDigest, forceSocket, false);
		}
		return futureRouting;
	}

	

	/**
	 * Looks for a route to the given locationKey, performing recursively. Since
	 * this method is not called concurrently, but sequentially, no
	 * synchronization is necessary.
	 * 
	 * @param futureResponses expected responses
	 * @param futureRouting the current routing future used
	 * @param queueToAsk all nodes which should be asked for routing information
	 * @param alreadyAsked nodes which already have been asked
	 * @param directHits stores direct hits received
	 * @param noNewInfo number of nodes contacted without any new information
	 * @param nrFailures number of nodes without a response
	 * @param maxDirectHits number of direct hits to stop at
	 * @param maxNoNewInfo number of nodes asked without new information to stop
	 *        at
	 * @param maxFailures number of failures to stop at
	 * @param parallel number of routing requests performed concurrently
	 * @param locationKey the node a route should be found to
	 * @param domainKey the domain of the network the current node and
	 *        locationKey is in
	 * @param contentKeys nodes which we got from another node
	 */
	private void routingRec(final FutureResponse[] futureResponses,
			final FutureRouting futureRouting, final SortedSet<PeerAddress> queueToAsk,
			final SortedSet<PeerAddress> alreadyAsked, final SortedSet<PeerAddress> directHits,
			final SortedSet<PeerAddress> potentialHits, final AtomicInteger noNewInfo,
			final AtomicInteger nrFailures, final int maxDirectHits, final int maxNoNewInfo,
			final int maxFailures, final int parallel, final Number160 locationKey,
			final Number160 domainKey, final Collection<Number160> contentKeys,
			final boolean cancelOnFinish, final Command command, final boolean isDigest,
			final boolean forceSocket, final boolean stopCreatingNewFutures)
	{
		int active = 0;
		for (int i = 0; i < parallel; i++)
		{
			if (futureResponses[i] == null && !stopCreatingNewFutures)
			{
				PeerAddress next = Utils.pollFirst(queueToAsk);
				// PeerAddress next = queueToAsk.pollFirst();
				if (next != null)
				{
					alreadyAsked.add(next);
					active++;
					futureResponses[i] = neighbors.closeNeighbors(next, locationKey, domainKey,
							contentKeys, command, isDigest, forceSocket);
				}
			}
			else if (futureResponses[i] != null)
				active++;
		}
		if (active == 0)
		{
			futureRouting.setNeighbors(directHits, potentialHits);
			cancel(cancelOnFinish, parallel, futureResponses);
			return;
		}
		final boolean last = active == 1;
		FutureForkJoin<FutureResponse> fp = new FutureForkJoin<FutureResponse>(1, false,
				futureResponses);
		fp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>()
		{
			@Override
			public void operationComplete(FutureForkJoin<FutureResponse> future) throws Exception
			{
				boolean finished;
				boolean done;
				if (future.isSuccess())
				{
					Message lastResponse = future.getLast().getResponse();
					PeerAddress remotePeer = lastResponse.getSender();
					potentialHits.add(remotePeer);
					Collection<PeerAddress> newNeighbors = lastResponse.getNeighbors();
					logger.debug("Peer " + remotePeer + " reported " + newNeighbors);
					long contentLength = lastResponse.getInteger();
					Map<Number160, Number160> keyMap = lastResponse.getKeyMap();
					if (evaluateDirectHits(contentLength, keyMap, remotePeer, directHits,
							maxDirectHits))
					{
						finished = true;
						done = finished;
					}
					else if (evaluateInformation(newNeighbors, queueToAsk, alreadyAsked, noNewInfo,
							maxNoNewInfo))
					{
						finished = last;
						done = true;
					}
					else
					{
						finished = false;
						done = finished;
					}
				}
				else
				{
					finished = nrFailures.incrementAndGet() > maxFailures;
					done = finished;
				}
				if (finished)
				{
					logger.debug("done1 " + directHits);
					logger.debug("done2 " + potentialHits);
					futureRouting.setNeighbors(directHits, potentialHits);
					cancel(cancelOnFinish, parallel, futureResponses);
				}
				else
				{
					routingRec(futureResponses, futureRouting, queueToAsk, alreadyAsked,
							directHits, potentialHits, noNewInfo, nrFailures, maxDirectHits,
							maxNoNewInfo, maxFailures, parallel, locationKey, domainKey,
							contentKeys, cancelOnFinish, command, isDigest, forceSocket, done);
				}
			}
		});
	}

	static void cancel(boolean cancelOnFinish, int parallel, FutureResponse[] futureResponses)
	{
		if (cancelOnFinish)
		{
			for (int i = 0; i < parallel; i++)
			{
				if (futureResponses[i] != null)
					futureResponses[i].cancel();
			}
		}
	}

	// checks if we reached the end of our search.
	static boolean evaluateDirectHits(long contentLength, Map<Number160, Number160> keyMap,
			PeerAddress recipient, final Collection<PeerAddress> directHits, int maxDirectHits)
	{
		if (contentLength > 0)
		{
			directHits.add(recipient);
			if (directHits.size() >= maxDirectHits)
				return true;
		}
		return false;
	}

	// checks if we reached the end of our search.
	static boolean evaluateInformation(Collection<PeerAddress> newNeighbors,
			final SortedSet<PeerAddress> queueToAsk, final Set<PeerAddress> alreadyAsked,
			final AtomicInteger noNewInfo, int maxNoNewInfo)
	{
		// TODO: check why this null check is required
		// if (newNeighbors == null)
		// return false;
		boolean newInformation = merge(queueToAsk, newNeighbors, alreadyAsked);
		if (newInformation)
		{
			noNewInfo.set(0);
			return false;
		}
		else
		{
			return noNewInfo.incrementAndGet() >= maxNoNewInfo;
		}
	}

	// updates queuetoask with new data, returns if we found peers closer than
	// we already know.
	static boolean merge(SortedSet<PeerAddress> queueToAsk, Collection<PeerAddress> newNeighbors,
			Set<PeerAddress> alreadyAsked)
	{
		final SortedSet<PeerAddress> result = new TreeSet<PeerAddress>(queueToAsk.comparator());
		Utils.difference(newNeighbors, alreadyAsked, result);
		if (result.size() == 0)
			return false;
		PeerAddress first = result.first();
		boolean newInfo = isNew(queueToAsk, first);
		queueToAsk.addAll(result);
		return newInfo;
	}

	static boolean isNew(SortedSet<PeerAddress> queueToAsk, PeerAddress first)
	{
		if (queueToAsk.contains(first))
			return false;
		SortedSet<PeerAddress> tmp = queueToAsk.headSet(first);
		return tmp.size() == 0;
	}
}
