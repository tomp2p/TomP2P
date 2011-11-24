/*
 * Copyright 2011 Thomas Bocek
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

import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;

/**
 * The routing future keeps track of the routing process. This means that the routing future is returned immediately and the
 * routing process starts in the background. There are two ways to wait for the routing process: (1) to use await*, which blocks
 * the current thread and waits for the routing process to complete, or (2) to use addListener*, which will be called when the
 * routing process completes. The listener may or may not run in the same thread.
 * 
 * The routing will always succeed if we do DHT operations or bootstrap to ourself. It will fail if we bootstrap to another peer, 
 * but could not contact any peer than ourself.
 * 
 * @see #setNeighbors(SortedSet, SortedSet, SortedSet, boolean, boolean)
 * 
 * @author Thomas Bocek
 */
public class FutureRouting extends BaseFutureImpl
{
	private SortedSet<PeerAddress> potentialHits;
	private SortedMap<PeerAddress, DigestInfo> directHits;
	private SortedSet<PeerAddress> routingPath;

	/**
	 * Sets the result of the routing process and finishes the future. This will notify all listeners. The future will always 
	 * succeed if we do DHT operations or bootstrap to ourself. It will fail if we bootstrap to another peer, but could not 
	 * contact any peer than ourself.
	 * 
	 * @see #getDirectHits()
	 * @see #getDirectHitsDigest()
	 * @see #getPotentialHits()
	 * @see #getRoutingPath()
	 * 
	 * @param directHits The direct hits, the peers in the direct set that reports to have the key (Number160) we were looking for.
	 * @param potentialHits The potential hits, the peers in the direct set and those peers that reports to *not* have the key (Number160) we 
	 * 		were looking for.
	 * @param routingPath A set of peers that took part in the routing process.
	 * @param isBootstrap Whether the future was triggered by the bootstrap process or the a P2P process
	 * @param isRoutingToOther Whether routing peers have been specified others than myself.
	 */
	public void setNeighbors(final SortedMap<PeerAddress, DigestInfo> directHits, final SortedSet<PeerAddress> potentialHits,
			final SortedSet<PeerAddress> routingPath, boolean isBootstrap, boolean isRoutingToOther)
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify()) return;
			this.potentialHits = potentialHits;
			this.directHits = directHits;
			this.routingPath = routingPath;
			if(isBootstrap && isRoutingToOther) 
			{
				// we need to fail if we only find ourself. This means that we did not connect to any peer and we did not
				// wanted connect to ourself.
				this.type = ((potentialHits.size() <= 1) && (directHits.size() == 0)) ? BaseFuture.FutureType.FAILED
						: BaseFuture.FutureType.OK;
			}
			else
			{
				// for DHT or bootstraping to ourself, we set to success, since we may want to store 
				// data on our peer rather than failing completely if we dont find other peers
				this.type = BaseFuture.FutureType.OK;
			}
			
		}
		notifyListerenrs();
	}

	/**
	 * The potential hits set contains those peers that are in the direct hit and that did report to *not* have the key (Number160) we were looking for. 
	 * We already check for the content during routing, since we send the information what we are looking for anyway, 
	 * so a reply if the content exists or not is not very expensive. However, a peer may lie about this.
	 * 
	 * @see #getDirectHits()
	 * @see #getDirectHitsDigest()
	 * 
	 * @return The potential hits, the peers in the direct set and those peers that reports to *not* have the key (Number160) we 
	 * 		were looking for. 
	 */
	public SortedSet<PeerAddress> getPotentialHits()
	{
		synchronized (lock)
		{
			return potentialHits;
		}
	}
	
	/**
	 * The direct hits set contains those peers that reported to have the key (Number160) we were looking for. 
	 * We already check for the content during routing, since we send the information what we are looking for anyway, 
	 * so a reply if the content exists or not is not very expensive. However, a peer may lie about this. 
	 * 
	 * @see #getPotentialHits()
	 * @see #getDirectHitsDigest()
	 * 
	 * @return The direct hits, the peers in the direct set that reports to have the key (Number160) we were looking for. 
	 */
	public SortedSet<PeerAddress> getDirectHits()
	{
		synchronized (lock)
		{
			//some Java implementations always return SortedSet, some don't (Android)
			Set<PeerAddress> tmp = directHits.keySet();
			//if we have a SortedSet, we are fine
			if(tmp instanceof SortedSet)
			{
				return (SortedSet<PeerAddress>)directHits.keySet();
			}
			//otherwise, create a new sorted set, put the existing values there, and return this.
			else
			{
				TreeSet<PeerAddress> tmp2 = new TreeSet<PeerAddress>(directHits.comparator());
				tmp2.addAll(tmp);
				return tmp2;
			}
		}
	}
	
	/**
	 * The direct hits map contains those peers that reported to have the key (Number160) we were looking for including 
	 * its digest (size of the result set and its xored hashes). We already check for the content during routing, since 
	 * we send the information what we are looking for anyway, so a reply if the content exists or not is not very 
	 * expensive. However, a peer may lie about this.
	 * 
	 * @see #getPotentialHits()
	 * @see #getDirectHits()
	 * 
	 * @return The direct hits including its digest (size of the result set and its xored hashes), when a peer reports to 
	 * have the key (Number160) we were looking for. 
	 */
	public SortedMap<PeerAddress, DigestInfo> getDirectHitsDigest()
	{
		synchronized (lock)
		{
			return directHits;
		}
	}

	/**
	 * Returns the peers that have been asked to provide neighbor information.
	 * The order is sorted by peers that were close to the target.
	 * 
	 * @return A set of peers that took part in the routing process.
	 */
	public SortedSet<PeerAddress> getRoutingPath()
	{
		synchronized (lock)
		{
			return routingPath;
		}
	}

	@Override
	public String getFailedReason()
	{
		synchronized (lock)
		{
			return "FutureRouting -> complete:" + completed + ", type:" + type.toString() + ", direct:"
					+ directHits.size() + ", neighbors:" + potentialHits.size();
		}
	}
}