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

import java.util.SortedSet;

import net.tomp2p.peers.PeerAddress;

public class FutureRouting extends BaseFutureImpl
{
	private SortedSet<PeerAddress> potentialHits;
	private SortedSet<PeerAddress> directHits;
	private SortedSet<PeerAddress> routingPath;

	public void setNeighbors(final SortedSet<PeerAddress> directHits, final SortedSet<PeerAddress> potentialHits,
			final SortedSet<PeerAddress> routingPath)
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify()) return;
			this.potentialHits = potentialHits;
			this.directHits = directHits;
			this.routingPath = routingPath;
			this.type = ((potentialHits.size() == 0) && (directHits.size() == 0)) ? BaseFuture.FutureType.FAILED
					: BaseFuture.FutureType.OK;
		}
		notifyListerenrs();
	}

	public SortedSet<PeerAddress> getPotentialHits()
	{
		synchronized (lock)
		{
			return potentialHits;
		}
	}

	public SortedSet<PeerAddress> getDirectHits()
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
