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

import net.tomp2p.peers.PeerAddress;

public class FutureForkedBootstrap extends FutureForkJoin<FutureRouting> implements FutureBootstrap
{
	final private Collection<PeerAddress> bootstrapTo;
	final private PeerAddress self;

	public FutureForkedBootstrap(PeerAddress self, Collection<PeerAddress> bootstrapTo, FutureRouting... forks)
	{
		super(forks);
		this.bootstrapTo = bootstrapTo;
		this.self = self;
	}

	@Override
	public Collection<PeerAddress> getBootstrapTo()
	{
		return bootstrapTo;
	}

	protected boolean setFinish(FutureRouting last, FutureType type)
	{
		if (completed)
			return false;
		// reevaluate, since we fail those who did not bootstrap.
		boolean failed = true;
		for (FutureRouting futureRouting : getAll())
		{
			if (futureRouting.isSuccess() && futureRouting.getDirectHits().size() == 0
					&& futureRouting.getPotentialHits().size() == 1)
			{
				if (!futureRouting.getPotentialHits().first().equals(self))
				{
					failed = false;
					break;
				}
			}
			else
			{
				failed = false;
				break;
			}
		}
		if (failed)
		{
			last.reason = "We could not contact any other peer than ourselfs.";
			return super.setFinish(last, FutureType.FAILED);
		}
		else
		{
			return super.setFinish(last, FutureType.OK);
		}
	}

}
