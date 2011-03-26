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


public class FutureWrappedBootstrap extends FutureWrapper<FutureBootstrap> implements
		FutureBootstrap
{
	private Collection<PeerAddress> bootstrapTo;

	public void waitForBootstrap(final FutureBootstrap bootstrap)
	{
		synchronized (lock)
		{
			this.bootstrapTo = bootstrap.getBootstrapTo();
		}
		waitFor(bootstrap);
	}
	@Deprecated
	public void waitForBootstrap(final FutureBootstrap bootstrap, final PeerAddress bootstrapTo)
	{
		synchronized (lock)
		{
			this.bootstrapTo = bootstrap.getBootstrapTo();
		}
		waitFor(bootstrap);
	}

	public void waitForBootstrap(final FutureBootstrap bootstrap,
			final Collection<PeerAddress> bootstrapTo)
	{
		synchronized (lock)
		{
			this.bootstrapTo = bootstrapTo;
		}
		waitFor(bootstrap);
	}

	public Collection<PeerAddress> getBootstrapTo()
	{
		synchronized (lock)
		{
			return bootstrapTo;
		}
	}
}
