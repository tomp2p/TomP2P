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
import java.util.concurrent.TimeUnit;

import net.tomp2p.peers.PeerAddress;

import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

/**
 * The future that keeps track of network discovery such as discovery if its
 * behind a NAT, the status if UPNP or NAT-PMP could be established, if there is
 * portforwarding.
 * 
 * @author Thomas Bocek
 * 
 */
public class FutureDiscover extends BaseFutureImpl
{
	// timeout to tell us when discovery failed.
	private Timeout timeout;
	// result
	private PeerAddress peerAddress;
	private boolean discoveredTCP = false;
	private boolean discoveredUDP = false;

	/**
	 * Creates a new future object and creates a timer that fires failed after a
	 * timeout.
	 * 
	 * @param timer The timer to use
	 * @param delaySec The delay in seconds
	 */
	public void setTimeout(Timer timer, int delaySec)
	{
		synchronized (lock)
		{
			timeout = timer.newTimeout(new DiscoverTimeoutTask(), delaySec, TimeUnit.SECONDS);
			addListener(new BaseFutureAdapter<FutureDiscover>()
					{
				@Override
				public void operationComplete(FutureDiscover future) throws Exception
				{
					// cancel timeout if we are done.
					synchronized (lock)
					{
						if(timeout != null)
						{
							timeout.cancel();
						}
					}
				}
			});
		}
	}

	/**
	 * Gets called if the discovery was a success and an other peer could ping
	 * us with TCP and UDP.
	 * 
	 * @param peerAddress The peerAddress of our server
	 */
	public void done(PeerAddress peerAddress)
	{
		//System.err.println("called done");
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
				return;
			this.type = FutureType.OK;
			this.peerAddress = peerAddress;
		}
		notifyListerenrs();
	}

	/**
	 * The peerAddress where we are reachable
	 * 
	 * @return The new un-firewalled peerAddress of this peer
	 */
	public PeerAddress getPeerAddress()
	{
		synchronized (lock)
		{
			return peerAddress;
		}
	}

	/**
	 * Intermediate result if TCP has been discovered. Set discoveredTCP True if
	 * other peer could reach us with a TCP ping.
	 */
	public void setDiscoveredTCP()
	{
		synchronized (lock)
		{
			this.discoveredTCP = true;
		}
	}

	/**
	 * Intermediate result if UDP has been discovered. Set discoveredUDP True if
	 * other peer could reach us with a UDP ping.
	 */
	public void setDiscoveredUDP()
	{
		synchronized (lock)
		{
			this.discoveredUDP = true;
		}
	}

	/**
	 * Checks if this peer can be reached via TCP.
	 * 
	 * @return True if this peer can be reached via TCP from outside.
	 */
	public boolean isDiscoveredTCP()
	{
		synchronized (lock)
		{
			return discoveredTCP;
		}
	}

	/**
	 * Checks if this peer can be reached via UDP.
	 * 
	 * @return True if this peer can be reached via UDP from outside.
	 */
	public boolean isDiscoveredUDP()
	{
		synchronized (lock)
		{
			return discoveredUDP;
		}

	}

	/**
	 * In case of no peer can contact us, we fire an failed.
	 */
	private final class DiscoverTimeoutTask implements TimerTask
	{
		private final long start=System.currentTimeMillis();
		@Override
		public void run(Timeout timeout) throws Exception
		{
			setFailed("Timeout in Discover: "+(System.currentTimeMillis()-start)+ "ms");
		}
	}
}
