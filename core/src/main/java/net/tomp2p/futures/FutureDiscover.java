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


import java.net.InetAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.peers.PeerAddress;

/**
 * The future that keeps track of network discovery such as discovery if its behind a NAT, the status if UPNP or NAT-PMP
 * could be established, if there is portforwarding.
 * 
 * @author Thomas Bocek
 */
public class FutureDiscover extends BaseFutureImpl<FutureDiscover> {

    // result
    private PeerAddress ourPeerAddress;

    private PeerAddress reporter;

    private boolean discoveredTCP = false;

    private boolean discoveredUDP = false;
    
    private boolean isNat = false;
    
    private InetAddress internalAddress;
    
    private InetAddress externalAddress;

    /**
     * Constructor.
     */
    public FutureDiscover() {
        self(this);
    }

    /**
     * Creates a new future object and creates a timer that fires failed after a timeout.
     * 
     * @param timer
     *            The timer to use
     * @param delaySec
     *            The delay in seconds
     */
    public void timeout(final PeerAddress serverPeerAddress, final ScheduledExecutorService timer, final int delaySec) {
        final DiscoverTimeoutTask task = new DiscoverTimeoutTask(serverPeerAddress);
        final ScheduledFuture<?> scheduledFuture = timer.schedule(task, TimeUnit.SECONDS.toMillis(delaySec), TimeUnit.MILLISECONDS);
        addListener(new BaseFutureAdapter<FutureDiscover>() {
            @Override
            public void operationComplete(final FutureDiscover future) throws Exception {
                // cancel timeout if we are done.
            	scheduledFuture.cancel(false);
            }
        });
    }

    /**
     * Gets called if the discovery was a success and an other peer could ping us with TCP and UDP.
     * 
     * @param ourPeerAddress
     *            The peerAddress of our server
     * @param reporter
     *            The peerAddress of the peer that reported our address
     */
    public void done(final PeerAddress ourPeerAddress, final PeerAddress reporter) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return;
            }
            this.type = FutureType.OK;
            this.ourPeerAddress = ourPeerAddress;
            this.reporter = reporter;
        }
        notifyListeners();
    }

    /**
     * The peerAddress where we are reachable.
     * 
     * @return The new un-firewalled peerAddress of this peer
     */
    public PeerAddress peerAddress() {
        synchronized (lock) {
            return ourPeerAddress;
        }
    }

    /**
     * @return The reporter that told us what peer address we have
     */
    public PeerAddress reporter() {
        synchronized (lock) {
            return reporter;
        }
    }

    /**
     * Intermediate result if TCP has been discovered. Set discoveredTCP True if other peer could reach us with a TCP
     * ping.
     */
    public void discoveredTCP() {
        synchronized (lock) {
            this.discoveredTCP = true;
        }
    }

    /**
     * Intermediate result if UDP has been discovered. Set discoveredUDP True if other peer could reach us with a UDP
     * ping.
     */
    public void discoveredUDP() {
        synchronized (lock) {
            this.discoveredUDP = true;
        }
    }

    /**
     * Checks if this peer can be reached via TCP.
     * 
     * @return True if this peer can be reached via TCP from outside.
     */
    public boolean isDiscoveredTCP() {
        synchronized (lock) {
            return discoveredTCP;
        }
    }

    /**
     * Checks if this peer can be reached via UDP.
     * 
     * @return True if this peer can be reached via UDP from outside.
     */
    public boolean isDiscoveredUDP() {
        synchronized (lock) {
            return discoveredUDP;
        }
    }

    /**
     * In case of no peer can contact us, we fire an failed.
     */
    private final class DiscoverTimeoutTask implements Runnable {
        private final long start = System.currentTimeMillis();
        private final PeerAddress serverPeerAddress;
        
        private DiscoverTimeoutTask(PeerAddress serverPeerAddress) {
        	this.serverPeerAddress = serverPeerAddress;
        }
        
        @Override
        public void run() {
        	failed(serverPeerAddress, "Timeout in Discover: " + 
        			(System.currentTimeMillis() - start) + "ms. However, I think my peer address is " + serverPeerAddress);
        }
    }
    
    public FutureDiscover failed(PeerAddress serverPeerAddress, final String failed) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return this;
            }
            this.reason = failed;
            this.ourPeerAddress = serverPeerAddress;
            this.type = FutureType.FAILED;
        }
        notifyListeners();
        return this;
    }

	public FutureDiscover externalHost(String failed, InetAddress internalAddress, InetAddress externalAddress) {
		synchronized (lock) {
            if (!completedAndNotify()) {
                return this;
            }
            this.reason = failed;
            this.type = FutureType.FAILED;
            this.internalAddress = internalAddress;
            this.externalAddress = externalAddress;
            this.isNat = true;
        }
        notifyListeners();
        return this;
	    
    }
	
	public InetAddress internalAddress() {
        synchronized (lock) {
            return internalAddress;
        }
    }
	
	public InetAddress externalAddress() {
        synchronized (lock) {
            return externalAddress;
        }
    }
	
	public boolean isNat() {
		synchronized (lock) {
			return isNat;
		}
	}
}
