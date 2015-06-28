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
package net.tomp2p.connection;

import java.security.KeyPair;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.p2p.MaintenanceTask;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.peers.RTT;
import net.tomp2p.rpc.BloomfilterFactory;
import net.tomp2p.storage.DigestStorage;
import net.tomp2p.storage.DigestTracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bean that holds non-sharable (unique for each peer) configuration settings for the peer. The sharable
 * configurations are stored in a {@link ConnectionBean}.
 * 
 * @author Thomas Bocek
 */
public class PeerBean {

	private static final Logger LOG = LoggerFactory.getLogger(PeerBean.class);
    
	private KeyPair keyPair;
    private PeerAddress serverPeerAddress;
    private PeerMap peerMap;
    private List<PeerStatusListener> peerStatusListeners = new ArrayList<PeerStatusListener>(1);
    private BloomfilterFactory bloomfilterFactory;
    private MaintenanceTask maintenanceTask;
    private DigestStorage digestStorage;
    private DigestTracker digestTracker;
    private HolePInitiator holePunchInitiator;
    private int holePNumberOfHoles;
    private int holePNumberOfPunches;
    
	/**
	 * This map is used for all open PeerConnections which are meant to stay
	 * open. {@link Number160} = peer ID.
	 */
	private ConcurrentHashMap<Number160, PeerConnection> openPeerConnections = new ConcurrentHashMap<Number160, PeerConnection>();

    /**
     * Creates a peer bean with a key pair.
     * 
     * @param keyPair
     *            The key pair that holds private and public key
     */
    public PeerBean(final KeyPair keyPair) {
        this.keyPair = keyPair;
    }

    /**
     * @return The key pair that holds private and public key
     */
    public KeyPair keyPair() {
        return keyPair;
    }

    /**
     * @return The address of this peer. This address may change.
     */
    public PeerAddress serverPeerAddress() {
        return serverPeerAddress;
    }

    /**
     * Sets a new address for this peer.
     * @param serverPeerAddress
     *            The new address of this peer.
     * @return This class
     */
    public PeerBean serverPeerAddress(final PeerAddress serverPeerAddress) {
        this.serverPeerAddress = serverPeerAddress;
        return this;
    }

    /**
     * @return The public and private key
     */
    public KeyPair getKeyPair() {
        return keyPair;
    }

    /**
     * Sets a new key pair for this peer.
     * @param keyPair
     *            The new private and public key for this peer.
     * @return This class
     */
    public PeerBean keyPair(final KeyPair keyPair) {
        this.keyPair = keyPair;
        return this;
    }

    /**
     * @return The map that stores neighbors
     */
    public PeerMap peerMap() {
        return peerMap;
    }

    /**
     * Sets a new map storing neighbors for this peer.
     * @param peerMap
     *            The new map that stores neighbors
     * @return This class
     */
    public PeerBean peerMap(final PeerMap peerMap) {
        this.peerMap = peerMap;
        return this;
    }

    /**
     * @return The listeners that are interested in the peer's status. E.g., peer is found to be online, offline or failed to respond in time.
     */
    public List<PeerStatusListener> peerStatusListeners() {
        return peerStatusListeners;
    }
    
    public PeerBean notifyPeerFound(PeerAddress sender, PeerAddress reporter, PeerConnection peerConnection, RTT roundTripTime) {
    	synchronized (peerStatusListeners) {
    		for (PeerStatusListener peerStatusListener : peerStatusListeners) {
    			peerStatusListener.peerFound(sender, reporter, peerConnection, roundTripTime);
    		}
    	}
    	return this;
    }

    /**
     * Adds a PeerStatusListener to this peer.
     * @param peerStatusListener
     *            The new listener that is interested in the peer's status
     * @return This class
     */
    public PeerBean addPeerStatusListener(final PeerStatusListener peerStatusListener) {
    	synchronized (peerStatusListeners) {
    		peerStatusListeners.add(peerStatusListener);    
        }
        return this;
    }
    
    /**
     * Removes a PeerStatusListener from this peer.
     * @param peerStatusListener The listener that is no longer intereseted in the peer's status
     * @return This class
     */
    public PeerBean removePeerStatusListener(final PeerStatusListener peerStatusListener) {
    	synchronized (peerStatusListeners) {
    		peerStatusListeners.remove(peerStatusListener);    
        }
        return this;
    }
    
    public PeerBean bloomfilterFactory(final BloomfilterFactory bloomfilterFactory) {
        this.bloomfilterFactory = bloomfilterFactory;
        return this;
    }

    public BloomfilterFactory bloomfilterFactory() {
        return bloomfilterFactory;
    }

    public PeerBean maintenanceTask(MaintenanceTask maintenanceTask) {
        this.maintenanceTask = maintenanceTask;
        return this;
    }
    
    public MaintenanceTask maintenanceTask() {
        return maintenanceTask;
    }
    
    public PeerBean digestStorage(DigestStorage digestStorage) {
        this.digestStorage = digestStorage;
        return this;
    }
    
    public DigestStorage digestStorage() {
        return digestStorage;
    }
    
    public PeerBean digestTracker(DigestTracker digestTracker) {
        this.digestTracker = digestTracker;
        return this;
    }
    
    public DigestTracker digestTracker() {
        return digestTracker;
    }
    
    
    public PeerBean holePunchInitiator(HolePInitiator holePunchInitiator) {
    	this.holePunchInitiator = holePunchInitiator;
    	return this;
    }
    
    public HolePInitiator holePunchInitiator() {
    	return holePunchInitiator;
    }

    /**
     * Returns a {@link ConcurrentHashMap} with all currently open
     * PeerConnections. Number160 = peer ID.
     *
     * @return openPeerConnections
     */
	public ConcurrentHashMap<Number160, PeerConnection> openPeerConnections() {
		return openPeerConnections;
	}

	/**
	 * Returns the {@link PeerConnection} for the given {@link Number160}
	 * peerId.
	 * 
	 * @param {@link Number160} peerId The ID of the peer
	 * @return {@link PeerConnection} peerConnection The connection associated to the peer ID
	 */
	public PeerConnection peerConnection(final Number160 peerId) {
		PeerConnection peerConnection = openPeerConnections.get(peerId);
		if (peerConnection != null) {
			return peerConnection;
		} else {
			LOG.error("There was no PeerConnection for peerId = " + peerId);
			return null;
		}
	}
	
	public PeerBean holePNumberOfHoles(final int holePNumberOfHoles) {
		this.holePNumberOfHoles = holePNumberOfHoles;
		return this;
	}
	
	public int holePNumberOfHoles() {
		return holePNumberOfHoles;
	}

	public PeerBean holePNumberOfPunches(final int holePNumberOfPunches) {
		this.holePNumberOfPunches = holePNumberOfPunches;
		return this;
	}
	
	public int holePNumberOfPunches() {
		return this.holePNumberOfPunches;
	}
}
