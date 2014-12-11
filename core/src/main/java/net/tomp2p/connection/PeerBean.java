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
import net.tomp2p.peers.LocalMap;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.BloomfilterFactory;
import net.tomp2p.storage.DigestStorage;
import net.tomp2p.storage.DigestTracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bean that holds non-sharable (unique for each peer) configuration settings for the peer. The sharable
 * configurations are stored in {@link ConnectionBean}.
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
    private HolePunchInitiator holePunchInitiator;
    private LocalMap localMap;
    
	/**
	 * This map is used for all open peerConnections which are meant to stay
	 * open. {@link Number160} = peerId. {@link Integer} = amount of seconds which this connection
	 * should be kept alive.
	 */
	private ConcurrentHashMap<Number160, PeerConnection> openPeerConnections = new ConcurrentHashMap<Number160, PeerConnection>();

    /**
     * Creates a bean with a key pair.
     * 
     * @param keyPair
     *            The key pair that holds private public key
     */
    public PeerBean(final KeyPair keyPair) {
        this.keyPair = keyPair;
    }

    /**
     * @return The key pair that holds private public key
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
     * @param keyPair
     *            The public and private key
     * @return This class
     */
    public PeerBean keyPair(final KeyPair keyPair) {
        this.keyPair = keyPair;
        return this;
    }

    /**
     * @return The peerMap that stores neighbors
     */
    public PeerMap peerMap() {
        return peerMap;
    }

    /**
     * @param peerMap
     *            The peerMap that stores neighbors
     * @return This class
     */
    public PeerBean peerMap(final PeerMap peerMap) {
        this.peerMap = peerMap;
        return this;
    }

    /**
     * @return The listeners that are interested in the peer status, e.g., peer is found to be online, or a peer is
     *         offline or failed to respond in time.
     */
    public List<PeerStatusListener> peerStatusListeners() {
        return peerStatusListeners;
    }
    
    public PeerBean notifyPeerFound(PeerAddress sender, PeerAddress reporter, PeerConnection peerConnection) {
    	synchronized (peerStatusListeners) {
    		for (PeerStatusListener peerStatusListener : peerStatusListeners) {
    			peerStatusListener.peerFound(sender, reporter, peerConnection);
    		}
    	}
    	return this;
    }

    /**
     * @param peerStatusListener
     *            The listener that is interested in the peer status, e.g., peer is found to be online, or a peer is
     *            offline or failed to respond in time
     * @return This class
     */
    public PeerBean addPeerStatusListener(final PeerStatusListener peerStatusListener) {
    	synchronized (peerStatusListeners) {
    		peerStatusListeners.add(peerStatusListener);    
        }
        return this;
    }
    
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
    
    
    public PeerBean holePunchInitiator(HolePunchInitiator holePunchInitiator) {
    	this.holePunchInitiator = holePunchInitiator;
    	return this;
    }
    
    public HolePunchInitiator holePunchInitiator() {
    	return holePunchInitiator;
    }

	/**
	 * Returns a {@link ConcurrentHashMap} with all currently open
	 * PeerConnections.
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
	 * @param {@link Number160} peerId
	 * @return {@link PeerConnection} peerConnection
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

	public PeerBean localMap(LocalMap localMap) {
	    this.localMap = localMap;
	    return this;
    }
	
	public LocalMap localMap() {
	    return localMap;
    }
}
