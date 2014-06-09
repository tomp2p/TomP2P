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

import net.tomp2p.p2p.MaintenanceTask;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.BloomfilterFactory;
import net.tomp2p.storage.DigestStorage;
import net.tomp2p.storage.DigestTracker;

/**
 * A bean that holds non-sharable (unique for each peer) configuration settings for the peer. The sharable
 * configurations are stored in {@link ConnectionBean}.
 * 
 * @author Thomas Bocek
 */
public class PeerBean {

    private KeyPair keyPair;
    private PeerAddress serverPeerAddress;
    private PeerMap peerMap;
    private List<PeerStatusListener> peerStatusListeners = new ArrayList<PeerStatusListener>(1);
    private BloomfilterFactory bloomfilterFactory;
    private MaintenanceTask maintenanceTask;
    private DigestStorage digestStorage;
    private DigestTracker digestTracker;
    

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
     * @return The peermap that stores neighbors
     */
    public PeerMap peerMap() {
        return peerMap;
    }

    /**
     * @param peerMap
     *            The peermap that stores neighbors
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

    /**
     * @param peerStatusListeners
     *            The listeners that are interested in the peer status, e.g., peer is found to be online, or a peer is
     *            offline or failed to respond in time
     * @return This class
     */
    public PeerBean addPeerStatusListeners(final PeerStatusListener peerStatusListener) {
    	synchronized (peerStatusListeners) {
    		peerStatusListeners.add(peerStatusListener);    
        }
        return this;
    }
    
    public PeerBean removePeerStatusListeners(final PeerStatusListener peerStatusListener) {
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
}
