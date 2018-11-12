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

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import net.tomp2p.p2p.MaintenanceTask;
import net.tomp2p.p2p.PeerAddressManager;
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
@NoArgsConstructor
@Accessors(chain = true, fluent = true)
public class PeerBean {

    private static final Logger LOG = LoggerFactory.getLogger(PeerBean.class);
    
    @Getter final private List<PeerStatusListener> peerStatusListeners = new ArrayList<PeerStatusListener>(1);
    
    @Getter @Setter private KeyPair keyPair;
    @Getter @Setter private PeerAddress serverPeerAddress;
    @Getter @Setter private PeerMap peerMap;
    @Getter @Setter private BloomfilterFactory bloomfilterFactory;
    @Getter @Setter private MaintenanceTask maintenanceTask;
    @Getter @Setter private DigestStorage digestStorage;
    @Getter @Setter private DigestTracker digestTracker;
    @Getter @Setter private NATHandler natHandler;
    @Getter @Setter private PeerAddressManager shortIdLookup;

    //This map is used for all open PeerConnections which are meant to stay open. {@link Number256} = peer ID.

    public PeerBean notifyPeerFound(PeerAddress sender, PeerAddress reporter,
            RTT roundTripTime) {
        synchronized (peerStatusListeners) {
            for (PeerStatusListener peerStatusListener : peerStatusListeners) {
                peerStatusListener.peerFound(sender, reporter, roundTripTime);
            }
        }
        return this;
    }

    /**
     * Adds a PeerStatusListener to this peer.
     *
     * @param peerStatusListener The new listener that is interested in the peer's status
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
     *
     * @param peerStatusListener The listener that is no longer intereseted in the peer's status
     * @return This class
     */
    public PeerBean removePeerStatusListener(final PeerStatusListener peerStatusListener) {
        synchronized (peerStatusListeners) {
            peerStatusListeners.remove(peerStatusListener);
        }
        return this;
    }

}
