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
package net.tomp2p.peers;

/**
 * This interface can be added to the map, to get notified of peer insertion or removal. This is useful for replication.
 * 
 * @author Thomas Bocek
 */
public interface PeerMapChangeListener {
    /**
     * This method is called if a peer is added to the map. The peer is always added to the non-verified map first.
     * 
     * @param peerAddress
     *            The peer that has been added.
     * @param verified
     *            True if the peer was inserted in the verified map
     */
    void peerInserted(PeerAddress peerAddress, boolean verified);

    /**
     * This method is called if a peer is removed from the map.
     * 
     * @param peerAddress
     *            The peer that has been removed and add to the cache.
     * @param storedPeerAddress
     *            Contains information statistical information
     */
    void peerRemoved(PeerAddress peerAddress, PeerStatatistic storedPeerAddress);

    /**
     * This method is called if a peer is updated.
     * 
     * @param peerAddress
     *            The peer can change its IP and some statistical data
     * @param storedPeerAddress
     *            Contains information statistical information
     */
    void peerUpdated(PeerAddress peerAddress, PeerStatatistic storedPeerAddress);
}
