/*
 * Copyright 2012 Thomas Bocek
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

package net.tomp2p.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapChangeListener;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.storage.ReplicationStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has 3 methods that are called from outside eventes: check, peerInsert, peerRemoved.
 */
public class Replication implements PeerMapChangeListener {
    private static final Logger LOG = LoggerFactory.getLogger(Replication.class);

    private final List<ResponsibilityListener> listeners = new ArrayList<ResponsibilityListener>();

    private final PeerMap peerMap;

    private final PeerAddress selfAddress;

    private final ReplicationStorage replicationStorage;

    private int replicationFactor;

    /**
     * Constructor.
     * 
     * @param replicationStorage
     *            The interface where we shore the replication responsibilities
     * @param selfAddress
     *            My address to know for what my peer is responsible
     * @param peerMap
     *            The map of my neighbors
     * @param replicationFactor
     *            The replication factor
     */
    public Replication(final ReplicationStorage replicationStorage, final PeerAddress selfAddress,
            final PeerMap peerMap, final int replicationFactor) {
        this.replicationStorage = replicationStorage;
        this.selfAddress = selfAddress;
        this.peerMap = peerMap;
        this.replicationFactor = replicationFactor;
        peerMap.addPeerMapChangeListener(this);
    }

    /**
     * 
     * @param replicationFactor
     *            Set the replication factor
     */
    public void setReplicationFactor(final int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    /**
     * 
     * @return The replication factor.
     */
    public int getReplicationFactor() {
        return replicationFactor;
    }

    /**
     * Checks if the user enabled replication.
     * 
     * @return True if replication is enabled.
     */
    public boolean isReplicationEnabled() {
        return listeners.size() > 0;
    }

    /**
     * Add responsibility listener. If the first listener is added, replication is considered enabled.
     * 
     * @param responsibilityListener
     *            The responsibility listener.
     */
    public void addResponsibilityListener(final ResponsibilityListener responsibilityListener) {
        listeners.add(responsibilityListener);
    }

    /**
     * Remove a responsibility listener. If all responsibility listeners are removed, replication is considered
     * disabled.
     * 
     * @param responsibilityListener
     *            The responsibility listener.
     */
    public void removeResponsibilityListener(final ResponsibilityListener responsibilityListener) {
        listeners.remove(responsibilityListener);
    }

    /**
     * Notify if I'm responsible and something needs to change, i.e., to make sure that there are enough replicas.
     * 
     * @param locationKey
     *            The location key.
     */
    private void notifyMeResponsible(final Number160 locationKey) {
        for (ResponsibilityListener responsibilityListener : listeners) {
            responsibilityListener.meResponsible(locationKey);
        }
    }

    /**
     * Notify if an other peer is responsible and we should transfer data to this peer.
     * 
     * @param locationKey
     *            The location key.
     * @param other
     *            The other peer.
     */
    private void notifyOtherResponsible(final Number160 locationKey, final PeerAddress other) {
        for (ResponsibilityListener responsibilityListener : listeners) {
            responsibilityListener.otherResponsible(locationKey, other);
        }
    }

    /**
     * Update responsibilities. This happens for a put / add.
     * 
     * @param locationKey
     *            The location key.
     */
    public void updateAndNotifyResponsibilities(final Number160 locationKey) {
        if (!isReplicationEnabled()) {
            return;
        }
        PeerAddress closest = closest(locationKey);
        if (closest.getPeerId().equals(selfAddress.getPeerId())) {
            if (replicationStorage.updateResponsibilities(locationKey, closest.getPeerId())) {
                // I am responsible for this content
                notifyMeResponsible(locationKey);
            }
        } else {
            if (replicationStorage.updateResponsibilities(locationKey, closest.getPeerId())) {
                // notify that someone else is now responsible for the
                // content with key responsibleLocations
                notifyOtherResponsible(locationKey, closest);
            }
        }
    }

    @Override
    public void peerInserted(final PeerAddress peerAddress, final boolean verified) {
        if (!isReplicationEnabled() || !verified) {
            return;
        }
        LOG.debug("The peer {} was inserted in my map. I'm {}", peerAddress, selfAddress);
        // check if we should change responsibility.
        Collection<Number160> myResponsibleLocations = replicationStorage.findContentForResponsiblePeerID(selfAddress
                .getPeerId());
        LOG.debug("I ({}) am currently responsibel for {}", selfAddress, myResponsibleLocations);
        
        for (Number160 myResponsibleLocation : myResponsibleLocations) {
            PeerAddress closest = closest(myResponsibleLocation);
            if (!closest.getPeerId().equals(selfAddress.getPeerId())) {
                if (replicationStorage.updateResponsibilities(myResponsibleLocation, closest.getPeerId())) {
                    // notify that someone else is now responsible for the
                    // content with key responsibleLocations
                    notifyOtherResponsible(myResponsibleLocation, closest);
                }
            } else if (isInReplicationRange(myResponsibleLocation, peerAddress, replicationFactor)) {
                // we are still responsible, but a new peer joined and if it is within the x close peers, we need to
                // replicate
                if (replicationStorage.updateResponsibilities(myResponsibleLocation, peerAddress.getPeerId())) {
                    notifyOtherResponsible(myResponsibleLocation, peerAddress);
                }
            }
        }
    }

    @Override
    public void peerRemoved(final PeerAddress peerAddress, final PeerStatatistic peerStatatistic) {
        if (!isReplicationEnabled()) {
            return;
        }
        // check if we should change responsibility.
        Collection<Number160> otherResponsibleLocations = replicationStorage
                .findContentForResponsiblePeerID(peerAddress.getPeerId());
        Collection<Number160> myResponsibleLocations = replicationStorage.findContentForResponsiblePeerID(selfAddress
                .getPeerId());
        // check if we are now responsible for content where the other peer was responsible
        for (Number160 otherResponsibleLocation : otherResponsibleLocations) {
            PeerAddress closest = closest(otherResponsibleLocation);
            if (closest.getPeerId().equals(selfAddress.getPeerId())) {
                if (replicationStorage.updateResponsibilities(otherResponsibleLocation, closest.getPeerId())) {
                    // notify that someone I'm now responsible for the
                    // content
                    // with key responsibleLocations
                    notifyMeResponsible(otherResponsibleLocation);
                    // we don't need to check this again, so remove it from the list if present
                    myResponsibleLocations.remove(otherResponsibleLocation);
                }
            } else {
                replicationStorage.updateResponsibilities(otherResponsibleLocation, closest.getPeerId());
            }
        }
        // now check for our responsibilities. If a peer is gone and it was in the replication range, we need make sure
        // we have enough copies
        for (Number160 myResponsibleLocation : myResponsibleLocations) {
            if (isInReplicationRange(myResponsibleLocation, peerAddress, replicationFactor)) {
                notifyMeResponsible(myResponsibleLocation);
            }
        }
    }

    @Override
    public void peerUpdated(final PeerAddress peerAddress, final PeerStatatistic peerStatatistic) {
        // do nothing
    }

    /**
     * Returns the closest peer to a number (including myself).
     * 
     * @param locationKey
     *            The locationKey
     * @return The peer that is responsible for the location key, including myself.
     */
    private PeerAddress closest(final Number160 locationKey) {
        SortedSet<PeerAddress> tmp = peerMap.closePeers(locationKey, 1);
        tmp.add(selfAddress);
        return tmp.iterator().next();
    }

    /**
     * Checks if a peeraddress is within the replication range. This means that the peer should also receive a replica.
     * 
     * @param locationKey
     *            The locationKey
     * @param peerAddress
     *            The peeraddress to check if its within the replication range
     * @param replicationFactor
     *            The replication factor
     * 
     * @return True if the peer is within replication range, otherwise false.
     */
    private boolean isInReplicationRange(final Number160 locationKey, final PeerAddress peerAddress,
            final int replicationFactor) {
        SortedSet<PeerAddress> tmp = peerMap.closePeers(locationKey, replicationFactor);
        return tmp.headSet(peerAddress).size() < replicationFactor;
    }
}
