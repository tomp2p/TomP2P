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

package net.tomp2p.p2p;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapChangeListener;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.storage.StorageLayer;

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

    private final StorageLayer backend;

    private int replicationFactor;

    private boolean nRootReplication;

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
     * @param nRoot
     *            <code>true</code> for n-root replication, <code>false</code> for 0-root replication
     */
    public Replication(final StorageLayer backend, final PeerAddress selfAddress,
            final PeerMap peerMap, final int replicationFactor, final boolean nRoot) {
        this.backend = backend;
        this.selfAddress = selfAddress;
        this.peerMap = peerMap;
        this.replicationFactor = replicationFactor;
        this.nRootReplication = nRoot;
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
	 * 
	 * @param nRootReplication flag
	 *            Set <code>true</code> for n-root replication or <code>false</code> for 0-root replication.
	 */
	public void setNRootReplication(boolean nRootReplication) {
		this.nRootReplication = nRootReplication;
	}

	/**
	 * 
	 * @return <code>true</code> if n-root replication is enabled
	 */
	public boolean isNRootReplicationEnabled() {
		return nRootReplication;
	}
	
	/**
	 * 
	 * @return <code>true</code> if 0-root replication is enabled
	 */
	public boolean is0RootReplicationEnabled() {
		return !nRootReplication;
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
    
    private void notifyMeResponsible(final Number160 locationKey, PeerAddress newPeer) {
        for (ResponsibilityListener responsibilityListener : listeners) {
            responsibilityListener.meResponsible(locationKey, newPeer);
        }
    }

    /**
     * Notify if an other peer is responsible and we should transfer data to this peer.
     * 
     * @param locationKey
     *            The location key.
     * @param other
     *            The other peer.
     * @param delayed Indicates if the other peer should get notified immediately or delayed. The case for delayed is that
     *            multiple non responsible peers may call this and a delayed call in that case may be better.
     */
    private void notifyOtherResponsible(final Number160 locationKey, final PeerAddress other, final boolean delayed) {
        for (ResponsibilityListener responsibilityListener : listeners) {
            responsibilityListener.otherResponsible(locationKey, other, delayed);
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
        if (!nRootReplication) {
            PeerAddress closest = closest(locationKey);
            if (closest.getPeerId().equals(selfAddress.getPeerId())) {
                if (backend.updateResponsibilities(locationKey, closest.getPeerId())) {
                    // I am responsible for this content
                    notifyMeResponsible(locationKey);
                }
            } else {
                if (backend.updateResponsibilities(locationKey, closest.getPeerId())) {
                    // notify that someone else is now responsible for the
                    // content with key responsibleLocations
                    notifyOtherResponsible(locationKey, closest, false);
                }
            }
        } else {
			if (isInReplicationRange(locationKey, selfAddress, replicationFactor)) {
				if (backend.updateResponsibilities(locationKey, selfAddress.getPeerId())) {
					LOG.debug("I {} am now responsible for key {}.", selfAddress, locationKey);
					notifyMeResponsible(locationKey);
				} else {
					LOG.debug("I {} already know that I'm responsible for key {}.", selfAddress, locationKey);
					// get all other replica nodes
					Collection<Number160> peerIds = backend
							.findPeerIDsForResponsibleContent(locationKey);
					peerIds.remove(selfAddress.getPeerId());
					// check if all other replica nodes are still responsible
					boolean hasToNotifyReplicaSet = false;
					for (Number160 otherReplica : peerIds) {
						// TODO avoid this initialization
						PeerAddress peerAddress = new PeerAddress(otherReplica);
						if (!isInReplicationRange(locationKey, peerAddress, replicationFactor)) {
							LOG.debug("I {} detected that {} is not responsible anymore for key {}.",
									selfAddress, peerAddress, locationKey);
							backend.removeResponsibility(locationKey, otherReplica);
							hasToNotifyReplicaSet = true;
						} else {
							LOG.debug("I {} checked that {} is still responsible for key {}.", selfAddress,
									peerAddress, locationKey);
						}
					}
					if (hasToNotifyReplicaSet) {
						// at least one replica node is not responsible anymore,
						// notify replica set
						notifyMeResponsible(locationKey);
					}
				}
			} else {
				LOG.debug("I {} am not responsible for key {}.", selfAddress, locationKey);
				backend.removeResponsibility(locationKey);
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
        Collection<Number160> myResponsibleLocations = backend.findContentForResponsiblePeerID(selfAddress
                .getPeerId());
        LOG.debug("I {} have to check replication responsibilities for {}.", selfAddress, myResponsibleLocations);
        
        for (Number160 myResponsibleLocation : myResponsibleLocations) {
			if (!nRootReplication) {
				// use 0-root replication strategy
				PeerAddress closest = closest(myResponsibleLocation);
				if (!closest.getPeerId().equals(selfAddress.getPeerId())) {
					if (isInReplicationRange(myResponsibleLocation, selfAddress, replicationFactor)) {
    					if (backend.updateResponsibilities(myResponsibleLocation, closest.getPeerId())) {
    						LOG.debug("I {} didn't know that {} is responsible for {}.", selfAddress, closest,
    								myResponsibleLocation);
    						// notify that someone else is now responsible for the
    						// content with key responsibleLocations
    						notifyOtherResponsible(myResponsibleLocation, closest, false);
    						// cancel any pending notifyMeResponsible*, as we are
    						// not responsible anymore.
    					} else {
    						LOG.debug("I {} know already that {} is responsible for {}.", selfAddress, closest,
    								myResponsibleLocation);
    					}
					} else {
						// notify closest replica node about responsibility
						notifyOtherResponsible(myResponsibleLocation, closest, false);
						LOG.debug("I {} am no more in the replica set of {}.", selfAddress, myResponsibleLocation);
						backend.removeResponsibility(myResponsibleLocation);
					}
				} else if (isInReplicationRange(myResponsibleLocation, peerAddress, replicationFactor)) {
					LOG.debug("{} is in the replica set for {}.", peerAddress, myResponsibleLocation);
					// we are still responsible, but a new peer joined and if it
					// is within the x close peers, we need to
					// replicate
					if (backend.updateResponsibilities(myResponsibleLocation,
							selfAddress.getPeerId())) {
						LOG.debug("I {} didn't know that I'm responsible for {}.", selfAddress,
								myResponsibleLocation);
						// I figured out I'm the new responsible, so check all
						// my peer in the replication range
						notifyMeResponsible(myResponsibleLocation);
					} else {
						LOG.debug("I {} already know that I'm responsible for {}.", selfAddress,
								myResponsibleLocation);
						// new peer joined, I'm responsible, so replicate to
						// that peer
						notifyMeResponsible(myResponsibleLocation, peerAddress);
					}
				}
			} else {
				// use n-root replication strategy
				if (isInReplicationRange(myResponsibleLocation, peerAddress, replicationFactor)) {
					// newly joined peer has duty to replicate
					if (isInReplicationRange(myResponsibleLocation, selfAddress, replicationFactor)) {
						// new joined peer and I have to replicate
						if (backend.updateResponsibilities(myResponsibleLocation,
								selfAddress.getPeerId())) {
							LOG.debug("I {} didn't know my replication responsibility for {}.", selfAddress,
									myResponsibleLocation);
							// notify all replicas
							notifyMeResponsible(myResponsibleLocation);
						} else {
							LOG.debug("I {} already know my replication responsibility for {}.", selfAddress,
									myResponsibleLocation);
						}
						if (backend.updateResponsibilities(myResponsibleLocation,
								peerAddress.getPeerId())) {
							LOG.debug(
									"I {} didn't know common replication responsibility with newly joined peer {} for {}.",
									selfAddress, peerAddress, myResponsibleLocation);
							// newly joined peer has to get notified
							notifyMeResponsible(myResponsibleLocation, peerAddress);
						} else {
							LOG.debug(
									"I {} already know common replication responsibility with newly joined peer {} for {}.",
									selfAddress, peerAddress, myResponsibleLocation);
						}
					} else {
						LOG.debug(
								"I {} figured out replication responsibility of newly joined peer {} for {}.",
								selfAddress, peerAddress, myResponsibleLocation);
						// newly joined peer has to get notified
						notifyOtherResponsible(myResponsibleLocation, peerAddress, false);
						// don't add newly joined peer id and it's new
						// replication responsibility to our replication map,
						// because given key doesn't affect us anymore
						LOG.debug("I {} am not responsible anymore for {}.", selfAddress, myResponsibleLocation);
						// I'm not in replication range, I don't need to know
						// about all responsibility entries to the given key
						backend.removeResponsibility(myResponsibleLocation);
					}
				} else {
					// newly joined peer doesn't have to replicate
					if (isInReplicationRange(myResponsibleLocation, selfAddress, replicationFactor)) {
						// I still have to replicate
						if (backend.updateResponsibilities(myResponsibleLocation,
								selfAddress.getPeerId())) {
							LOG.debug(
									"I {} didn't know my replication responsibility. Newly joined peer {} doesn't has to replicate {}.",
									selfAddress, peerAddress, myResponsibleLocation);
							notifyMeResponsible(myResponsibleLocation);
						} else {
							LOG.debug(
									"I {} already know my replication responsibility. Newly joined peer {} doesn't has to replicate {}.",
									selfAddress, peerAddress, myResponsibleLocation);
						}
					} else {
						LOG.debug(
								"I {} and newly joined peer {} don't have to replicate {}.",
								selfAddress, peerAddress, myResponsibleLocation);
						// I'm not in replication range, I don't need to know
						// about all responsibility entries to the given key
						backend.removeResponsibility(myResponsibleLocation);
					}
				}
			}
		}
    }

    @Override
    public void peerRemoved(final PeerAddress peerAddress, final PeerStatatistic peerStatatistic) {
        if (!isReplicationEnabled()) {
            return;
        }
        LOG.debug("The peer {} was removed from my map. I'm {}", peerAddress, selfAddress);
        // check if we should change responsibility.
        Collection<Number160> otherResponsibleLocations = backend
                .findContentForResponsiblePeerID(peerAddress.getPeerId());
        LOG.debug("I {} know that {} has to replicate {}.", selfAddress, peerAddress, otherResponsibleLocations);
        Collection<Number160> myResponsibleLocations = backend.findContentForResponsiblePeerID(selfAddress
                .getPeerId());
        LOG.debug("I {} have to replicate {}.", selfAddress, myResponsibleLocations);
		if (!nRootReplication) {
			// check if we are now responsible for content where the other peer
			// was responsible
			for (Number160 otherResponsibleLocation : otherResponsibleLocations) {
				PeerAddress closest = closest(otherResponsibleLocation);
				if (closest.getPeerId().equals(selfAddress.getPeerId())) {
					if (backend.updateResponsibilities(otherResponsibleLocation,
							closest.getPeerId())) {
						LOG.debug("I {} am responsible for {} after leaving of {}.", selfAddress,
								otherResponsibleLocations, peerAddress);
						notifyMeResponsible(otherResponsibleLocation);
						// we don't need to check this again, so remove it from
						// the list if present
						myResponsibleLocations.remove(otherResponsibleLocation);
					} else {
						LOG.debug("I {} already know that I'm responsible for {} after leaving of {}.",
								selfAddress, otherResponsibleLocations, peerAddress);
					}
				} else {
					if (backend.updateResponsibilities(otherResponsibleLocation,
							closest.getPeerId())) {
						LOG.debug("We should check if the closer peer has the content");
						notifyOtherResponsible(otherResponsibleLocation, closest, true);
					}
				}
			}
			// now check for our responsibilities. If a peer is gone and it was
			// in the replication range, we need make sure
			// we have enough copies
			for (Number160 myResponsibleLocation : myResponsibleLocations) {
				if (isInReplicationRange(myResponsibleLocation, peerAddress, replicationFactor)) {
					LOG.debug(
							"Leaving {} affects my {} replication responsiblity for {}.",
							selfAddress, peerAddress, myResponsibleLocation);
					notifyMeResponsible(myResponsibleLocation);
				} else {
					LOG.debug("Leaving {} doesn't affect my {} replication responsibility for {}.",
							peerAddress, selfAddress, myResponsibleLocation);
				}
			}
		} else {
			// check if we are now responsible for content where the other peer
			// was responsible
			for (Number160 otherResponsibleLocation : otherResponsibleLocations) {
				if (isInReplicationRange(otherResponsibleLocation, selfAddress, replicationFactor)) {
					if (backend.updateResponsibilities(otherResponsibleLocation,
							selfAddress.getPeerId())) {
						LOG.debug(
								"I {} didn't know my replication responsiblity for {}, which gets discharged by leaving {}.",
								selfAddress, otherResponsibleLocation, peerAddress);
						// notify that someone I'm now responsible for the
						// content with key responsibleLocations
						notifyMeResponsible(otherResponsibleLocation);
						// we don't need to check this again, so remove it from
						// the list if present
						myResponsibleLocations.remove(otherResponsibleLocation);
					} else {
						LOG.debug(
								"I {} already know my replication responsiblity for {}, which gets discharged by leaving {}.",
								selfAddress, otherResponsibleLocation, peerAddress);
					}
				} else {
					LOG.debug("I {} don't have {}'s replication responsiblity for {}.", selfAddress,
							peerAddress, otherResponsibleLocation);
				}
				// remove stored replication responsibility of leaving node
				backend.removeResponsibility(otherResponsibleLocation, peerAddress.getPeerId());
			}
			// now check for our responsibilities. If a peer is gone and it was
			// in the replication range, we need make sure we have enough copies
			for (Number160 myResponsibleLocation : myResponsibleLocations) {
				if (isInReplicationRange(myResponsibleLocation, peerAddress, replicationFactor)) {
					LOG.debug(
							"I {} realized that leaving {} had also replication responsibility for {}."
							+ " The replica set has to get notified about the leaving replica node.",
							selfAddress, peerAddress, myResponsibleLocation);
					notifyMeResponsible(myResponsibleLocation);
				} else {
					LOG.debug("Leaving {} doesn't affect my {} replication responsibility for {}.",
							peerAddress, selfAddress, myResponsibleLocation);
				}
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
        tmp.add(selfAddress);
        return tmp.headSet(peerAddress).size() < replicationFactor;
    }
}
