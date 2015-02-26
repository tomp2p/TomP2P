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
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.ReplicationListener;
import net.tomp2p.dht.StorageLayer;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapChangeListener;
import net.tomp2p.peers.PeerStatistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has 3 methods that are called from outside eventes: check, peerInsert, peerRemoved.
 */
public class Replication implements PeerMapChangeListener, ReplicationListener {
    private static final Logger LOG = LoggerFactory.getLogger(Replication.class);

    private final List<ResponsibilityListener> listeners = new ArrayList<ResponsibilityListener>();

    private final PeerMap peerMap;

    private final PeerAddress selfAddress;

    private final StorageLayer backend;

    private int replicationFactor;

    private boolean nRootReplication;

    private boolean keepData;
    
    final PeerDHT peer;

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
     * @param keepData
     *           flag indicating if data will be kept in memory after loss of replication responsibility
     */
    public Replication(final PeerDHT peer, final int replicationFactor, final boolean nRoot, final boolean keepData) {
    	this.peer = peer;
    	this.backend = peer.storageLayer();
        this.selfAddress = peer.peerAddress();
        this.peerMap = peer.peer().peerBean().peerMap();
        this.replicationFactor = replicationFactor;
        this.nRootReplication = nRoot;
        this.keepData = keepData;
        peerMap.addPeerMapChangeListener(this);
    }
    
    /**
     * 
     * @return The replication factor.
     */
    public Replication replicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
        return this;
    }

    /**
     * 
     * @return The replication factor.
     */
    public int replicationFactor() {
        return replicationFactor;
    }

	/**
	 * 
	 * @param nRootReplication flag
	 *            Set <code>true</code> for n-root replication or <code>false</code> for 0-root replication.
	 */
	public void nRootReplication(boolean nRootReplication) {
		this.nRootReplication = nRootReplication;
	}

	/**
	 * 
	 * @return <code>true</code> if n-root replication is enabled
	 */
	public boolean isNRootReplication() {
		return nRootReplication;
	}
	
	/**
	 * 
	 * @return <code>true</code> if 0-root replication is enabled
	 */
	public boolean is0RootReplication() {
		return !nRootReplication;
	}

	/**
	 * 
	 * @param keepData flag
	 *            If <code>true</code> data will be kept in memory after loosing replication responsibility.
	 *            If <code>false</code> data will be deleted.
	 */
	public void keepData(boolean keepData) {
		this.keepData = keepData;
	}

	/**
	 * 
	 * @return <code>true</code> if data will keep in memory after loosing replication responsibility
	 */
	public boolean isKeepingData() {
		return keepData;
	}

    /**
     * Checks if the user enabled replication.
     * 
     * @return True if replication is enabled.
     */
    public boolean isReplication() {
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
	private FutureForkJoin<FutureDone<Void>> notifyMeResponsible(final Number160 locationKey) {
		final FutureDone[] futureDones = new FutureDone[listeners.size()];
		int index = 0;
		for (ResponsibilityListener responsibilityListener : listeners) {
			futureDones[index++] = responsibilityListener.meResponsible(locationKey);
		}
		final FutureForkJoin<FutureDone<Void>> retVal = new FutureForkJoin<FutureDone<Void>>(
		        new AtomicReferenceArray<FutureDone<Void>>(futureDones));
		peer.peer().notifyAutomaticFutures(retVal);
		return retVal;
	}
    
	private FutureForkJoin<FutureDone<Void>> notifyMeResponsible(final Number160 locationKey, PeerAddress newPeer) {
		final FutureDone[] futureDones = new FutureDone[listeners.size()];
		int index = 0;
		for (ResponsibilityListener responsibilityListener : listeners) {
			futureDones[index++] = responsibilityListener.meResponsible(locationKey, newPeer);
		}
		final FutureForkJoin<FutureDone<Void>> retVal = new FutureForkJoin<FutureDone<Void>>(
		        new AtomicReferenceArray<FutureDone<Void>>(futureDones));
		peer.peer().notifyAutomaticFutures(retVal);
		return retVal;
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
	private FutureForkJoin<FutureDone<Void>> notifyOtherResponsible(final Number160 locationKey, final PeerAddress other) {
		final FutureDone[] futureDones = new FutureDone[listeners.size()];
		int index = 0;
		for (ResponsibilityListener responsibilityListener : listeners) {
			futureDones[index++] = responsibilityListener.otherResponsible(locationKey, other);
		}
		final FutureForkJoin<FutureDone<Void>> retVal = new FutureForkJoin<FutureDone<Void>>(
		        new AtomicReferenceArray<FutureDone<Void>>(futureDones));
		peer.peer().notifyAutomaticFutures(retVal);
		return retVal;
	}
    
    @Override
    public void dataRemoved(Number160 locationKey) {
    	if (!isReplication()) {
            return;
        }
	    // TODO Auto-generated method stub
    }

    /**
     * Update responsibilities. This happens for a put / add.
     * 
     * @param locationKey
     *            The location key.
     */
    @Override
    public void dataInserted(final Number160 locationKey) {
        if (!isReplication()) {
            return;
        }

        if (!nRootReplication) {
            PeerAddress closest = closest(locationKey);
            if (closest.peerId().equals(selfAddress.peerId())) {
                if (backend.updateResponsibilities(locationKey, closest.peerId())) {
                    // I am responsible for this content
                	FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyMeResponsible(locationKey);
                	peer.peer().notifyAutomaticFutures(futureForkJoin);
                }
            } else {
                if (backend.updateResponsibilities(locationKey, closest.peerId())) {
                    // notify that someone else is now responsible for the
                    // content with key responsibleLocations
                	FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyOtherResponsible(locationKey, closest);
                	peer.peer().notifyAutomaticFutures(futureForkJoin);
                }
            }
        } else {
			if (isInReplicationRange(locationKey, selfAddress, replicationFactor)) {
				if (backend.updateResponsibilities(locationKey, selfAddress.peerId())) {
					LOG.debug("I {} am now responsible for key {}.", selfAddress, locationKey);
					FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyMeResponsible(locationKey);
					peer.peer().notifyAutomaticFutures(futureForkJoin);
				}
			} else {
				LOG.debug("I {} am not responsible for key {}.", selfAddress, locationKey);
				final PeerAddress closest = closest(locationKey);
				// Try to notify another replica node.
				FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyOtherResponsible(
						locationKey, closest);
				// Remove responsibility after successful notifying.
				futureForkJoin
						.addListener(new BaseFutureListener<BaseFuture>() {
							@Override
							public void operationComplete(BaseFuture future) throws Exception {
								if (future.isSuccess()) {
									backend.removeResponsibility(
											locationKey,
											keepData);
								} else {
									LOG.debug(
											"I {} couldn't notify newly joined peer {} about responsibility for {}."
													+ " I keep responsibility.",
											selfAddress, closest,
											locationKey);
								}
							}
							@Override
							public void exceptionCaught(Throwable t)
									throws Exception {
								LOG.error("Unexcepted exception ocurred.", t);
							}
						});
				peer.peer().notifyAutomaticFutures(futureForkJoin);
			}
		}
    }

    @Override
    public void peerInserted(final PeerAddress peerAddress, final boolean verified) {
        if (!isReplication() || !verified) {
            return;
        }
        LOG.debug("The peer {} was inserted in my map. I'm {}", peerAddress, selfAddress);
        // check if we should change responsibility.
        Collection<Number160> myResponsibleLocations = backend.findContentForResponsiblePeerID(selfAddress
                .peerId());
        LOG.debug("I {} have to check replication responsibilities for {}.", selfAddress, myResponsibleLocations);
        
        for (final Number160 myResponsibleLocation : myResponsibleLocations) {
			if (!nRootReplication) {
				// use 0-root replication strategy
				PeerAddress closest = closest(myResponsibleLocation);
				if (!closest.peerId().equals(selfAddress.peerId())) {
					if (isInReplicationRange(myResponsibleLocation, selfAddress, replicationFactor)) {
    					if (backend.updateResponsibilities(myResponsibleLocation, closest.peerId())) {
    						LOG.debug("I {} didn't know that {} is responsible for {}.", selfAddress, closest,
    								myResponsibleLocation);
    						// notify that someone else is now responsible for the
    						// content with key responsibleLocations
    						FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyOtherResponsible(myResponsibleLocation, closest);
    						peer.peer().notifyAutomaticFutures(futureForkJoin);
    						// cancel any pending notifyMeResponsible*, as we are
    						// not responsible anymore.
    					} else {
    						LOG.debug("I {} know already that {} is responsible for {}.", selfAddress, closest,
    								myResponsibleLocation);
    					}
					} else {
						// notify closest replica node about responsibility
						FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyOtherResponsible(myResponsibleLocation, closest);
						peer.peer().notifyAutomaticFutures(futureForkJoin);
						LOG.debug("I {} am no more in the replica set of {}.", selfAddress, myResponsibleLocation);
						backend.removeResponsibility(myResponsibleLocation, keepData);
					}
				} else if (isInReplicationRange(myResponsibleLocation, peerAddress, replicationFactor)) {
					LOG.debug("{} is in the replica set for {}.", peerAddress, myResponsibleLocation);
					// we are still responsible, but a new peer joined and if it
					// is within the x close peers, we need to
					// replicate
					if (backend.updateResponsibilities(myResponsibleLocation,
							selfAddress.peerId())) {
						LOG.debug("I {} didn't know that I'm responsible for {}.", selfAddress,
								myResponsibleLocation);
						// I figured out I'm the new responsible, so check all
						// my peer in the replication range
						FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyMeResponsible(myResponsibleLocation);
						peer.peer().notifyAutomaticFutures(futureForkJoin);
					} else {
						LOG.debug("I {} already know that I'm responsible for {}.", selfAddress,
								myResponsibleLocation);
						// new peer joined, I'm responsible, so replicate to
						// that peer
						FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyMeResponsible(myResponsibleLocation, peerAddress);
						peer.peer().notifyAutomaticFutures(futureForkJoin);
					}
				}
			} else {
				// check if newly joined peer has duty to replicate
				if (isInReplicationRange(myResponsibleLocation, peerAddress, replicationFactor)) {
					// check if I still have to replicate
					if (isInReplicationRange(myResponsibleLocation, selfAddress, replicationFactor)) {
						LOG.debug("I {} and newly joined peer {} have replication responibility for {}.",
								selfAddress, peerAddress, myResponsibleLocation);
						// newly joined peer has to get notified
						FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyMeResponsible(myResponsibleLocation, peerAddress);
						peer.peer().notifyAutomaticFutures(futureForkJoin);
					} else {
						LOG.debug("I {} lose and newly joined peer {} gets replication responsibility for {}.",
								selfAddress, peerAddress, myResponsibleLocation);
						// newly joined peer has to get notified
						FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyOtherResponsible(
								myResponsibleLocation, peerAddress);
						// I'm not in replication range, I don't need to know
						// about all responsibility entries to the given key.
						// Remove responsibility after notifying newly joined peer.
						futureForkJoin.addListener(new BaseFutureListener<BaseFuture>() {
							@Override
							public void operationComplete(BaseFuture future)
									throws Exception {
										if (future.isSuccess()) {
											backend.removeResponsibility(
													myResponsibleLocation,
													keepData);
										} else {
											LOG.debug(
													"I {} couldn't notify newly joined peer {} about responsibility for {}."
															+ " I keep responsibility.",
													selfAddress, peerAddress,
													myResponsibleLocation);
										}
							}
							@Override
							public void exceptionCaught(Throwable t)
									throws Exception {
								LOG.error("Unexcepted exception ocurred.", t);
							}
						});
						peer.peer().notifyAutomaticFutures(futureForkJoin);
					}
				} else {
					// check if I still have to replicate
					if (!isInReplicationRange(myResponsibleLocation,
							selfAddress, replicationFactor)) {
						LOG.debug(
								"I {} and newly joined peer {} don't have to replicate {}.",
								selfAddress, peerAddress, myResponsibleLocation);
						// I'm not in replication range, I don't need to know
						// about all responsibility entries to the given key
						final PeerAddress closest = closest(myResponsibleLocation);
						// Try to notify another replica node.
						FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyOtherResponsible(
								myResponsibleLocation, closest);
						// Remove responsibility after successful notifying.
						futureForkJoin
								.addListener(new BaseFutureListener<BaseFuture>() {
									@Override
									public void operationComplete(BaseFuture future) throws Exception {
										if (future.isSuccess()) {
											backend.removeResponsibility(
													myResponsibleLocation,
													keepData);
										} else {
											LOG.debug(
													"I {} couldn't notify newly joined peer {} about responsibility for {}."
															+ " I keep responsibility.",
													selfAddress, closest,
													myResponsibleLocation);
										}
									}
									@Override
									public void exceptionCaught(Throwable t)
											throws Exception {
										LOG.error("Unexcepted exception ocurred.", t);
									}
								});
						peer.peer().notifyAutomaticFutures(futureForkJoin);
					}
				}
			}
		}
    }

    @Override
    public void peerRemoved(final PeerAddress peerAddress, final PeerStatistic peerStatatistic) {
        if (!isReplication()) {
            return;
        }
        LOG.debug("The peer {} was removed from my map. I'm {}", peerAddress, selfAddress);
        // check if we should change responsibility.
        Collection<Number160> otherResponsibleLocations = backend
                .findContentForResponsiblePeerID(peerAddress.peerId());
        LOG.debug("I {} know that {} has to replicate {}.", selfAddress, peerAddress, otherResponsibleLocations);
        Collection<Number160> myResponsibleLocations = backend.findContentForResponsiblePeerID(selfAddress
                .peerId());
        LOG.debug("I {} have to replicate {}.", selfAddress, myResponsibleLocations);
		if (!nRootReplication) {
			// check if we are now responsible for content where the other peer
			// was responsible
			for (Number160 otherResponsibleLocation : otherResponsibleLocations) {
				PeerAddress closest = closest(otherResponsibleLocation);
				if (closest.peerId().equals(selfAddress.peerId())) {
					if (backend.updateResponsibilities(otherResponsibleLocation,
							closest.peerId())) {
						LOG.debug("I {} am responsible for {} after leaving of {}.", selfAddress,
								otherResponsibleLocations, peerAddress);
						FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyMeResponsible(otherResponsibleLocation);
						peer.peer().notifyAutomaticFutures(futureForkJoin);
						// we don't need to check this again, so remove it from
						// the list if present
						myResponsibleLocations.remove(otherResponsibleLocation);
					} else {
						LOG.debug("I {} already know that I'm responsible for {} after leaving of {}.",
								selfAddress, otherResponsibleLocations, peerAddress);
					}
				} else {
					if (backend.updateResponsibilities(otherResponsibleLocation,
							closest.peerId())) {
						LOG.debug("We should check if the closer peer has the content");
						FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyOtherResponsible(otherResponsibleLocation, closest);
						peer.peer().notifyAutomaticFutures(futureForkJoin);
						// we don't need to check this again, so remove it from
						// the list if present
						myResponsibleLocations.remove(otherResponsibleLocation);
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
					FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyMeResponsible(myResponsibleLocation);
					peer.peer().notifyAutomaticFutures(futureForkJoin);
				} else {
					LOG.debug("Leaving {} doesn't affect my {} replication responsibility for {}.",
							peerAddress, selfAddress, myResponsibleLocation);
				}
			}
		} else {
            // Check for our responsibilities. If a peer is gone and it was
            // in the replication range, we need make sure we have enough copies
            for (Number160 myResponsibleLocation : myResponsibleLocations) {
                if (isInReplicationRange(myResponsibleLocation, peerAddress,
                        replicationFactor)) {
                    LOG.debug(
                            "I {} realized that leaving {} had also replication responsibility for {}."
                                    + " The replica set has to get notified about the leaving replica node.",
                            selfAddress, peerAddress, myResponsibleLocation);
                    FutureForkJoin<FutureDone<Void>> futureForkJoin = notifyMeResponsible(myResponsibleLocation);
                    peer.peer().notifyAutomaticFutures(futureForkJoin);
                } else {
                    LOG.debug(
                            "Leaving {} doesn't affect my {} replication responsibility for {}.",
                            peerAddress, selfAddress, myResponsibleLocation);
                }
            }
        }
    }

    @Override
    public void peerUpdated(final PeerAddress peerAddress, final PeerStatistic peerStatatistic) {
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
        SortedSet<PeerStatistic> tmp = peerMap.closePeers(locationKey, 1);
        tmp.add(new PeerStatistic(selfAddress));
        return tmp.iterator().next().peerAddress();
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
        SortedSet<PeerStatistic> tmp = peerMap.closePeers(locationKey, replicationFactor);
        PeerStatistic peerAddressStatistic = new PeerStatistic(peerAddress);
        PeerStatistic selfStatistic = new PeerStatistic(selfAddress);
        tmp.add(selfStatistic);
        SortedSet<PeerStatistic> tmp2 = tmp.headSet(peerAddressStatistic);
        return tmp2.size() < replicationFactor;
    }	
}
