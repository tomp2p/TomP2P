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
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.PutBuilder;
import net.tomp2p.dht.StorageRPC;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.ResponsibilityListener;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.storage.Data;
import net.tomp2p.synchronization.PeerSync;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implements the default indirect replication.
 * 
 * @author Thomas Bocek
 * @author Maxat Pernebayev
 * 
 */
public class IndirectReplication implements ResponsibilityListener, Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(IndirectReplication.class);
    private static final int DEFAULT_REPLICATION_FACTOR = 6;

    private final PeerDHT peer;
    
    private boolean autoReplication = false;
    private ReplicationFactor replicationFactor;
    private int delayMillis = -1;
    private int intervalMillis = -1;
    private boolean rsync = false;
    private int blockSize = -1;
    private ReplicationSender replicationSender;
    private boolean nRoot = false;
    private boolean keepData = false;
    private Replication replication;
    
    private ScheduledFuture<?> scheduledFuture;
    
    private List<ResponsibilityListener> responsibilityListeners = null;
    
    public IndirectReplication(PeerDHT peer) {
    	this.peer = peer;

		peer.peer().addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				IndirectReplication.this.shutdown();
				return new FutureDone<Void>().done();
			}
		});
    }
    
    public boolean isAutoReplication() {
    	return autoReplication;
    }
    
    public IndirectReplication autoReplication(boolean autoReplication) {
    	this.autoReplication = autoReplication;
    	return this;
    }
    
    public IndirectReplication autoReplication() {
    	this.autoReplication = true;
    	return this;
    }
    
    public boolean isRsync() {
    	return rsync;
    }
    
    public IndirectReplication rsync(boolean rsync) {
    	this.rsync = rsync;
    	return this;
    }
    
    public IndirectReplication rsync() {
    	this.rsync = true;
    	return this;
    }
    
    public boolean isNRoot() {
    	return nRoot;
    }
    
    public IndirectReplication nRoot(boolean nRoot) {
    	this.nRoot = nRoot;
    	return this;
    }
    
    public IndirectReplication nRoot() {
    	this.nRoot = true;
    	return this;
    }
    
    public boolean isKeepingData() {
    	return keepData;
    }
    
    public IndirectReplication keepData(boolean keepData) {
    	this.keepData = keepData;
    	return this;
    }
    
	/**
	 * Replicated and stored data will be not deleted after loss of replication responsibility.
	 */
    public IndirectReplication keepData() {
    	this.keepData = false;
    	return this;
    }
    public IndirectReplication replicationFactor(ReplicationFactor replicationFactor) {
    	this.replicationFactor = replicationFactor;
    	return this;
    }
    
    public IndirectReplication replicationFactor(final int replicationFactor) {
    	this.replicationFactor = new ReplicationFactor() {
			@Override
			public int replicationFactor() {
				return replicationFactor;
			}
		};
    	return this;
    }
    
    public ReplicationFactor replicationFactor() {
    	return replicationFactor;
    }
    
    public IndirectReplication delayMillis(int delayMillis) {
    	this.delayMillis = delayMillis;
    	return this;
    }
    
    public int delayMillis() {
    	return delayMillis;
    }
    
    public IndirectReplication intervalMillis(int intervalMillis) {
    	this.intervalMillis = intervalMillis;
    	return this;
    }
    
    public int intervalMillis() {
    	return intervalMillis;
    }
    
    public IndirectReplication blockSize(int blockSize) {
    	this.blockSize = blockSize;
    	return this;
    }
    
    public int blockSize() {
    	return blockSize;
    }
    
    public IndirectReplication start() {
    	
    	if (intervalMillis == -1) {
			intervalMillis = 60 * 1000;
		}
		if (delayMillis == -1) {
			delayMillis = 30 * 1000;
		}
		if (blockSize == -1) {
			blockSize = 700;
		}
    	
    	if(autoReplication) {
    		replicationFactor = new AutoReplication(peer.peer()); 
    	} else if (replicationFactor == null) {
    		replicationFactor = new ReplicationFactor() {
				@Override
				public int replicationFactor() {
					return DEFAULT_REPLICATION_FACTOR;
				}
			};
    	}
    	
    	this.replication = new Replication(peer, replicationFactor.replicationFactor(), nRoot, keepData);
    	this.replication.addResponsibilityListener(this);
    	if(responsibilityListeners!=null) {
    		for(ResponsibilityListener responsibilityListener:responsibilityListeners) {
    			this.replication.addResponsibilityListener(responsibilityListener);
    		}
    		responsibilityListeners = null;
    	}
    	peer.storeRPC().replicationListener(replication);
    	
		if(rsync) {
			replicationSender = new PeerSync(peer, replication, blockSize);
		} else if (replicationSender == null) {
			replicationSender = new DefaultReplicationSender(peer);
		}
    	
    	scheduledFuture = peer.peer().connectionBean().timer().scheduleAtFixedRate(
    			this, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    	return this;
    }
    
    public IndirectReplication addResponsibilityListener(final ResponsibilityListener responsibilityListener) {
    	if(replication == null) {
    		if(responsibilityListeners == null) {
    			responsibilityListeners = new ArrayList<ResponsibilityListener>();    			
    		}
    		responsibilityListeners.add(responsibilityListener);
    	} else {
    		replication.addResponsibilityListener(responsibilityListener);
    	}
    	return this;
    }
    
    public IndirectReplication removeResponsibilityListener(final ResponsibilityListener responsibilityListener) {
    	if(replication == null) {
    		if(responsibilityListeners != null) {
    			responsibilityListeners.remove(responsibilityListener);
    		}
    	} else {
    		replication.removeResponsibilityListener(responsibilityListener);
    	}
    	return this;
    }

    

    @Override
    public FutureDone<?> otherResponsible(final Number160 locationKey, final PeerAddress other) {

        LOG.debug("Other peer {} is responsible for {}. I'm {}", other, locationKey, peer.peerAddress());
        
        Number640 min = new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO);
        Number640 max = new Number640(locationKey, Number160.MAX_VALUE, Number160.MAX_VALUE,
        		Number160.MAX_VALUE);
        final NavigableMap<Number640, Data> dataMap = peer.storageLayer().get(min, max, -1, true);
        LOG.debug("transfer from {} to {} for key {}", peer.peerAddress(), other, locationKey);
        return replicationSender.sendDirect(other, locationKey, dataMap);
    }

    @Override
    public FutureDone<?> meResponsible(final Number160 locationKey) {
        LOG.debug("I ({}) now responsible for {}", peer.peerAddress(), locationKey);
        return synchronizeData(locationKey);
    }
    
    @Override
    public FutureDone<?> meResponsible(final Number160 locationKey, PeerAddress newPeer) {
        LOG.debug("I ({}) sync {} to {}", peer.peerAddress(), locationKey, newPeer);
        Number640 min = new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO);
        Number640 max = new Number640(locationKey, Number160.MAX_VALUE, Number160.MAX_VALUE,
                Number160.MAX_VALUE);
        final NavigableMap<Number640, Data> dataMap = peer.storageLayer().get(min, max, -1, true);
        return replicationSender.sendDirect(newPeer, locationKey, dataMap);
    }

    @Override
    public void run() {
        // we get called every x seconds for content we are responsible for. So
        // we need to make sure that there are enough copies. The easy way is to
        // publish it again... The good way is to do a diff
        Collection<Number160> locationKeys = peer.storageLayer().findContentForResponsiblePeerID(peer.peerID());
        
        for (Number160 locationKey : locationKeys) {
            synchronizeData(locationKey);
        }
        // recalculate replication factor
        int replicationFactor = IndirectReplication.this.replicationFactor.replicationFactor();
        replication.replicationFactor(replicationFactor);
    }

	public static String getVersionKeysFromMap(Map<Number640, Data> dataMap) {
		String result = "";
		for (Number640 key : dataMap.keySet()) {
			result += key.versionKey() + " ";
		}
		return result;
	}

    /**
     * Get the data that I'm responsible for and make sure that there are enough replicas.
     * 
     * @param locationKey
     *            The location key.
     */
    private FutureDone<?> synchronizeData(final Number160 locationKey) {
        Number640 min = new Number640(locationKey, Number160.ZERO, Number160.ZERO, Number160.ZERO);
        Number640 max = new Number640(locationKey, Number160.MAX_VALUE, Number160.MAX_VALUE,
                Number160.MAX_VALUE);
        final NavigableMap<Number640, Data> dataMap = peer.storageLayer().get(min, max, -1, true);
        return send(locationKey, dataMap);
        
    }

    /**
     * If my peer is responsible, I'll issue a put if absent to make sure all replicas are stored.
     * 
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param dataMapConverted
     *            The data to store
     * @return The future of the put
     */
    protected FutureDone<?> send(final Number160 locationKey, final NavigableMap<Number640, Data> dataMapConverted) {
        int replicationFactor = replication.replicationFactor() - 1;
        List<PeerAddress> closePeers = new ArrayList<PeerAddress>();
        SortedSet<PeerStatistic> sortedSet = peer.peerBean().peerMap()
                .closePeers(locationKey, replicationFactor);
        int count = 0;
        List<FutureDone<?>> retVal = new ArrayList<FutureDone<?>>(replicationFactor);
        for (PeerStatistic peerStatistic : sortedSet) {
            count++;
            closePeers.add(peerStatistic.peerAddress());
            retVal.add(replicationSender.sendDirect(peerStatistic.peerAddress(), locationKey, dataMapConverted));
            if (count == replicationFactor) {
                break;
            }
        }
        LOG.debug("[storage refresh] I ({}) restore {} to {}", peer.peerAddress(),
                locationKey, closePeers);
        return FutureDone.whenAll(retVal);
    }
    
    public void shutdown() {
    	if(scheduledFuture!=null) {
    		scheduledFuture.cancel(false);
    	}
    }

    private static class DefaultReplicationSender implements ReplicationSender {
        private StorageRPC storageRPC;
        private PeerDHT peer;
 
        private DefaultReplicationSender(PeerDHT peer) {
            this.peer = peer;
            this.storageRPC = peer.storeRPC();
        }

        /**
         * If an other peer is responsible, we send this peer our data, so that the other peer can take care of this.
         * 
         * @param other
         *            The other peer
         * @param locationKey
         *            The location key
         * @param domainKey
         *            The domain key
         * @param dataMapConvert
         *            The data to store
         */
        public FutureDone<Void> sendDirect(final PeerAddress other, final Number160 locationKey, final NavigableMap<Number640, Data> dataMap) {
            final FutureDone<Void> futureDone = new FutureDone<Void>();
        	FutureChannelCreator futureChannelCreator = peer.peer().connectionBean().reservation().create(0, 1);
        	Utils.addReleaseListener(futureChannelCreator, futureDone);
            futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(final FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        PutBuilder putBuilder = new PutBuilder(peer, locationKey);
                        putBuilder.dataMap(dataMap);
                        FutureResponse futureResponse = storageRPC.putReplica(other, putBuilder,
                                future.channelCreator());
                        futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
							@Override
							public void operationComplete(FutureResponse future)
									throws Exception {
								if(future.isSuccess()) {
									futureDone.done();	
								} else {
									futureDone.failed(future);
								}
							}
						});
                        peer.peer().notifyAutomaticFutures(futureResponse);
                    } else {
                    	futureDone.failed(future);
                        LOG.error("otherResponsible failed {}", future.failedReason());
                    }
                }
            });
            return futureDone;
        }   
    }
}
