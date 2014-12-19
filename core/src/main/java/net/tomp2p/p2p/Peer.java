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
package net.tomp2p.p2p;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.PeerCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.p2p.builder.AnnounceBuilder;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.p2p.builder.DiscoverBuilder;
import net.tomp2p.p2p.builder.PingBuilder;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.p2p.builder.ShutdownBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.AnnounceRPC;
import net.tomp2p.rpc.BroadcastRPC;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.PingRPC;
import net.tomp2p.rpc.QuitRPC;
import net.tomp2p.rpc.RawDataReply;

/**
 * This is the main class to start DHT operations. This class makes use of the build pattern and for each DHT operation,
 * a builder class is returned. The main operations can be initiated with {@link #put(Number160)},
 * {@link #get(Number160)}, {@link #add(Number160)}, {@link #addTracker(Number160)}, {@link #getTracker(Number160)},
 * {@link #remove(Number160)}, {@link #submit(Number160, Worker)}, {@link #send(Number160)}, {@link #sendDirect()},
 * {@link #broadcast(Number160)}. Each of those operations return a builder that offers more options. One of the main
 * difference to a "regular" DHT is that TomP2P can store a map (key-values) instead of just values. To distinguish 
 * those, the keys are termed location key (for finding the right peer in the network), and content key (to store more 
 * than one value on a peer). For the put builder e.g. the following options can be set: 
 * 
 * <ul>
 * <li>{@link PutBuilder#setData(Number160, net.tomp2p.storage.Data)} - puts a content key with a value</li>
 * <li>{@link PutBuilder#dataMap(Map) - puts multiple content key / values at once</li>
 * <li>{@link PutBuilder#setPutIfAbsent() - only puts data if its not already on the peers</li>
 * <li>...</li>
 * </ul>
 * 
 * Furthermore, TomP2P also provides to store keys in different domains to avoid key collisions 
 * ({@link PutBuilder#setDomainKey(Number160)).
 * 
 * @author Thomas Bocek
 */
public class Peer {
    // As soon as the user calls listen, this connection handler is set
    private final PeerCreator peerCreator;
    // the id of this node
    private final Number160 peerId;
    // the p2p network identifier, two different networks can have the same
    // ports
    private final int p2pId;

    // Distributed
    private DistributedRouting distributedRouting;

    // RPC
    private PingRPC pingRCP;
    private QuitRPC quitRPC;
    private NeighborRPC neighborRPC;
    private DirectDataRPC directDataRPC;
    private BroadcastRPC broadcastRPC;
    private AnnounceRPC announceRPC;

    private volatile boolean shutdown = false;
    
    private List<AutomaticFuture> automaticFutures = Collections.synchronizedList(new ArrayList<AutomaticFuture>(1)); 
    private List<Shutdown> shutdownListeners = Collections.synchronizedList(new ArrayList<Shutdown>(5)); 

    /**
     * Create a peer. Please use {@link PeerBuilder} to create a class
     * 
     * @param p2pID
     *            The P2P ID
     * @param peerId
     *            The Id of the peer
     * @param peerCreator
     *            The peer creator that holds the peer bean and connection bean
     */
    Peer(final int p2pID, final Number160 peerId, final PeerCreator peerCreator) {
        this.p2pId = p2pID;
        this.peerId = peerId;
        this.peerCreator = peerCreator;
    }

    PeerCreator peerCreator() {
        return peerCreator;
    }

    public PingRPC pingRPC() {
        if (pingRCP == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return pingRCP;
    }

    public Peer pingRPC(PingRPC pingRCP) {
        this.pingRCP = pingRCP;
        return this;
    }
    
    public QuitRPC quitRPC() {
    	if (quitRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return quitRPC;    
    }
    
    public Peer quitRPC(QuitRPC quitRPC) {
    	this.quitRPC = quitRPC;
        return this;
    }

    public NeighborRPC neighborRPC() {
        if (neighborRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return neighborRPC;
    }

    public Peer neighborRPC(NeighborRPC neighborRPC) {
        this.neighborRPC = neighborRPC;
        return this;
    }

    public DirectDataRPC directDataRPC() {
        if (directDataRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return directDataRPC;
    }

    public Peer directDataRPC(DirectDataRPC directDataRPC) {
        this.directDataRPC = directDataRPC;
        return this;
    }

    public BroadcastRPC broadcastRPC() {
        if (broadcastRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return broadcastRPC;
    }
    
    public Peer broadcastRPC(BroadcastRPC broadcastRPC) {
        this.broadcastRPC = broadcastRPC;
        return this;
    }
    
    public AnnounceRPC announceRPC() {
        if (announceRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return announceRPC;
    }
    
    public Peer announceRPC(AnnounceRPC announceRPC) {
        this.announceRPC = announceRPC;
        return this;
    }

    public DistributedRouting distributedRouting() {
        if (distributedRouting == null) {
            throw new RuntimeException("Not enabled, please enable this P2P function in PeerMaker");
        }
        return distributedRouting;
    }

    public void distributedRouting(DistributedRouting distributedRouting) {
        this.distributedRouting = distributedRouting;
    }

    public PeerBean peerBean() {
        return peerCreator.peerBean();
    }

    public ConnectionBean connectionBean() {
        return peerCreator.connectionBean();
    }

    public Number160 peerID() {
        return peerId;
    }

    public int p2pId() {
        return p2pId;
    }

    public PeerAddress peerAddress() {
        return peerBean().serverPeerAddress();
    }
    
    public Peer notifyAutomaticFutures(BaseFuture future) {
    	synchronized (automaticFutures) {
    		for(AutomaticFuture automaticFuture:automaticFutures) {
                automaticFuture.futureCreated(future);
    		}
        }
        return this;
    }

    // *********************************** Basic P2P operation starts here

    public void rawDataReply(final RawDataReply rawDataReply) {
        directDataRPC().rawDataReply(rawDataReply);
    }

    public void objectDataReply(final ObjectDataReply objectDataReply) {
        directDataRPC().objectDataReply(objectDataReply);
    }
    
    public FuturePeerConnection createPeerConnection(final PeerAddress destination) {
    	return createPeerConnection(destination, PeerConnection.HEART_BEAT_MILLIS);
    }

    /**
     * Opens a TCP connection and keeps it open. The user can provide the idle timeout, which means that the connection
     * gets closed after that time of inactivity. If the other peer goes offline or closes the connection (due to
     * inactivity), further requests with this connections reopens the connection. This method blocks until a
     * connection can be reserved.
     * 
     * @param destination
     *            The end-point to connect to
     * @param heartBeatMillis
     *            // TODO update doc
     *            time in milliseconds after a connection gets closed if idle, -1 if it should remain always open until
     *            the user closes the connection manually.
     * @return A class that needs to be passed to those methods that should use the already open connection. If the
     *         connection could not be reserved, maybe due to a shutdown, null is returned.
     */
    public FuturePeerConnection createPeerConnection(final PeerAddress destination, final int heartBeatMillis) {
        final FuturePeerConnection futureDone = new FuturePeerConnection(destination);
        final FutureChannelCreator fcc = connectionBean().reservation().createPermanent(1);
        fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    final ChannelCreator cc = fcc.channelCreator();
                    final PeerConnection peerConnection = new PeerConnection(destination, cc, heartBeatMillis);
                    futureDone.done(peerConnection);
                } else {
                    futureDone.failed(future);
                }
            }
        });
        return futureDone;
    }

    // -------------------------------------------------- Direct, bootstrap, ping and broadcast   

    public SendDirectBuilder sendDirect(PeerAddress recipientAddress) {
        return new SendDirectBuilder(this, recipientAddress);
    }

    public SendDirectBuilder sendDirect(FuturePeerConnection recipientConnection) {
        return new SendDirectBuilder(this, recipientConnection);
    }
    
    public SendDirectBuilder sendDirect(PeerConnection peerConnection) {
        return new SendDirectBuilder(this, peerConnection);
    }
    
    public BootstrapBuilder bootstrap() {
        return new BootstrapBuilder(this);
    }
    
    /**
	 * Sends a friendly shutdown message to my close neighbors in the DHT.
	 * 
	 * @return A builder for shutdown that runs asynchronous.
	 */
    public ShutdownBuilder announceShutdown() {
        return new ShutdownBuilder(this);
    }

    public PingBuilder ping() {
        return new PingBuilder(this);
    }

    public DiscoverBuilder discover() {
        return new DiscoverBuilder(this);
    }

    public BroadcastBuilder broadcast(Number160 messageKey) {
        return new BroadcastBuilder(this, messageKey);
    }

    /**
     * Shuts down everything.
     * 
     * @return The future, when shutdown is completed
     */
    public BaseFuture shutdown() {
        //prevent the shutdown from being called twice
        if (!shutdown) {
            shutdown = true;
            
            final List<Shutdown> copy;
            synchronized (shutdownListeners) {
            	copy = new ArrayList<Shutdown>(shutdownListeners);
            }
            final FutureLateJoin<BaseFuture> futureLateJoin = new FutureLateJoin<BaseFuture>(shutdownListeners.size() + 1);
            for(Shutdown shutdown:copy) {
            	futureLateJoin.add(shutdown.shutdown());
            	removeShutdownListener(shutdown);
            }
            futureLateJoin.add(peerCreator.shutdown());
            return futureLateJoin;
        } else {
            return new FutureDone<Void>().failed("already shutting / shut down");
        }
    }

    /**
     * @return True if the peer is about or already has shutdown
     */
    public boolean isShutdown() {
        return shutdown;
    }

	public Peer addShutdownListener(Shutdown shutdown) {
		shutdownListeners.add(shutdown);
		return this;
    }
	
	public Peer removeShutdownListener(Shutdown shutdown) {
		shutdownListeners.remove(shutdown);
		return this;
	}
	
	public Peer addAutomaticFuture(AutomaticFuture automaticFuture) {
		automaticFutures.add(automaticFuture);
		return this;
    }
	
	public Peer removeAutomaticFuture(AutomaticFuture automaticFuture) {
		automaticFutures.remove(automaticFuture);
		return this;
	}

	public AnnounceBuilder localAnnounce() {
	    return new AnnounceBuilder(this);
    }	
}
