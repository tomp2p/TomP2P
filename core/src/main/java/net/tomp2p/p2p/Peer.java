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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.DataSend;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.network.KCP;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.p2p.builder.DiscoverBuilder;
import net.tomp2p.p2p.builder.PingBuilder;
import net.tomp2p.p2p.builder.ShutdownBuilder;
import net.tomp2p.peers.Number256;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.*;
import net.tomp2p.utils.Pair;

/**
 * This is the main class to start DHT operations. This class makes use of the build pattern and for each DHT operation,
 * a builder class is returned. The main operations can be initiated with {put(Number256)},
 * {get(Number256)}, {add(Number256)}, {addTracker(Number256)}, {getTracker(Number256)},
 * {remove(Number256)}, {submit(Number256, Worker)}, {send(Number256)}, {sendDirect()},
 * {broadcast(Number256)}. Each of those operations return a builder that offers more options. One of the main
 * difference to a "regular" DHT is that TomP2P can store a map (key-values) instead of just values. To distinguish 
 * those, the keys are termed location key (for finding the right peer in the network), and content key (to store more 
 * than one value on a peer). For the put builder e.g. the following options can be set: 
 * 

 * 
 * Furthermore, TomP2P also provides to store keys in different domains to avoid key collisions 
 *
 * @author Thomas Bocek
 */
public class Peer {
    // As soon as the user calls listen, this connection handler is set
    private final PeerCreator peerCreator;
    // the id of this node
    private final Number256 peerId;
    // the p2p network identifier, two different networks can have the same
    // ports
    private final int p2pId;

    // Distributed
    private DistributedRouting distributedRouting;

    // RPC
    private PingRPC pingRCP;
    private QuitRPC quitRPC;
    private NeighborRPC neighborRPC;
    private BroadcastRPC broadcastRPC;
    private AbstractHolePRPC holePRPC;
    
    private volatile boolean shutdown = false;
    
    private List<AutomaticFuture> automaticFutures = Collections.synchronizedList(new ArrayList<AutomaticFuture>(1)); 
    private List<Shutdown> shutdownListeners = Collections.synchronizedList(new ArrayList<Shutdown>(5));


    /**
     * Creates a peer. Please use {@link PeerBuilder} to create a {@link Peer} instance.
     * 
     * @param p2pID
     *            The P2P ID
     * @param peerId
     *            The ID of the peer
     * @param peerCreator
     *            The peer creator that holds the peer bean and the connection bean
     */
    Peer(final int p2pID, final Number256 peerId, final PeerCreator peerCreator) {
        this.p2pId = p2pID;
        this.peerId = peerId;
        this.peerCreator = peerCreator;
    }

    PeerCreator peerCreator() {
        return peerCreator;
    }

    public PingRPC pingRPC() {
        if (pingRCP == null) {
            throw new RuntimeException("Ping RPC not enabled. Please enable this RPC in the PeerBuilder.");
        }
        return pingRCP;
    }

    public Peer pingRPC(PingRPC pingRCP) {
        this.pingRCP = pingRCP;
        return this;
    }
    
    public QuitRPC quitRPC() {
    	if (quitRPC == null) {
            throw new RuntimeException("Quit RPC not enabled. Please enable this RPC in the PeerBuilder.");
        }
        return quitRPC;    
    }
    
    public Peer quitRPC(QuitRPC quitRPC) {
    	this.quitRPC = quitRPC;
        return this;
    }

    public NeighborRPC neighborRPC() {
        if (neighborRPC == null) {
            throw new RuntimeException("Neighbor RPC not enabled. Please enable this RPC in the PeerBuilder.");
        }
        return neighborRPC;
    }

    public Peer neighborRPC(NeighborRPC neighborRPC) {
        this.neighborRPC = neighborRPC;
        return this;
    }

    public BroadcastRPC broadcastRPC() {
        if (broadcastRPC == null) {
            throw new RuntimeException("Broadcast RPC not enabled. Please enable this RPC in the PeerBuilder.");
        }
        return broadcastRPC;
    }
    
    public Peer broadcastRPC(BroadcastRPC broadcastRPC) {
        this.broadcastRPC = broadcastRPC;
        return this;
    }
    
    public Peer holePRPC(AbstractHolePRPC holePRPC) {
    	this.holePRPC = holePRPC;
    	return this;
    }
    
    public AbstractHolePRPC holePRPC() {
    	return this.holePRPC;
    }

    public DistributedRouting distributedRouting() {
        if (distributedRouting == null) {
            throw new RuntimeException("DistributedRouting not enabled. Please enable this P2P function in the PeerBuilder.");
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

    /**
     * The ID of this peer.
     * @return
     */
    public Number256 peerID() {
        return peerId;
    }

    /**
     * The P2P network identifier. Two different networks can use the same ports.
     * @return
     */
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

    public void dataReply(final int sessionId, DataStream dataStream) {
        connectionBean().channelServer().dataReply(sessionId, dataStream);
    }

    public DataSend sendDirect(final int sessionId, PeerAddress recipient, DataStream dataStream) {
        InetSocketAddress inetSocketAddress = recipient.createSocket(peerAddress());
        final KCP kcp = connectionBean().channelServer().openKCP(sessionId, inetSocketAddress);
        DataSend p = new DataSend() {
            @Override
            public void send(ByteBuffer buffer) {
                System.out.println("send "+buffer.remaining()+" kcp "+kcp);
                kcp.send(buffer);
            }
        };
        connectionBean().channelServer().dataReply(sessionId, inetSocketAddress.getAddress(), dataStream);
        return p;
    }



    // -------------------------------------------------- Direct, bootstrap, ping and broadcast
    
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

    /**
     * Returns a new builder for a broadcast, which can be used to send a message to all peers.
     * 
     * @param messageKey The message key *must* be unique for broadcast. In order to avoid 
     * duplicates, multiple messages with the same message key will be ignored, thus, subsequent 
     * broadcast may fail.
     * 
     * @return a new builder for a broadcast
     */
    public BroadcastBuilder broadcast(Number256 messageKey) {
        return new BroadcastBuilder(this, messageKey);
    }

    /**
     * Shuts down everything.
     * 
     * @return The future, when shutdown is completed
     */
    public BaseFuture shutdown() {
        // prevent shutdown from being called twice
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
            return new FutureDone<Void>().failed("Already shutting / shut down.");
        }
    }

    /**
     * @return True, if the peer is about to be shut down or has done so already.
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

    public Pair<Number256, byte[]> getPeerId(int recipientShortId) {
        return null;
    }
}
