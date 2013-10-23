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

import io.netty.util.ResourceLeakDetector;
import io.netty.util.Timer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.tomp2p.connection2.Bindings;
import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.connection2.PeerConnection;
import net.tomp2p.connection2.PeerCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.natpmp.NatPmpException;
import net.tomp2p.p2p.builder.AddBuilder;
import net.tomp2p.p2p.builder.AddTrackerBuilder;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.p2p.builder.DiscoverBuilder;
import net.tomp2p.p2p.builder.GetBuilder;
import net.tomp2p.p2p.builder.GetTrackerBuilder;
import net.tomp2p.p2p.builder.ParallelRequestBuilder;
import net.tomp2p.p2p.builder.PingBuilder;
import net.tomp2p.p2p.builder.PutBuilder;
import net.tomp2p.p2p.builder.RemoveBuilder;
import net.tomp2p.p2p.builder.SendBuilder;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.p2p.builder.ShutdownBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.BroadcastRPC;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.PingRPC;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.PeerExchangeRPC;
import net.tomp2p.rpc.QuitRPC;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.rpc.SynchronizationRPC;
//import net.tomp2p.rpc.TaskRPC;
import net.tomp2p.rpc.TrackerRPC;
//import net.tomp2p.task.AsyncTask;
//import net.tomp2p.task.Worker;







import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <li>{@link PutBuilder#setDataMap(Map) - puts multiple content key / values at once</li>
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
    // domain used if no domain provided
    private static final Logger LOG = LoggerFactory.getLogger(Peer.class);
    // As soon as the user calls listen, this connection handler is set
    private final PeerCreator peerCreator;
    // the id of this node
    private final Number160 peerId;
    // the p2p network identifier, two different networks can have the same
    // ports
    private final int p2pID;

    // Distributed
    private DistributedHashTable distributedHashMap;
    private DistributedTracker distributedTracker;
    private DistributedRouting distributedRouting;
    //private DistributedTask distributedTask;
    // private AsyncTask asyncTask;

    // RPC
    private PingRPC handshakeRCP;
    private StorageRPC storageRPC;
    private NeighborRPC neighborRPC;
    private QuitRPC quitRCP;
    private PeerExchangeRPC peerExchangeRPC;
    private DirectDataRPC directDataRPC;
    private TrackerRPC trackerRPC;
    // private TaskRPC taskRPC;
    private BroadcastRPC broadcastRPC;
    private SynchronizationRPC synchronizationRPC;

    //
    private boolean shutdown = false;
    
    private List<AutomaticFuture> automaticFutures = null;

    //
    // final private ConnectionConfiguration configuration;

    // final private Map<BaseFuture, Long> pendingFutures = Collections.synchronizedMap(new CacheMap<BaseFuture, Long>(
    // 1000, true));

    // private boolean masterFlag = true;

    // private List<ScheduledFuture<?>> scheduledFutures = Collections
    // .synchronizedList(new ArrayList<ScheduledFuture<?>>());

    // final private List<PeerListener> listeners = new ArrayList<PeerListener>();

    // private Timer timer;

    // final public static int BLOOMFILTER_SIZE = 1024;

    //
    // final private int maintenanceThreads;

    // final private int replicationThreads;

    // final private int replicationRefreshMillis;

    // final private PeerMap peerMap;

    // final private int maxMessageSize;

    // private volatile boolean shutdown = false;

    /**
     * Create a peer. Please use {@link PeerMaker} to create a class
     * 
     * @param p2pID
     *            The P2P ID
     * @param peerId
     *            The Id of the peer
     * @param peerCreator
     *            The peer creator that holds the peer bean and connection bean
     */
    Peer(final int p2pID, final Number160 peerId, final PeerCreator peerCreator) {
        this.p2pID = p2pID;
        this.peerId = peerId;
        this.peerCreator = peerCreator;
        ResourceLeakDetector.setEnabled(false);
    }

    PeerCreator peerCreator() {
        return peerCreator;
    }

    public PingRPC getHandshakeRPC() {
        if (handshakeRCP == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return handshakeRCP;
    }

    public void setHandshakeRPC(PingRPC handshakeRPC) {
        this.handshakeRCP = handshakeRPC;
    }

    public StorageRPC getStoreRPC() {
        if (storageRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return storageRPC;
    }

    public void setStorageRPC(StorageRPC storageRPC) {
        this.storageRPC = storageRPC;
    }

    public NeighborRPC getNeighborRPC() {
        if (neighborRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return neighborRPC;
    }

    public void setNeighborRPC(NeighborRPC neighborRPC) {
        this.neighborRPC = neighborRPC;
    }

    public QuitRPC getQuitRPC() {
        if (quitRCP == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return quitRCP;
    }

    public void setQuitRPC(QuitRPC quitRCP) {
        this.quitRCP = quitRCP;
    }

    public PeerExchangeRPC getPeerExchangeRPC() {
        if (peerExchangeRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return peerExchangeRPC;
    }

    public void setPeerExchangeRPC(PeerExchangeRPC peerExchangeRPC) {
        this.peerExchangeRPC = peerExchangeRPC;
    }

    public DirectDataRPC getDirectDataRPC() {
        if (directDataRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return directDataRPC;
    }

    public void setDirectDataRPC(DirectDataRPC directDataRPC) {
        this.directDataRPC = directDataRPC;
    }

    public TrackerRPC getTrackerRPC() {
        if (trackerRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return trackerRPC;
    }

    public void setTrackerRPC(TrackerRPC trackerRPC) {
        this.trackerRPC = trackerRPC;
    }

    /*
     * public TaskRPC getTaskRPC() { if (taskRPC == null) { throw new
     * RuntimeException("Not enabled, please enable this RPC in PeerMaker"); } return taskRPC; }
     * 
     * public void setTaskRPC(TaskRPC taskRPC) { this.taskRPC = taskRPC; }
     */

    public void setBroadcastRPC(BroadcastRPC broadcastRPC) {
        this.broadcastRPC = broadcastRPC;

    }

    public BroadcastRPC getBroadcastRPC() {
        if (broadcastRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return broadcastRPC;
    }
    
    public void setSynchronizationRPC(SynchronizationRPC synchronizationRPC) {
    	this.synchronizationRPC = synchronizationRPC;
    }
    
    public SynchronizationRPC getSynchronizationRPC() {
    	if(synchronizationRPC == null) {
    		throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
    	}
    	return synchronizationRPC;
    }

    public DistributedRouting getDistributedRouting() {
        if (distributedRouting == null) {
            throw new RuntimeException("Not enabled, please enable this P2P function in PeerMaker");
        }
        return distributedRouting;
    }

    public void setDistributedRouting(DistributedRouting distributedRouting) {
        this.distributedRouting = distributedRouting;
    }

    public DistributedHashTable getDistributedHashMap() {
        if (distributedHashMap == null) {
            throw new RuntimeException("Not enabled, please enable this P2P function in PeerMaker");
        }
        return distributedHashMap;
    }

    public void setDistributedHashMap(DistributedHashTable distributedHashMap) {
        this.distributedHashMap = distributedHashMap;
    }

    public DistributedTracker getDistributedTracker() {
        if (distributedTracker == null) {
            throw new RuntimeException("Not enabled, please enable this P2P function in PeerMaker");
        }
        return distributedTracker;
    }

    public void setDistributedTracker(DistributedTracker distributedTracker) {
        this.distributedTracker = distributedTracker;
    }

    /*
     * public AsyncTask getAsyncTask() { if (asyncTask == null) { throw new
     * RuntimeException("Not enabled, please enable this RPC in PeerMaker"); } return asyncTask; }
     * 
     * public void setAsyncTask(AsyncTask asyncTask) { this.asyncTask = asyncTask; }
     */

    /*public DistributedTask getDistributedTask() {
        if (distributedTask == null) {
            throw new RuntimeException("Not enabled, please enable this P2P function in PeerMaker");
        }
        return distributedTask;
    }

    public void setDistributedTask(DistributedTask task) {
        this.distributedTask = task;
    }*/

    public PeerBean getPeerBean() {
        return peerCreator.peerBean();
    }

    public ConnectionBean getConnectionBean() {
        return peerCreator.connectionBean();
    }

    public Number160 getPeerID() {
        return peerId;
    }

    public int getP2PID() {
        return p2pID;
    }

    public PeerAddress getPeerAddress() {
        return getPeerBean().serverPeerAddress();
    }
    
    
    Peer setAutomaticFutures(List<AutomaticFuture> automaticFutures) {
        this.automaticFutures = Collections.unmodifiableList(automaticFutures);
        return this;
    }
    
    //TODO: expose this
    public Peer notifyAutomaticFutures(BaseFuture future) {
        if(automaticFutures!=null) {
            for(AutomaticFuture automaticFuture:automaticFutures) {
                automaticFuture.futureCreated(future);
            }
        }
        return this;
    }

    // *********************************** DHT / Tracker operations start here

    public void setRawDataReply(final RawDataReply rawDataReply) {
        getDirectDataRPC().setReply(rawDataReply);
    }

    public void setObjectDataReply(final ObjectDataReply objectDataReply) {
        getDirectDataRPC().setReply(objectDataReply);
    }

    /**
     * Opens a TCP connection and keeps it open. The user can provide the idle timeout, which means that the connection
     * gets closed after that time of inactivity. If the other peer goes offline or closes the connection (due to
     * inactivity), further requests with this connections reopens the connection. This methods blocks until a
     * connection can be reserver.
     * 
     * @param destination
     *            The end-point to connect to
     * @param idleTCPMillis
     *            time in milliseconds after a connection gets closed if idle, -1 if it should remain always open until
     *            the user closes the connection manually.
     * @return A class that needs to be passed to those methods that should use the already open connection. If the
     *         connection could not be reserved, maybe due to a shutdown, null is returned.
     */
    public FuturePeerConnection createPeerConnection(final PeerAddress destination) {
        final FuturePeerConnection futureDone = new FuturePeerConnection(destination);
        final FutureChannelCreator fcc = getConnectionBean().reservation().createPermanent(1);
        fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    final ChannelCreator cc = fcc.getChannelCreator();
                    final PeerConnection peerConnection = new PeerConnection(destination, cc);
                    futureDone.setDone(peerConnection);
                } else {
                    futureDone.setFailed(future);
                }

            }
        });
        return futureDone;
    }

    /**
     * The Dynamic and/or Private Ports are those from 49152 through 65535
     * (http://www.iana.org/assignments/port-numbers).
     * 
     * @param internalHost
     *            The IP of the internal host
     * @return True if port forwarding seemed to be successful
     */
    public boolean setupPortForwanding(final String internalHost) {
        Bindings bindings = getConnectionBean().channelServer().bindings();
        int portUDP = bindings.getOutsideUDPPort();
        int portTCP = bindings.getOutsideTCPPort();
        boolean success;

        try {
            success = getConnectionBean().natUtils().mapUPNP(internalHost, getPeerAddress().tcpPort(),
                    getPeerAddress().udpPort(), portUDP, portTCP);
        } catch (IOException e) {
            success = false;
        }

        if (!success) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("cannot find UPNP devices");
            }
            try {
                success = getConnectionBean().natUtils().mapPMP(getPeerAddress().tcpPort(),
                        getPeerAddress().udpPort(), portUDP, portTCP);
                if (!success) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("cannot find NAT-PMP devices");
                    }
                }
            } catch (NatPmpException e1) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("cannot find NAT-PMP devices ", e1);
                }
            }
        }
        return success;
    }

    // New API - builder pattern
    // --------------------------------------------------

    /*
     * public SubmitBuilder submit(Number160 locationKey, Worker worker) { return new SubmitBuilder(this, locationKey,
     * worker); }
     */

    public AddBuilder add(Number160 locationKey) {
        return new AddBuilder(this, locationKey);
    }

    public PutBuilder put(Number160 locationKey) {
        return new PutBuilder(this, locationKey);
    }

    public GetBuilder get(Number160 locationKey) {
        return new GetBuilder(this, locationKey);
    }

    public RemoveBuilder remove(Number160 locationKey) {
        return new RemoveBuilder(this, locationKey);
    }

    /**
     * The send method works as follows:
     * 
     * <pre>
     * 1. routing: find close peers to the content hash. 
     *    You can control the routing behavior with 
     *    setRoutingConfiguration() 
     * 2. sending: send the data to the n closest peers. 
     *    N is set via setRequestP2PConfiguration(). 
     *    If you want to send it to the closest one, use 
     *    setRequestP2PConfiguration(1, 5, 0)
     * </pre>
     * 
     * @param locationKey
     *            The target hash to search for during the routing process
     * @return The send builder that allows to set options
     */
    public SendBuilder send(Number160 locationKey) {
        return new SendBuilder(this, locationKey);
    }

    public SendDirectBuilder sendDirect(PeerAddress recipientAddress) {
        return new SendDirectBuilder(this, recipientAddress);
    }

    public SendDirectBuilder sendDirect(FuturePeerConnection recipientConnection) {
        return new SendDirectBuilder(this, recipientConnection);
    }

    public BootstrapBuilder bootstrap() {
        return new BootstrapBuilder(this);
    }

    public PingBuilder ping() {
        return new PingBuilder(this);
    }

    public DiscoverBuilder discover() {
        return new DiscoverBuilder(this);
    }

    public AddTrackerBuilder addTracker(Number160 locationKey) {
        return new AddTrackerBuilder(this, locationKey);
    }

    public GetTrackerBuilder getTracker(Number160 locationKey) {
        return new GetTrackerBuilder(this, locationKey);
    }

    public ParallelRequestBuilder parallelRequest(Number160 locationKey) {
        return new ParallelRequestBuilder(this, locationKey);
    }

    public BroadcastBuilder broadcast(Number160 messageKey) {
        return new BroadcastBuilder(this, messageKey);
    }

    /**
     * Sends a friendly shutdown message to my close neighbors in the DHT.
     * 
     * @return A builder for shutdown that runs asynchronous.
     */
    public ShutdownBuilder announceShutdown() {
        return new ShutdownBuilder(this);
    }

    /**
     * Shuts down everything.
     * 
     * @return The future, when shutdown is completed
     */
    public FutureDone<Void> shutdown() {
        //prevent the shutdown from being called twice
        if (!shutdown) {
            shutdown = true;
            return peerCreator.shutdown();
            
        } else {
            return new FutureDone<Void>().setFailed("already shutting / shut down");
        }
    }

    /**
     * @return True if the peer is about or already has shutdown
     */
    public boolean isShutdown() {
        return shutdown;
    }
}
