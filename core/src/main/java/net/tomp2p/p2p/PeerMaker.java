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

import io.netty.channel.ChannelHandler;

import java.io.IOException;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;

import net.tomp2p.connection2.Bindings;
import net.tomp2p.connection2.ChannelClientConfiguration;
import net.tomp2p.connection2.ChannelServerConficuration;
import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.connection2.DefaultSignatureFactory;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.connection2.PeerCreator;
import net.tomp2p.connection2.PipelineFilter;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.replication.Replication;
import net.tomp2p.replication.ReplicationExecutor;
import net.tomp2p.rpc.BloomfilterFactory;
import net.tomp2p.rpc.BroadcastRPC;
import net.tomp2p.rpc.DefaultBloomfilterFactory;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.PeerExchangeRPC;
import net.tomp2p.rpc.PingRPC;
import net.tomp2p.rpc.QuitRPC;
import net.tomp2p.rpc.StorageRPC;
//import net.tomp2p.rpc.TaskRPC;
import net.tomp2p.rpc.TrackerRPC;
import net.tomp2p.storage.IdentityManagement;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.storage.StorageMemory;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.utils.Utils;

/**
 * The maker / builder of a {@link Peer} class.
 * 
 * @author Thomas Bocek
 * 
 */
public class PeerMaker {
    private static final KeyPair EMPTY_KEYPAIR = new KeyPair(null, null);
    // if the permits are chosen too high, then we might run into timeouts as we cant handle that many connections
    // withing the time limit
    private static final int MAX_PERMITS_PERMANENT_TCP = 250;
    private static final int MAX_PERMITS_UDP = 250;
    private static final int MAX_PERMITS_TCP = 250;

    // required
    private final Number160 peerId;

    // optional with reasonable defaults

    private KeyPair keyPair = null;

    private int p2pID = -1;

    private int tcpPort = -1;

    private int udpPort = -1;

    private Bindings bindings = null;

    private PeerMap peerMap = null;

    private Peer masterPeer = null;

    private ChannelServerConficuration channelServerConfiguration = null;

    private ChannelClientConfiguration channelClientConfiguration = null;

    private PeerStatusListener[] peerStatusListeners = null;

    private StorageGeneric storage = null;

    private TrackerStorage trackerStorage = null;
    
    private Boolean behindFirewall = null;

    // private int workerThreads = Runtime.getRuntime().availableProcessors() + 1;

    // private File fileMessageLogger = null;

    // private ConnectionConfiguration configuration = null;

    // private StorageGeneric storage;

    private BroadcastHandler broadcastHandler;

    private BloomfilterFactory bloomfilterFactory;
    
    private Timer timer = null;
    
    private MaintenanceTask maintenanceTask = null;
    
    private ReplicationExecutor replicationExecutor = null;
    
    private List<AutomaticFuture> automaticFutures = null;

    // private ReplicationExecutor replicationExecutor;

    // max, message size to transmit
    // private int maxMessageSize = 2 * 1024 * 1024;

    // PeerMap
    // private int bagSize = 2;

    // private int cacheTimeoutMillis = 60 * 1000;

    // private int maxNrBeforeExclude = 2;

    // private int[] waitingTimeBetweenNodeMaintenenceSeconds = { 5, 10, 20, 40, 80, 160 };

    // private int cacheSize = 100;

    // enable / disable
    private boolean enableHandShakeRPC = true;
    private boolean enableStorageRPC = true;
    private boolean enableNeighborRPC = true;
    private boolean enableQuitRPC = true;
    private boolean enablePeerExchangeRPC = true;
    private boolean enableDirectDataRPC = true;
    private boolean enableTrackerRPC = true;
    private boolean enableTaskRPC = true;

    // P2P
    private boolean enableRouting = true;
    private boolean enableDHT = true;
    private boolean enableTracker = true;
    private boolean enableTask = true;
    private boolean enableMaintenance = true;
    private boolean enableIndirectReplication = false;
    private boolean enableBroadcast = true;

    // private Random rnd;

    /**
     * Creates a peermaker with the peer ID and an empty key pair.
     * 
     * @param peerId
     *            The peer Id
     */
    public PeerMaker(final Number160 peerId) {
        this.peerId = peerId;
    }

    /**
     * Creates a peermaker with the key pair and generates out of this key pair the peer ID.
     * 
     * @param keyPair
     *            The public private key
     */
    public PeerMaker(final KeyPair keyPair) {
        this.peerId = Utils.makeSHAHash(keyPair.getPublic().getEncoded());
        this.keyPair = keyPair;
    }

    /**
     * Create a peer and start to listen for incoming connections.
     * 
     * @return The peer that can operate in the P2P network.
     * @throws IOException .
     */
    public Peer makeAndListen() throws IOException {
        
        if (behindFirewall == null) {
            behindFirewall = false;
        }
        
        if (channelServerConfiguration == null) {
            channelServerConfiguration = createDefaultChannelServerConfiguration();
        }
        if (channelClientConfiguration == null) {
            channelClientConfiguration = createDefaultChannelClientConfiguration();
        }
        if (keyPair == null) {
            keyPair = EMPTY_KEYPAIR;
        }
        if (p2pID == -1) {
            p2pID = 1;
        }
        if (tcpPort == -1) {
            tcpPort = Bindings.DEFAULT_PORT;
        }
        channelServerConfiguration.setTcpPort(tcpPort);
        if (udpPort == -1) {
            udpPort = Bindings.DEFAULT_PORT;
        }
        channelServerConfiguration.setUdpPort(tcpPort);
        if (bindings == null) {
            bindings = new Bindings();
        }
        channelServerConfiguration.setBindings(bindings);
        if (peerMap == null) {
            peerMap = new PeerMap(new PeerMapConfiguration(peerId));
        }

        if (storage == null) {
            storage = new StorageMemory();
        }

        if (peerStatusListeners == null) {
            peerStatusListeners = new PeerStatusListener[] { peerMap };
        }
        
        if (masterPeer == null && timer == null) {
            timer = new Timer();
        }

        final PeerCreator peerCreator;
        if (masterPeer != null) {
            peerCreator = new PeerCreator(masterPeer.peerCreator(), peerId, keyPair);
        } else {
            peerCreator = new PeerCreator(p2pID, peerId, keyPair, channelServerConfiguration,
                    channelClientConfiguration, peerStatusListeners, timer);
        }

        final Peer peer = new Peer(p2pID, peerId, peerCreator);

        PeerBean peerBean = peerCreator.peerBean();
        peerBean.peerMap(peerMap);
        peerBean.keyPair(keyPair);
        peerBean.storage(storage);

        if (trackerStorage == null) {
            trackerStorage = new TrackerStorage(new IdentityManagement(peerBean.serverPeerAddress()), 300,
                    peerBean.getReplicationTracker(), new Maintenance());
        }

        peerBean.trackerStorage(trackerStorage);

        if (bloomfilterFactory == null) {
            peerBean.bloomfilterFactory(new DefaultBloomfilterFactory());
        }
        
        if (broadcastHandler == null) {
            broadcastHandler = new DefaultBroadcastHandler(peer, new Random());
        }

        ConnectionBean connectionBean = peerCreator.connectionBean();
        // peerBean.setStorage(getStorage());
        Replication replicationStorage = new Replication(peerBean.storage(), peerBean.serverPeerAddress(), peerMap, 5);
        peerBean.replicationStorage(replicationStorage);

        // TrackerStorage storageTracker = new TrackerStorage(identityManagement,
        // configuration.getTrackerTimoutSeconds(), peerBean, maintenance);
        // peerBean.setTrackerStorage(storageTracker);
        // Replication replicationTracker = new Replication(storageTracker, selfAddress, peerMap, 5);
        // peerBean.setReplicationTracker(replicationTracker);

        // peerMap.addPeerOfflineListener(storageTracker);

        // TaskManager taskManager = new TaskManager(connectionBean, workerThreads);
        // peerBean.setTaskManager(taskManager);

        // IdentityManagement identityManagement = new IdentityManagement(selfAddress);
        // Maintenance maintenance = new Maintenance();

        initRPC(peer, connectionBean, peerBean);
        initP2P(peer, connectionBean, peerBean);
        
        if(maintenanceTask == null) {
            maintenanceTask = new MaintenanceTask();
        }
        maintenanceTask.init(peer, connectionBean.timer());
        maintenanceTask.addMaintainable(peerMap);
        peerBean.maintenanceTask(maintenanceTask);
        
        // indirect replication
        if(replicationExecutor == null && isEnableIndirectReplication() && isEnableStorageRPC()) {
            replicationExecutor = new ReplicationExecutor(peer); 
        }
        if (replicationExecutor != null) {
            replicationExecutor.init(peer, connectionBean.timer());
        }
        peerBean.replicationExecutor(replicationExecutor);
        
        if(automaticFutures!=null) {
            peer.setAutomaticFutures(automaticFutures);
        }

        return peer;
    }

    public ChannelServerConficuration createDefaultChannelServerConfiguration() {
        ChannelServerConficuration channelServerConfiguration = new ChannelServerConficuration();
        channelServerConfiguration.setBindings(bindings);
        channelServerConfiguration.setTcpPort(tcpPort);
        channelServerConfiguration.setUdpPort(udpPort);
        channelServerConfiguration.setBehindFirewall(behindFirewall);
        channelServerConfiguration.pipelineFilter(new DefaultPipelineFilter());
        channelServerConfiguration.signatureFactory(new DefaultSignatureFactory());
        return channelServerConfiguration;
    }

    public ChannelClientConfiguration createDefaultChannelClientConfiguration() {
        ChannelClientConfiguration channelClientConfiguration = new ChannelClientConfiguration();
        channelClientConfiguration.maxPermitsPermanentTCP(MAX_PERMITS_PERMANENT_TCP);
        channelClientConfiguration.maxPermitsTCP(MAX_PERMITS_TCP);
        channelClientConfiguration.maxPermitsUDP(MAX_PERMITS_UDP);
        channelClientConfiguration.pipelineFilter(new DefaultPipelineFilter());
        channelClientConfiguration.signatureFactory(new DefaultSignatureFactory());
        return channelClientConfiguration;
    }

    /**
     * Initialize the RPC communications.
     * 
     * @param peer
     *            The peer where the RPC reference is stored
     * @param connectionBean
     *            The connection bean
     * @param peerBean
     *            The peer bean
     */
    private void initRPC(final Peer peer, final ConnectionBean connectionBean, final PeerBean peerBean) {
        // RPC communication
        if (isEnableHandShakeRPC()) {
            PingRPC handshakeRCP = new PingRPC(peerBean, connectionBean);
            peer.setHandshakeRPC(handshakeRCP);
        }

        if (isEnableStorageRPC()) {
            StorageRPC storageRPC = new StorageRPC(peerBean, connectionBean);
            peer.setStorageRPC(storageRPC);
        }

        if (isEnableNeighborRPC()) {
            NeighborRPC neighborRPC = new NeighborRPC(peerBean, connectionBean);
            peer.setNeighborRPC(neighborRPC);
        }

        if (isEnableDirectDataRPC()) {
            DirectDataRPC directDataRPC = new DirectDataRPC(peerBean, connectionBean);
            peer.setDirectDataRPC(directDataRPC);
        }

        if (isEnableQuitRPC()) {
            QuitRPC quitRCP = new QuitRPC(peerBean, connectionBean);
            quitRCP.addPeerStatusListener(peerMap);
            peer.setQuitRPC(quitRCP);
        }

        if (isEnablePeerExchangeRPC()) {
            PeerExchangeRPC peerExchangeRPC = new PeerExchangeRPC(peerBean, connectionBean);
            peer.setPeerExchangeRPC(peerExchangeRPC);
        }

        if (isEnableDirectDataRPC()) {
            DirectDataRPC directDataRPC = new DirectDataRPC(peerBean, connectionBean);
            peer.setDirectDataRPC(directDataRPC);
        }

        if (isEnableTrackerRPC()) {
            TrackerRPC trackerRPC = new TrackerRPC(peerBean, connectionBean);
            peer.setTrackerRPC(trackerRPC);
        }

        if (isEnableBroadcast()) {
            BroadcastRPC broadcastRPC = new BroadcastRPC(peerBean, connectionBean, broadcastHandler);
            peer.setBroadcastRPC(broadcastRPC);
        }
         
    }

    private void initP2P(final Peer peer, final ConnectionBean connectionBean, final PeerBean peerBean) {
        // distributed communication

        if (isEnableRouting() && isEnableNeighborRPC()) {
            DistributedRouting routing = new DistributedRouting(peerBean, peer.getNeighborRPC());
            peer.setDistributedRouting(routing);
        }
        
        if (isEnableRouting() && isEnableStorageRPC() && isEnableDirectDataRPC()) {
            DistributedHashTable dht = new DistributedHashTable(peer.getDistributedRouting(),
                    peer.getStoreRPC(), peer.getDirectDataRPC(), peer.getQuitRPC());
            peer.setDistributedHashMap(dht);
        } 
        /*if (isEnableRouting() && isEnableTrackerRPC() &&
         * isEnablePeerExchangeRPC()) { DistributedTracker tracker = new DistributedTracker(peerBean,
         * peer.getDistributedRouting(), peer.getTrackerRPC(), peer.getPeerExchangeRPC());
         * peer.setDistributedTracker(tracker); } if (isEnableTaskRPC() && isEnableTask() && isEnableRouting()) { // the
         * task manager needs to use the rpc to send the result back. //TODO: enable again
         * //peerBean.getTaskManager().init(peer.getTaskRPC()); //AsyncTask asyncTask = new AsyncTask(peer.getTaskRPC(),
         * connectionBean.getScheduler(), peerBean); //peer.setAsyncTask(asyncTask);
         * //peerBean.getTaskManager().addListener(asyncTask);
         * //connectionBean.getScheduler().startTracking(peer.getTaskRPC(), //
         * connectionBean.getConnectionReservation()); //DistributedTask distributedTask = new
         * DistributedTask(peer.getDistributedRouting(), peer.getAsyncTask());
         * //peer.setDistributedTask(distributedTask); } // maintenance if (isEnableMaintenance()) { //TODO: enable
         * again //connectionHandler // .getConnectionBean() // .getScheduler() //
         * .startMaintainance(peerBean.getPeerMap(), peer.getHandshakeRPC(), //
         * connectionBean.getConnectionReservation(), 5); } 
         */ 
    }

    public Number160 peerId() {
        return peerId;
    }

    public KeyPair keyPair() {
        return keyPair;
    }

    public PeerMaker keyPair(KeyPair keyPair) {
        this.keyPair = keyPair;
        return this;
    }

    public int p2pId() {
        return p2pID;
    }

    public PeerMaker p2pId(int p2pID) {
        this.p2pID = p2pID;
        return this;
    }

    public int tcpPort() {
        return tcpPort;
    }

    public PeerMaker tcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
        return this;
    }

    public int udpPort() {
        return udpPort;
    }

    public PeerMaker udpPort(int udpPort) {
        this.udpPort = udpPort;
        return this;
    }

    public PeerMaker ports(int port) {
        this.udpPort = port;
        this.tcpPort = port;
        return this;
    }

    public Bindings bindings() {
        return bindings;
    }

    public PeerMaker bindings(Bindings bindings) {
        this.bindings = bindings;
        return this;
    }

    public PeerMap peerMap() {
        return peerMap;
    }

    public PeerMaker peerMap(PeerMap peerMap) {
        this.peerMap = peerMap;
        return this;
    }

    public Peer masterPeer() {
        return masterPeer;
    }

    public PeerMaker masterPeer(Peer masterPeer) {
        this.masterPeer = masterPeer;
        return this;
    }

    public ChannelServerConficuration channelServerConfiguration() {
        return channelServerConfiguration;
    }

    public PeerMaker channelServerConfiguration(ChannelServerConficuration channelServerConfiguration) {
        this.channelServerConfiguration = channelServerConfiguration;
        return this;
    }

    public ChannelClientConfiguration channelClientConfiguration() {
        return channelClientConfiguration;
    }

    public PeerMaker channelClientConfiguration(ChannelClientConfiguration channelClientConfiguration) {
        this.channelClientConfiguration = channelClientConfiguration;
        return this;
    }

    public PeerStatusListener[] peerStatusListeners() {
        return peerStatusListeners;
    }

    public PeerMaker peerStatusListeners(PeerStatusListener[] peerStatusListeners) {
        this.peerStatusListeners = peerStatusListeners;
        return this;
    }

    public BroadcastHandler broadcastHandler() {
        return broadcastHandler;
    }

    public PeerMaker broadcastHandler(BroadcastHandler broadcastHandler) {
        this.broadcastHandler = broadcastHandler;
        return this;
    }

    public BloomfilterFactory bloomfilterFactory() {
        return bloomfilterFactory;
    }

    public PeerMaker bloomfilterFactory(BloomfilterFactory bloomfilterFactory) {
        this.bloomfilterFactory = bloomfilterFactory;
        return this;
    }
    
    public MaintenanceTask maintenanceTask() {
        return maintenanceTask;
    }

    public PeerMaker maintenanceTask(MaintenanceTask maintenanceTask) {
        this.maintenanceTask = maintenanceTask;
        return this;
    }
    
    public ReplicationExecutor replicationExecutor() {
        return replicationExecutor;
    }

    public PeerMaker replicationExecutor(ReplicationExecutor replicationExecutor) {
        this.replicationExecutor = replicationExecutor;
        return this;
    }
    
    // isEnabled methods

    public boolean isEnableHandShakeRPC() {
        return enableHandShakeRPC;
    }

    public PeerMaker setEnableHandShakeRPC(boolean enableHandShakeRPC) {
        this.enableHandShakeRPC = enableHandShakeRPC;
        return this;
    }

    public boolean isEnableStorageRPC() {
        return enableStorageRPC;
    }

    public PeerMaker setEnableStorageRPC(boolean enableStorageRPC) {
        this.enableStorageRPC = enableStorageRPC;
        return this;
    }

    public boolean isEnableNeighborRPC() {
        return enableNeighborRPC;
    }

    public PeerMaker setEnableNeighborRPC(boolean enableNeighborRPC) {
        this.enableNeighborRPC = enableNeighborRPC;
        return this;
    }

    public boolean isEnableQuitRPC() {
        return enableQuitRPC;
    }

    public PeerMaker setEnableQuitRPC(boolean enableQuitRPC) {
        this.enableQuitRPC = enableQuitRPC;
        return this;
    }

    public boolean isEnablePeerExchangeRPC() {
        return enablePeerExchangeRPC;
    }

    public PeerMaker setEnablePeerExchangeRPC(boolean enablePeerExchangeRPC) {
        this.enablePeerExchangeRPC = enablePeerExchangeRPC;
        return this;
    }

    public boolean isEnableDirectDataRPC() {
        return enableDirectDataRPC;
    }

    public PeerMaker setEnableDirectDataRPC(boolean enableDirectDataRPC) {
        this.enableDirectDataRPC = enableDirectDataRPC;
        return this;
    }

    public boolean isEnableTrackerRPC() {
        return enableTrackerRPC;
    }

    public PeerMaker setEnableTrackerRPC(boolean enableTrackerRPC) {
        this.enableTrackerRPC = enableTrackerRPC;
        return this;
    }

    public boolean isEnableTaskRPC() {
        return enableTaskRPC;
    }

    public PeerMaker setEnableTaskRPC(boolean enableTaskRPC) {
        this.enableTaskRPC = enableTaskRPC;
        return this;
    }

    public boolean isEnableRouting() {
        return enableRouting;
    }

    public PeerMaker setEnableRouting(boolean enableRouting) {
        this.enableRouting = enableRouting;
        return this;
    }

    public boolean isEnableDHT() {
        return enableDHT;
    }

    public PeerMaker setEnableDHT(boolean enableDHT) {
        this.enableDHT = enableDHT;
        return this;
    }

    public boolean isEnableTracker() {
        return enableTracker;
    }

    public PeerMaker setEnableTracker(boolean enableTracker) {
        this.enableTracker = enableTracker;
        return this;
    }

    public boolean isEnableTask() {
        return enableTask;
    }

    public PeerMaker setEnableTask(boolean enableTask) {
        this.enableTask = enableTask;
        return this;
    }

    public boolean isEnableMaintenance() {
        return enableMaintenance;
    }

    public PeerMaker setEnableMaintenance(boolean enableMaintenance) {
        this.enableMaintenance = enableMaintenance;
        return this;
    }

    public boolean isEnableIndirectReplication() {
        return enableIndirectReplication;
    }

    public PeerMaker setEnableIndirectReplication(boolean enableIndirectReplication) {
        this.enableIndirectReplication = enableIndirectReplication;
        return this;
    }

    public boolean isEnableBroadcast() {
        return enableBroadcast;
    }

    public PeerMaker setEnableBroadcast(boolean enableBroadcast) {
        this.enableBroadcast = enableBroadcast;
        return this;
    }
    
    /**
     * @return True if this peer is behind a firewall and cannot be accessed directly
     */
    public boolean isBehindFirewall() {
        return behindFirewall == null?false:behindFirewall;
    }

    /**
     * @param behindFirewall
     *            Set to true if this peer is behind a firewall and cannot be accessed directly
     * @return This class
     */
    public PeerMaker setBehindFirewall(final boolean behindFirewall) {
        this.behindFirewall = behindFirewall;
        return this;
    }

    /**
     * Set peer to be behind a firewall and cannot be accessed directly.
     * 
     * @return This class
     */
    public PeerMaker setBehindFirewall() {
        this.behindFirewall = true;
        return this;
    }
    
    public PeerMaker addAutomaticFuture(AutomaticFuture automaticFuture) {
        if(automaticFutures==null) {
            automaticFutures = new ArrayList<>(1);
        }
        automaticFutures.add(automaticFuture);
        return this;
    }

    /**
     * The default filter is no filter, just return the same array.
     * 
     * @author Thomas Bocek
     * 
     */
    private static class DefaultPipelineFilter implements PipelineFilter {
        @Override
        public void filter(final Map<String, ChannelHandler> channelHandlers, final boolean tcp,
                final boolean client) {
        }
    }

}
