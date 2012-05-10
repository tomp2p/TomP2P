package net.tomp2p.p2p;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionHandler;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapKadImpl;
import net.tomp2p.replication.DefaultStorageReplication;
import net.tomp2p.replication.Replication;
import net.tomp2p.replication.TrackerStorageReplication;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.HandshakeRPC;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.PeerExchangeRPC;
import net.tomp2p.rpc.QuitRPC;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.rpc.TaskRPC;
import net.tomp2p.rpc.TrackerRPC;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.storage.StorageMemory;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.task.AsyncTask;
import net.tomp2p.task.TaskManager;
import net.tomp2p.utils.Utils;

public class PeerMaker
{
	final private static KeyPair EMPTY_KEYPAIR = new KeyPair(null, null);
	//required
	final private Number160 peerId;
	//optional with reasonable defaults
	private int p2pID = 1;
	private KeyPair keyPair = EMPTY_KEYPAIR;
	//
	private int workerThreads = Runtime.getRuntime().availableProcessors() + 1;
	private int maintenanceThreads = 5;
	private int replicationThreads = 5;
	// indirect replication
	private int replicationRefreshMillis=60 * 1000;
	private int tcpPort = Bindings.DEFAULT_PORT;
	private int udpPort = Bindings.DEFAULT_PORT;
	private Peer masterPeer = null;
	private File fileMessageLogger = null;
	private Bindings bindings = new Bindings();
	private ConnectionConfiguration configuration = new ConnectionConfiguration();
	private StorageGeneric storage = new StorageMemory();
	// max, message size to transmit
	private int maxMessageSize = 2 * 1024 * 1024;
	// PeerMap
	private int bagSize = 2;
	private int cacheTimeoutMillis = 60 * 1000;
	private int maxNrBeforeExclude = 2;
	private int[] waitingTimeBetweenNodeMaintenenceSeconds = { 5, 10, 20, 40, 80, 160 };
	private int cacheSize = 100;
	private boolean isBehindFirewallPeerMap = false;
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
	
	public PeerMaker(final Number160 peerId)
	{
		this.peerId = peerId;
	}
	
	public PeerMaker(final KeyPair keyPair)
	{
		this.peerId = Utils.makeSHAHash(keyPair.getPublic().getEncoded());
		this.keyPair = keyPair;
	}
	
	public Peer buildAndListen() throws IOException
	{
		final PeerMapKadImpl peerMap = new PeerMapKadImpl(peerId, getBagSize(), getCacheTimeoutMillis(), getMaxNrBeforeExclude(),
				getWaitingTimeBetweenNodeMaintenenceSeconds(), getCacheSize(), isBehindFirewallPeerMap());
		final Peer peer = new Peer(getP2PId(), peerId, keyPair, getMaintenanceThreads(), 
				getReplicationThreads(), getConfiguration(), peerMap, getMaxMessageSize());
		final ConnectionHandler connectionHandler;
		if(getMasterPeer() != null)
		{
			connectionHandler = peer.listen(getMasterPeer());
		}
		else
		{
			connectionHandler = peer.listen(udpPort, tcpPort, bindings, fileMessageLogger);
		}
		init(peer, connectionHandler, peerMap.getStatistics());
		return peer;
	}
	
	private void init(final Peer peer, final ConnectionHandler connectionHandler, final Statistics statistics)
	{
		PeerBean peerBean = connectionHandler.getPeerBean();
		peerBean.setStatistics(statistics);
		ConnectionBean connectionBean = connectionHandler.getConnectionBean();
		PeerAddress selfAddress = peerBean.getServerPeerAddress();
		PeerMap peerMap = peerBean.getPeerMap();
		peerBean.setStorage(getStorage());
		Replication replicationStorage = new Replication(getStorage(), selfAddress, peerMap);
		peerBean.setReplicationStorage(replicationStorage);
		// create tracker and add replication feature
		IdentityManagement identityManagement = new IdentityManagement(selfAddress);
		Maintenance maintenance = new Maintenance();
		//use the memory for tracker storage, no need to store the reference on disk
		
		TrackerStorage storageTracker = new TrackerStorage(identityManagement,
				configuration.getTrackerTimoutSeconds(), peerBean, maintenance);
		peerBean.setTrackerStorage(storageTracker);
		Replication replicationTracker = new Replication(storageTracker, selfAddress, peerMap);
		peerBean.setReplicationTracker(replicationTracker);
		
		peerMap.addPeerOfflineListener(storageTracker);
		// RPC communication
		if(isEnableHandShakeRPC())
		{
			HandshakeRPC handshakeRCP = new HandshakeRPC(peerBean, connectionBean, peer.getListeners());
			peer.setHandshakeRPC(handshakeRCP);
		}
		if(isEnableStorageRPC())
		{
			StorageRPC storageRPC = new StorageRPC(peerBean, connectionBean);
			peer.setStorageRPC(storageRPC);
		}
		if(isEnableNeighborRPC())
		{
			NeighborRPC neighborRPC = new NeighborRPC(peerBean, connectionBean);
			peer.setNeighborRPC(neighborRPC);
		}
		if(isEnableQuitRPC())
		{
			QuitRPC quitRCP = new QuitRPC(peerBean, connectionBean);
			peer.setQuitRPC(quitRCP);
		}
		if(isEnablePeerExchangeRPC())
		{
			PeerExchangeRPC peerExchangeRPC = new PeerExchangeRPC(peerBean, connectionBean);
			peer.setPeerExchangeRPC(peerExchangeRPC);
		}
		if(isEnableDirectDataRPC())
		{
			DirectDataRPC directDataRPC = new DirectDataRPC(peerBean, connectionBean);
			peer.setDirectDataRPC(directDataRPC);
		}
		if(isEnableTrackerRPC())
		{
			TrackerRPC trackerRPC = new TrackerRPC(peerBean, connectionBean, configuration);
			peer.setTrackerRPC(trackerRPC);
		}
		if(isEnableTaskRPC())
		{
			// create task manager, which is needed by the task RPC
			peerBean.setTaskManager(new TaskManager(peer, connectionBean, workerThreads));
			TaskRPC taskRPC = new TaskRPC(peerBean, connectionBean);
			peer.setTaskRPC(taskRPC);
		}
		if(isEnablePeerExchangeRPC())
		{
			// replication for trackers, which needs PEX
			TrackerStorageReplication trackerStorageReplication = new TrackerStorageReplication(peer,
					peer.getPeerExchangeRPC(), peer.getPendingFutures(), storageTracker, configuration.isForceTrackerTCP());
			replicationTracker.addResponsibilityListener(trackerStorageReplication);
		}
		// distributed communication
		if(isEnableRouting() && isEnableNeighborRPC())
		{
			DistributedRouting routing = new DistributedRouting(peerBean, peer.getNeighborRPC());
			peer.setDistributedRouting(routing);
		}
		if(isEnableRouting() && isEnableStorageRPC() && isEnableDirectDataRPC())
		{
			DistributedHashTable dht = new DistributedHashTable(peer.getDistributedRouting(), peer.getStoreRPC(), peer.getDirectDataRPC());
			peer.setDistributedHashMap(dht);
		}
		if(isEnableRouting() && isEnableTrackerRPC() && isEnablePeerExchangeRPC())
		{
			DistributedTracker tracker = new DistributedTracker(peerBean, peer.getDistributedRouting(), peer.getTrackerRPC(), peer.getPeerExchangeRPC());
			peer.setDistributedTracker(tracker);
		}
		if(isEnableTaskRPC() && isEnableTask() && isEnableRouting())
		{
			//the task manager needs to use the rpc to send the result back.
			peerBean.getTaskManager().init(peer.getTaskRPC());
			AsyncTask asyncTask = new AsyncTask(peer.getTaskRPC(), connectionBean.getScheduler(), peerBean);
			peer.setAsyncTask(asyncTask);
			peerBean.getTaskManager().addListener(asyncTask);
			connectionBean.getScheduler().startTracking(peer.getTaskRPC(), connectionBean.getConnectionReservation());
			DistributedTask distributedTask = new DistributedTask(peer.getDistributedRouting(), peer.getAsyncTask());
			peer.setDistributedTask(distributedTask);
		}
		// maintenance
		if (isEnableMaintenance())
		{
			connectionHandler.getConnectionBean().getScheduler().startMaintainance(
					peerBean.getPeerMap(), peer.getHandshakeRPC(), connectionBean.getConnectionReservation(), 5);
		}
		// indirect replication
		if (isEnableIndirectReplication() && isEnableStorageRPC())
		{
			DefaultStorageReplication defaultStorageReplication = new DefaultStorageReplication(peer,
					peerBean.getStorage(), peer.getStoreRPC(), peer.getPendingFutures(), configuration.isForceStorageUDP());
			peer.getScheduledFutures().add(connectionBean.getScheduler().getScheduledExecutorServiceReplication().
					scheduleWithFixedDelay(defaultStorageReplication,
							replicationRefreshMillis, replicationRefreshMillis, TimeUnit.MILLISECONDS));
			replicationStorage.addResponsibilityListener(defaultStorageReplication);
		}
		connectionBean.getScheduler().startDelayedChannelCreator();
	}
	
	public PeerMaker setPeerMapConfiguration(int bagSize, int cacheTimeoutMillis, int maxNrBeforeExclude, 
			int []waitingTimeBetweenNodeMaintenenceSeconds, int cacheSize, boolean isBehindFirewall)
	{
		this.setBagSize(bagSize);
		this.setCacheTimeoutMillis(cacheTimeoutMillis);
		this.setMaxNrBeforeExclude(maxNrBeforeExclude);
		this.setWaitingTimeBetweenNodeMaintenenceSeconds(waitingTimeBetweenNodeMaintenenceSeconds);
		this.setCacheSize(cacheSize);
		this.setBehindFirewallPeerMap(isBehindFirewall);
		return this;
	}
	
	public PeerMaker setP2PId(int p2pID)
	{
		this.p2pID = p2pID;
		return this;
	}
	
	public int getP2PId()
	{
		return p2pID;
	}
	
	public PeerMaker setKeyPair(KeyPair keyPair)
	{
		this.keyPair = keyPair;
		return this;
	}
	
	public KeyPair getKeyPair()
	{
		return keyPair;
	}

	public int getWorkerThreads()
	{
		return workerThreads;
	}

	public PeerMaker setWorkerThreads(int workerThreads)
	{
		this.workerThreads = workerThreads;
		return this;
	}

	public int getMaintenanceThreads()
	{
		return maintenanceThreads;
	}

	public PeerMaker setMaintenanceThreads(int maintenanceThreads)
	{
		this.maintenanceThreads = maintenanceThreads;
		return this;
	}

	public int getReplicationThreads()
	{
		return replicationThreads;
	}

	public PeerMaker setReplicationThreads(int replicationThreads)
	{
		this.replicationThreads = replicationThreads;
		return this;
	}

	public int getReplicationRefreshMillis()
	{
		return replicationRefreshMillis;
	}

	public PeerMaker setReplicationRefreshMillis(int replicationRefreshMillis)
	{
		this.replicationRefreshMillis = replicationRefreshMillis;
		return this;
	}

	public int getTcpPort()
	{
		return tcpPort;
	}

	public PeerMaker setTcpPort(int tcpPort)
	{
		this.tcpPort = tcpPort;
		return this;
	}

	public int getUdpPort()
	{
		return udpPort;
	}

	public PeerMaker setUdpPort(int udpPort)
	{
		this.udpPort = udpPort;
		return this;
	}
	
	public PeerMaker setPorts(int port)
	{
		this.udpPort = port;
		this.tcpPort = port;
		return this;
	}

	public Bindings getBindings()
	{
		return bindings;
	}

	public PeerMaker setBindings(Bindings bindings)
	{
		this.bindings = bindings;
		return this;
	}

	public File getFileMessageLogger()
	{
		return fileMessageLogger;
	}

	public PeerMaker setFileMessageLogger(File fileMessageLogger)
	{
		this.fileMessageLogger = fileMessageLogger;
		return this;
	}

	public ConnectionConfiguration getConfiguration()
	{
		return configuration;
	}

	public PeerMaker setConfiguration(ConnectionConfiguration configuration)
	{
		this.configuration = configuration;
		return this;
	}

	public StorageGeneric getStorage()
	{
		return storage;
	}

	public PeerMaker setStorage(StorageGeneric storage)
	{
		this.storage = storage;
		return this;
	}

	public int getBagSize()
	{
		return bagSize;
	}

	public PeerMaker setBagSize(int bagSize)
	{
		this.bagSize = bagSize;
		return this;
	}

	public int getCacheTimeoutMillis()
	{
		return cacheTimeoutMillis;
	}

	public PeerMaker setCacheTimeoutMillis(int cacheTimeoutMillis)
	{
		this.cacheTimeoutMillis = cacheTimeoutMillis;
		return this;
	}

	public int getMaxNrBeforeExclude()
	{
		return maxNrBeforeExclude;
	}

	public PeerMaker setMaxNrBeforeExclude(int maxNrBeforeExclude)
	{
		this.maxNrBeforeExclude = maxNrBeforeExclude;
		return this;
	}

	public int[] getWaitingTimeBetweenNodeMaintenenceSeconds()
	{
		return waitingTimeBetweenNodeMaintenenceSeconds;
	}

	public PeerMaker setWaitingTimeBetweenNodeMaintenenceSeconds(
			int[] waitingTimeBetweenNodeMaintenenceSeconds)
	{
		this.waitingTimeBetweenNodeMaintenenceSeconds = waitingTimeBetweenNodeMaintenenceSeconds;
		return this;
	}

	public int getCacheSize()
	{
		return cacheSize;
	}

	public PeerMaker setCacheSize(int cacheSize)
	{
		this.cacheSize = cacheSize;
		return this;
	}

	public boolean isBehindFirewallPeerMap()
	{
		return isBehindFirewallPeerMap;
	}

	public PeerMaker setBehindFirewallPeerMap(boolean isBehindFirewallPeerMap)
	{
		this.isBehindFirewallPeerMap = isBehindFirewallPeerMap;
		return this;
	}

	public boolean isEnableHandShakeRPC()
	{
		return enableHandShakeRPC;
	}

	public PeerMaker setEnableHandShakeRPC(boolean enableHandShakeRPC)
	{
		this.enableHandShakeRPC = enableHandShakeRPC;
		return this;
	}

	public boolean isEnableStorageRPC()
	{
		return enableStorageRPC;
	}

	public PeerMaker setEnableStorageRPC(boolean enableStorageRPC)
	{
		this.enableStorageRPC = enableStorageRPC;
		return this;
	}

	public boolean isEnableNeighborRPC()
	{
		return enableNeighborRPC;
	}

	public PeerMaker setEnableNeighborRPC(boolean enableNeighborRPC)
	{
		this.enableNeighborRPC = enableNeighborRPC;
		return this;
	}

	public boolean isEnableQuitRPC()
	{
		return enableQuitRPC;
	}

	public PeerMaker setEnableQuitRPC(boolean enableQuitRPC)
	{
		this.enableQuitRPC = enableQuitRPC;
		return this;
	}

	public boolean isEnablePeerExchangeRPC()
	{
		return enablePeerExchangeRPC;
	}

	public PeerMaker setEnablePeerExchangeRPC(boolean enablePeerExchangeRPC)
	{
		this.enablePeerExchangeRPC = enablePeerExchangeRPC;
		return this;
	}

	public boolean isEnableDirectDataRPC()
	{
		return enableDirectDataRPC;
	}

	public PeerMaker setEnableDirectDataRPC(boolean enableDirectDataRPC)
	{
		this.enableDirectDataRPC = enableDirectDataRPC;
		return this;
	}

	public boolean isEnableTrackerRPC()
	{
		return enableTrackerRPC;
	}

	public PeerMaker setEnableTrackerRPC(boolean enableTrackerRPC)
	{
		this.enableTrackerRPC = enableTrackerRPC;
		return this;
	}

	public boolean isEnableTaskRPC()
	{
		return enableTaskRPC;
	}

	public PeerMaker setEnableTaskRPC(boolean enableTaskRPC)
	{
		this.enableTaskRPC = enableTaskRPC;
		return this;
	}

	public boolean isEnableRouting()
	{
		return enableRouting;
	}

	public PeerMaker setEnableRouting(boolean enableRouting)
	{
		this.enableRouting = enableRouting;
		return this;
	}

	public boolean isEnableDHT()
	{
		return enableDHT;
	}

	public PeerMaker setEnableDHT(boolean enableDHT)
	{
		this.enableDHT = enableDHT;
		return this;
	}

	public boolean isEnableTracker()
	{
		return enableTracker;
	}

	public PeerMaker setEnableTracker(boolean enableTracker)
	{
		this.enableTracker = enableTracker;
		return this;
	}

	public boolean isEnableTask()
	{
		return enableTask;
	}

	public PeerMaker setEnableTask(boolean enableTask)
	{
		this.enableTask = enableTask;
		return this;
	}

	public boolean isEnableMaintenance()
	{
		return enableMaintenance;
	}

	public PeerMaker setEnableMaintenance(boolean enableMaintenance)
	{
		this.enableMaintenance = enableMaintenance;
		return this;
	}

	public Peer getMasterPeer()
	{
		return masterPeer;
	}

	public PeerMaker setMasterPeer(Peer masterPeer)
	{
		this.masterPeer = masterPeer;
		return this;
	}
	
	public int getMaxMessageSize()
	{
		return maxMessageSize;
	}
	
	public PeerMaker setMaxMessageSize(int maxMessageSize)
	{
		this.maxMessageSize = maxMessageSize;
		return this;
	}

	public boolean isEnableIndirectReplication()
	{
		return enableIndirectReplication;
	}

	public PeerMaker setEnableIndirectReplication(boolean enableIndirectReplication)
	{
		this.enableIndirectReplication = enableIndirectReplication;
		return this;
	}
}