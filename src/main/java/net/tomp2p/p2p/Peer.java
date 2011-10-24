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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.ConnectionHandler;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureData;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRunnable;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.futures.FutureWrappedBootstrap;
import net.tomp2p.p2p.config.ConfigurationDirect;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.ConfigurationRemove;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.ConfigurationTrackerGet;
import net.tomp2p.p2p.config.ConfigurationTrackerStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapKadImpl;
import net.tomp2p.replication.DefaultStorageReplication;
import net.tomp2p.replication.Replication;
import net.tomp2p.replication.TrackerStorageReplication;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.HandshakeRPC;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.PeerExchangeRPC;
import net.tomp2p.rpc.QuitRPC;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.rpc.TrackerRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.ResponsibilityMemory;
import net.tomp2p.storage.StorageMemory;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.utils.CacheMap;
import net.tomp2p.utils.Utils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TomP2P implements besides the following distributed hash table (DHT)
 * operations:
 * <ul>
 * <li>value=get(locationKey)</li>
 * <li>put(locationKey,value)</li>
 * <li>remove(locationKey)</li>
 * </ul>
 * 
 * also the following operations:
 * <ul>
 * <li>value=get(locationKey,contentKey)</li>
 * <li>put(locationKey,contentKey,value)</li>
 * <li>remove(locationKey,contentKey)</li>
 * </ul>
 * 
 * The advantage of TomP2P is that multiple values can be stored in one
 * location. Furthermore, TomP2P also provides to store keys in different
 * domains to avoid key collisions.
 * 
 * @author Thomas Bocek
 * 
 */
public class Peer
{
	// domain used if no domain provided
	final private static Logger logger = LoggerFactory.getLogger(Peer.class);
	final private static KeyPair EMPTY_KEYPAIR = new KeyPair(null, null);
	// As soon as the user calls listen, this connection handler is set
	private ConnectionHandler connectionHandler;
	// the id of this node
	final private Number160 peerId;
	// the p2p network identifier, two different networks can have the same
	// ports
	private final int p2pID;
	private final KeyPair keyPair;
	private DistributedHashHashMap dht;
	private DistributedTracker tracker;
	// private Replication replication;
	// private final List<NodeListener> nodeListeners = new
	// LinkedList<NodeListener>();
	// private ScheduledFuture<?> sf;
	private IdentityManagement identityManagement;
	private Maintenance maintenance;
	private HandshakeRPC handshakeRCP;
	private StorageRPC storageRPC;
	private NeighborRPC neighborRPC;
	private QuitRPC quitRCP;
	private PeerExchangeRPC peerExchangeRPC;
	private DirectDataRPC directDataRPC;
	private TrackerRPC trackerRPC;
	private DistributedRouting routing;
	// public final static ScheduledThreadPoolExecutor SCHEDULED_POOL =
	// (ScheduledThreadPoolExecutor) Executors
	// .newScheduledThreadPool(5);
	// final private Semaphore semaphoreShortMessages = new Semaphore(20);
	// final private Semaphore semaphoreLongMessages = new Semaphore(20);
	private Bindings bindings;
	//
	final private P2PConfiguration peerConfiguration;
	final private ConnectionConfiguration connectionConfiguration;
	// for maintenannce
	private ScheduledExecutorService scheduledExecutorServiceMaintenance;
	private ScheduledExecutorService scheduledExecutorServiceReplication;
	final private Map<BaseFuture, Long> pendingFutures = Collections.synchronizedMap(new CacheMap<BaseFuture, Long>(
			1000));
	private boolean masterFlag = true;
	private List<ScheduledFuture<?>> scheduledFutures = Collections
			.synchronizedList(new ArrayList<ScheduledFuture<?>>());
	final private List<PeerListener> listeners = new ArrayList<PeerListener>();
	private Timer timer;
	final public static int BLOOMFILTER_SIZE = 1024;
	

	// private volatile boolean running = false;
	public Peer(final KeyPair keyPair)
	{
		this(Utils.makeSHAHash(keyPair.getPublic().getEncoded()), keyPair);
	}

	public Peer(final Number160 nodeId)
	{
		this(1, nodeId, new P2PConfiguration(), new ConnectionConfiguration(), EMPTY_KEYPAIR);
	}

	public Peer(final Number160 nodeId, final KeyPair keyPair)
	{
		this(1, nodeId, new P2PConfiguration(), new ConnectionConfiguration(), keyPair);
	}

	public Peer(final int p2pID, final KeyPair keyPair)
	{
		this(p2pID, Utils.makeSHAHash(keyPair.getPublic().getEncoded()), keyPair);
	}

	public Peer(final int p2pID, final Number160 nodeId)
	{
		this(p2pID, nodeId, new P2PConfiguration(), new ConnectionConfiguration(), EMPTY_KEYPAIR);
	}

	public Peer(final int p2pID, final Number160 nodeId, KeyPair keyPair)
	{
		this(p2pID, nodeId, new P2PConfiguration(), new ConnectionConfiguration(), keyPair);
	}

	public Peer(final int p2pID, final Number160 nodeId, final ConnectionConfiguration connectionConfiguration)
	{
		this(p2pID, nodeId, new P2PConfiguration(), connectionConfiguration, EMPTY_KEYPAIR);
	}

	public Peer(final int p2pID, final Number160 nodeId, final P2PConfiguration peerConfiguration,
			final ConnectionConfiguration connectionConfiguration, final KeyPair keyPair)
	{
		this.p2pID = p2pID;
		this.peerId = nodeId;
		this.peerConfiguration = peerConfiguration;
		this.connectionConfiguration = connectionConfiguration;
		this.keyPair = keyPair;
	}

	public void addPeerListener(PeerListener listener)
	{
		if (isRunning())
			listener.notifyOnStart();
		listeners.add(listener);
	}

	public void removePeerListener()
	{
		listeners.remove(listeners);
	}

	/**
	 * Closes all connections of this node
	 * 
	 * @throws InterruptedException
	 */
	public void shutdown()
	{
		logger.info("shutdown in progres");
		synchronized (scheduledFutures)
		{
			for (ScheduledFuture<?> scheduledFuture : scheduledFutures)
				scheduledFuture.cancel(true);
		}
		if (masterFlag && timer != null)
			timer.stop();
		if (masterFlag && scheduledExecutorServiceMaintenance != null)
			scheduledExecutorServiceMaintenance.shutdownNow();
		if (masterFlag && scheduledExecutorServiceReplication != null)
			scheduledExecutorServiceReplication.shutdownNow();
		getConnectionHandler().shutdown();
		for (PeerListener listener : listeners)
			listener.notifyOnShutdown();
		getPeerBean().getStorage().close();
		connectionHandler = null;
	}

	public void listen() throws Exception
	{
		listen(connectionConfiguration.getDefaultPort(), connectionConfiguration.getDefaultPort());
	}

	public void listen(File messageLogger) throws Exception
	{
		listen(connectionConfiguration.getDefaultPort(), connectionConfiguration.getDefaultPort(), messageLogger);
	}

	public void listen(final int udpPort, final int tcpPort) throws Exception
	{
		listen(udpPort, tcpPort, new Bindings());
	}

	public void listen(final int udpPort, final int tcpPort, File messageLogger) throws Exception
	{
		listen(udpPort, tcpPort, new Bindings(), messageLogger);
	}

	public void listen(final int udpPort, final int tcpPort, final InetAddress bind) throws Exception
	{
		listen(udpPort, tcpPort, new Bindings(bind), null);
	}

	public void listen(final int udpPort, final int tcpPort, final Bindings bindings) throws Exception
	{
		listen(udpPort, tcpPort, bindings, null);
	}

	/**
	 * Lets this node listen on a port
	 * 
	 * @param udpPort
	 *            the UDP port to listen on
	 * @param tcpPort
	 *            the TCP port to listen on
	 * @param bindInformation
	 *            contains IP addresses to listen on
	 * @param replication
	 * @param statServer
	 * @throws Exception
	 */
	public void listen(final int udpPort, final int tcpPort, final Bindings bindings, final File messageLogger)
			throws Exception
	{
		// I'm the master
		masterFlag = true;
		this.timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS, 10);
		this.bindings = bindings;
		this.scheduledExecutorServiceMaintenance = Executors.newScheduledThreadPool(peerConfiguration
				.getMaintenanceThreads());
		this.scheduledExecutorServiceReplication = Executors.newScheduledThreadPool(peerConfiguration
				.getReplicationThreads());

		PeerMap peerMap = new PeerMapKadImpl(peerId, peerConfiguration);
		Statistics statistics = peerMap.getStatistics();
		init(new ConnectionHandler(udpPort, tcpPort, peerId, bindings, getP2PID(), connectionConfiguration,
				messageLogger, keyPair, peerMap, listeners, peerConfiguration), statistics);
		logger.debug("init done");
	}

	public void listen(final Peer master) throws Exception
	{
		// I'm a slave
		masterFlag = false;
		this.timer = master.timer;
		this.bindings = master.bindings;
		this.scheduledExecutorServiceMaintenance = master.scheduledExecutorServiceMaintenance;
		this.scheduledExecutorServiceReplication = master.scheduledExecutorServiceReplication;
		PeerMap peerMap = new PeerMapKadImpl(peerId, peerConfiguration);
		Statistics statistics = peerMap.getStatistics();
		init(new ConnectionHandler(master.getConnectionHandler(), peerId, keyPair, peerMap), statistics);
	}

	protected void init(final ConnectionHandler connectionHandler, Statistics statistics)
	{
		this.connectionHandler = connectionHandler;
		PeerBean peerBean = connectionHandler.getPeerBean();
		peerBean.setStatistics(statistics);
		// peerBean.
		ConnectionBean connectionBean = connectionHandler.getConnectionBean();
		//
		PeerAddress selfAddress = getPeerAddress();
		PeerMap peerMap = connectionHandler.getPeerBean().getPeerMap();
		// create storage and add replication feature
		ResponsibilityMemory responsibilityMemory = new ResponsibilityMemory();
		StorageMemory storage = new StorageMemory(responsibilityMemory);
		peerBean.setStorage(storage);
		Replication replicationStorage = new Replication(responsibilityMemory, selfAddress, peerMap);
		peerBean.setReplicationStorage(replicationStorage);
		// create tracker and add replication feature
		identityManagement = new IdentityManagement(getPeerAddress());
		maintenance = new Maintenance();
		Replication replicationTracker = new Replication(new ResponsibilityMemory(), selfAddress, peerMap);
		TrackerStorage storageTracker = new TrackerStorage(identityManagement, getP2PConfiguration().getTrackerTimoutSeconds(), replicationTracker, maintenance);
		peerBean.setTrackerStorage(storageTracker);
		peerMap.addPeerOfflineListener(storageTracker);
		
		// RPC communication
		handshakeRCP = new HandshakeRPC(peerBean, connectionBean);
		storageRPC = new StorageRPC(peerBean, connectionBean);
		neighborRPC = new NeighborRPC(peerBean, connectionBean);
		quitRCP = new QuitRPC(peerBean, connectionBean);
		peerExchangeRPC = new PeerExchangeRPC(peerBean, connectionBean);
		directDataRPC = new DirectDataRPC(peerBean, connectionBean);
		// replication for trackers, which needs PEX
		TrackerStorageReplication trackerStorageReplication=new TrackerStorageReplication(this, peerExchangeRPC, pendingFutures, storageTracker);
		replicationTracker.addResponsibilityListener(trackerStorageReplication);
		trackerRPC = new TrackerRPC(peerBean, connectionBean);
		
		// distributed communication
		routing = new DistributedRouting(peerBean, neighborRPC);
		dht = new DistributedHashHashMap(routing, storageRPC, directDataRPC);
		tracker = new DistributedTracker(peerBean, routing, trackerRPC, peerExchangeRPC);
		// maintenance
		if (peerConfiguration.isStartMaintenance())
			startMaintainance();
		for (PeerListener listener : listeners)
			listener.notifyOnStart();
	}

	public void setDefaultStorageReplication()
	{
		Replication replicationStorage = getPeerBean().getReplicationStorage();
		DefaultStorageReplication defaultStorageReplication = new DefaultStorageReplication(this,
				getPeerBean().getStorage(), storageRPC, pendingFutures);
		scheduledFutures.add(addIndirectReplicaiton(defaultStorageReplication));
		replicationStorage.addResponsibilityListener(defaultStorageReplication);
	}

	public Map<BaseFuture, Long> getPendingFutures()
	{
		return pendingFutures;
	}

	public boolean isRunning()
	{
		return connectionHandler != null;
	}

	public boolean isListening()
	{
		if (!isRunning())
			return false;
		return connectionHandler.isListening();
	}

	void startMaintainance()
	{
		scheduledFutures.add(addMaintainance(new Maintenance2(connectionHandler.getPeerBean().getPeerMap(),
				handshakeRCP, peerConfiguration)));
	}

	public void customLoggerMessage(String customMessage)
	{
		getConnectionHandler().customLoggerMessage(customMessage);
	}

	// public ConfigurationConnection getConfiguration()
	// {
	// return peerConfiguration;
	// }
	public HandshakeRPC getHandshakeRPC()
	{
		if (handshakeRCP == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		return handshakeRCP;
	}

	public StorageRPC getStoreRPC()
	{
		if (storageRPC == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		return storageRPC;
	}

	public QuitRPC getQuitRPC()
	{
		if (quitRCP == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		return quitRCP;
	}

	public PeerExchangeRPC getPeerExchangeRPC()
	{
		if (peerExchangeRPC == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		return peerExchangeRPC;
	}

	public DirectDataRPC getDirectDataRPC()
	{
		if (directDataRPC == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		return directDataRPC;
	}

	public TrackerRPC getTrackerRPC()
	{
		if (trackerRPC == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		return trackerRPC;
	}

	public DistributedRouting getRouting()
	{
		if (routing == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		return routing;
	}

	public ScheduledFuture<?> addIndirectReplicaiton(Runnable runnable)
	{
		return scheduledExecutorServiceReplication.scheduleWithFixedDelay(runnable,
				peerConfiguration.getReplicationRefreshMillis(), peerConfiguration.getReplicationRefreshMillis(),
				TimeUnit.MILLISECONDS);
	}

	public ScheduledFuture<?> addMaintainance(Runnable runnable)
	{
		return scheduledExecutorServiceMaintenance.scheduleWithFixedDelay(runnable, 0,
				(peerConfiguration.getWaitingTimeBetweenNodeMaintenenceSeconds()[0] + 1) / 2, TimeUnit.SECONDS);
	}

	public ConnectionHandler getConnectionHandler()
	{
		if (connectionHandler == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		else
			return connectionHandler;
	}

	public DistributedHashHashMap getDHT()
	{
		if (dht == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		return dht;
	}

	public DistributedTracker getTracker()
	{
		if (tracker == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		return tracker;
	}

	public PeerBean getPeerBean()
	{
		return getConnectionHandler().getPeerBean();
	}

	public ConnectionBean getConnectionBean()
	{
		return getConnectionHandler().getConnectionBean();
	}

	public Number160 getPeerID()
	{
		return peerId;
	}

	public PeerAddress getPeerAddress()
	{
		return getPeerBean().getServerPeerAddress();
	}

	public void setPeerMap(final PeerMap peerMap)
	{
		getPeerBean().setPeerMap(peerMap);
	}

	public int getP2PID()
	{
		return p2pID;
	}

	public void setRawDataReply(final RawDataReply rawDataReply)
	{
		getDirectDataRPC().setReply(rawDataReply);
	}

	public void setObjectDataReply(final ObjectDataReply objectDataReply)
	{
		getDirectDataRPC().setReply(objectDataReply);
	}
	
	/**
	 * Opens a TCP connection and keeps it open. The user can provide the idle timeout, which means that the 
	 * connection gets closed after that time of inacitivty. If the other peer goes offline or closes the connection 
	 * (due to inactivity), further requests with this connections reopens the connection. 
	 * 
	 * @param destination The end-point to connect to
	 * @param idleSeconds time in seconds after a connection gets closed if idle, -1 if it should remain always open until 
	 * 			the user closes the connection manually.
	 * @return A class that needs to be passed to those methods that should use the already open connection.
	 */
	public PeerConnection createPeerConnection(PeerAddress destination, int idleSeconds)
	{
		return new PeerConnection();
	}

	// custom message
	public FutureData send(final PeerAddress remotePeer, final ChannelBuffer requestBuffer)
	{
		return send(remotePeer, requestBuffer, true);
	}
	public FutureData send(final PeerConnection connection, final ChannelBuffer requestBuffer)
	{
		//not supported yet
		return null;
	}
	
	public FutureData send(final PeerAddress remotePeer, final Object object)
			throws IOException
	{
		byte[] me = Utils.encodeJavaObject(object);
		return send(remotePeer, ChannelBuffers.wrappedBuffer(me), false);
	}
	
	public FutureData send(final PeerConnection connection, final Object object)
			throws IOException
	{
		//not supported yet
		return null;
	}
	private FutureData send(final PeerAddress remotePeer, final ChannelBuffer requestBuffer, boolean raw)
	{
		if (Thread.currentThread().getName().startsWith(ConnectionHandler.THREAD_NAME))
		{
			//delayed reservation
			final RequestHandlerTCP request = getDirectDataRPC().send(remotePeer, requestBuffer.slice(), raw);
			FutureRunnable runner=new FutureRunnable()
			{
				@Override
				public void run()
				{
					final ChannelCreator cc=getConnectionBean().getReservation().reserve(1);
					request.sendTCP(cc);
					Utils.addReleaseListener(request.getFutureResponse(), cc, 1);
				}

				@Override
				public void failed(String reason) 
				{
					request.getFutureResponse().setFailed(reason);	
				}
			};
			getConnectionBean().getScheduler().callLater(runner);
			return (FutureData) request.getFutureResponse();
		}
		else
		{
			final ChannelCreator cc=getConnectionBean().getReservation().reserve(1);
			RequestHandlerTCP request = getDirectDataRPC().send(remotePeer, requestBuffer.slice(), raw);
			request.sendTCP(cc);
			Utils.addReleaseListener(request.getFutureResponse(), cc, 1);
			return (FutureData) request.getFutureResponse();
		}
	}

	// Boostrap and ping

	public FutureBootstrap bootstrapBroadcast()
	{
		return bootstrapBroadcast(connectionConfiguration.getDefaultPort());
	}

	public FutureBootstrap bootstrapBroadcast(int port)
	{
		final FutureWrappedBootstrap result = new FutureWrappedBootstrap();
		// limit after
		final FutureForkJoin<FutureResponse> tmp = pingBroadcast(port);
		tmp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>()
		{
			@Override
			public void operationComplete(final FutureForkJoin<FutureResponse> future) throws Exception
			{
				if (future.isSuccess())
				{
					FutureRunnable runner = new FutureRunnable() 
					{
						@Override
						public void run() 
						{
							final PeerAddress sender = future.getLast().getResponse().getSender();
							result.waitForBootstrap(bootstrap(sender));
						}
						@Override
						public void failed(String reason)
						{
							result.setFailed(reason);						
						}
					};
					getConnectionBean().getScheduler().callLater(runner);
					
				}
				else
					result.setFailed("could not reach anyone with the broadcast");
			}
		});
		return result;
	}

	FutureForkJoin<FutureResponse> pingBroadcast(int port)
	{
		final int size = bindings.getBroadcastAddresses().size();
		if (size > 0)
		{
			final FutureResponse[] validBroadcast = new FutureResponse[size];
			if (Thread.currentThread().getName().startsWith(ConnectionHandler.THREAD_NAME))
				throw new RuntimeException("calling from IO thread not supported yet, may cause a deadlock.");
			final ChannelCreator cc=getConnectionBean().getReservation().reserve(size);
			for (int i = 0; i < size; i++)
			{
				final InetAddress broadcastAddress = bindings.getBroadcastAddresses().get(i);
				final PeerAddress peerAddress = new PeerAddress(Number160.ZERO, broadcastAddress, port, port);
				validBroadcast[i] = getHandshakeRPC().pingBroadcastUDP(peerAddress, cc);
				Utils.addReleaseListener(validBroadcast[i], cc, 1);
				logger.debug("ping broadcast to " + broadcastAddress);
			}
			final FutureForkJoin<FutureResponse> pings = new FutureForkJoin<FutureResponse>(1, true, validBroadcast);
			return pings;
		}
		else
			throw new IllegalArgumentException("No broadcast address found. Cannot ping nothing");
	}

	

	public FutureResponse ping(final InetSocketAddress address)
	{
		if (Thread.currentThread().getName().startsWith(ConnectionHandler.THREAD_NAME))
			throw new RuntimeException("calling from IO thread not supported yet, may cause a deadlock.");
		final ChannelCreator cc=getConnectionBean().getReservation().reserve(1);
		FutureResponse futureResponse = getHandshakeRPC().pingUDP(new PeerAddress(Number160.ZERO, address), cc);
		Utils.addReleaseListener(futureResponse, cc, 1);
		return futureResponse;
	}

	public FutureBootstrap bootstrap(final InetSocketAddress address)
	{
		final FutureWrappedBootstrap result = new FutureWrappedBootstrap();
		final FutureResponse tmp = ping(address);
		tmp.addListener(new BaseFutureAdapter<FutureResponse>()
		{
			@Override
			public void operationComplete(final FutureResponse future) throws Exception
			{
				if (future.isSuccess())
				{
					final PeerAddress sender = future.getResponse().getSender();
					//need to invoke later as we run in a netty thread
					FutureRunnable runner = new FutureRunnable() 
					{
						@Override
						public void run() 
						{
							result.waitForBootstrap(bootstrap(sender));
						}
						
						@Override
						public void failed(String string) 
						{
							result.setFailed(string);
						}
					};
					getConnectionBean().getScheduler().callLater(runner);
				}
				else 
				{
					result.setFailed("could not reach anyone with the broadcast");
				}
			}
		});
		return result;
	}

	public FutureBootstrap bootstrap(final PeerAddress peerAddress)
	{
		Collection<PeerAddress> bootstrapTo=new ArrayList<PeerAddress>(1);
		bootstrapTo.add(peerAddress);
		return bootstrap(peerAddress, bootstrapTo, Configurations.defaultStoreConfiguration());
	}

	public FutureBootstrap bootstrap(final PeerAddress peerAddress, final Collection<PeerAddress> bootstrapTo, final ConfigurationStore config)
	{
		int conn=Math.max(config.getRoutingConfiguration().getParallel(),config.getRequestP2PConfiguration().getParallel());
		if (peerConfiguration.isBehindFirewall())
		{
			final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn * 2);
			if(bindings.isSetupUPNP())
				setupPortForwandingUPNP();
			final FutureWrappedBootstrap result = new FutureWrappedBootstrap();
			FutureDiscover futureDiscover = discover(peerAddress);
			futureDiscover.addListener(new BaseFutureAdapter<FutureDiscover>()
			{
				@Override
				public void operationComplete(FutureDiscover future) throws Exception
				{
					if (future.isSuccess())
					{
						FutureBootstrap futureBootstrap = routing.bootstrap(
								bootstrapTo,
								config.getRoutingConfiguration().getMaxNoNewInfo(
										config.getRequestP2PConfiguration().getMinimumResults()), config
										.getRoutingConfiguration().getMaxFailures(), config.getRoutingConfiguration()
										.getMaxSuccess(), config.getRoutingConfiguration().getParallel(), false, cc);
						result.waitForBootstrap(futureBootstrap);
						Utils.addReleaseListenerAll(futureBootstrap, cc);
					}
					else {
						result.setFailed("Network discovery failed.");
						cc.release();
					}

				}
			});
			return result;
		}
		else
		{
			final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn * 2);
			FutureBootstrap futureBootstrap = routing.bootstrap(bootstrapTo, config.getRoutingConfiguration()
					.getMaxNoNewInfo(config.getRequestP2PConfiguration().getMinimumResults()), config
					.getRoutingConfiguration().getMaxFailures(), config.getRoutingConfiguration().getMaxSuccess(),
					config.getRoutingConfiguration().getParallel(), false, cc);
			Utils.addReleaseListenerAll(futureBootstrap, cc);
			return futureBootstrap;
		}
	}
	
	/**
	 * The Dynamic and/or Private Ports are those from 49152 through 65535 (http://www.iana.org/assignments/port-numbers)
	 * @param port
	 * @return
	 */
	public void setupPortForwandingUPNP()
	{
		final int range = 65535 - 49152;
		Random rnd = new Random();
		int portUDP=bindings.getOutsideUDPPort();
		if(portUDP==0)
		{
			portUDP=rnd.nextInt(range)+49152;
			bindings.setOutsidePortUDP(portUDP);
		}
		int portTCP=bindings.getOutsideTCPPort();
		if(portTCP==0)
		{
			portTCP=rnd.nextInt(range)+49152;
			bindings.setOutsidePortTCP(portTCP);
		}
		try
		{
			connectionHandler.mapUPNP( getPeerAddress().portUDP(), getPeerAddress().portTCP(), portUDP, portTCP);
		}
		catch (IOException e)
		{
			logger.warn("cannot find UPNP devices "+e);
		}
		
	}

	/**
	 * Discover attempts to find the external IP address of this peer. This is done by first trying to set UPNP 
	 * with port forwarding (gives us the external address), query UPNP for the external address, and 
	 * pinging a well known peer.
	 *  
	 * @param peerAddress
	 * @return
	 */
	public FutureDiscover discover(final PeerAddress peerAddress)
	{
		final FutureDiscover futureDiscover = new FutureDiscover(timer, peerConfiguration.getDiscoverTimeoutSec());
		if (Thread.currentThread().getName().startsWith(ConnectionHandler.THREAD_NAME))
			throw new RuntimeException("calling from IO thread not supported yet, may cause a deadlock.");
		final ChannelCreator cc=getConnectionBean().getReservation().reserve(3);
		final FutureResponse futureResponseTCP = getHandshakeRPC().pingTCPDiscover(peerAddress, cc);
		Utils.addReleaseListener(futureResponseTCP, cc, 1);
		addPeerListener(new PeerListener()
		{
			private boolean changedUDP=false;
			private boolean changedTCP=false;
			
			@Override
			public void serverAddressChanged(PeerAddress peerAddress, boolean tcp)
			{
				if(tcp)
					changedTCP=true;
				else
					changedUDP=true;
				if(changedTCP && changedUDP) 
				{
					futureDiscover.done(peerAddress);
				}
			}

			@Override
			public void notifyOnStart()
			{
			}

			@Override
			public void notifyOnShutdown()
			{
			}
		});
		futureResponseTCP.addListener(new BaseFutureAdapter<FutureResponse>()
		{
			@Override
			public void operationComplete(FutureResponse future) throws Exception
			{
				PeerAddress serverAddress = getPeerBean().getServerPeerAddress();
				if (futureResponseTCP.isSuccess())
				{
					Collection<PeerAddress> tmp = futureResponseTCP.getResponse().getNeighbors();
					if (tmp.size() == 1)
					{
						PeerAddress seenAs = tmp.iterator().next();
						logger.debug("I'm seen as " + seenAs + " by peer " + peerAddress);
						if (!getPeerAddress().getInetAddress().equals(seenAs.getInetAddress()))
						{
							serverAddress=serverAddress.changePorts(bindings.getOutsideUDPPort(), bindings.getOutsideTCPPort());
							serverAddress=serverAddress.changeAddress(seenAs.getInetAddress());
							getPeerBean().setServerPeerAddress(serverAddress);
							//
							FutureResponse fr1 = getHandshakeRPC().pingTCPProbe(peerAddress, cc);
							FutureResponse fr2 = getHandshakeRPC().pingUDPProbe(peerAddress, cc);
							Utils.addReleaseListener(fr1, cc, 1);
							Utils.addReleaseListener(fr2, cc, 1);
						}
						// else -> we announce exactly how the other peer sees
						// us
						else
						{
							//important to release connection if not needed
							cc.release(2);
							futureDiscover.done(seenAs);
						}
					}
					else
					{
						//important to release connection if not needed
						cc.release(2);
						futureDiscover.setFailed("Peer " + peerAddress + " did not report our IP address");
						return;
					}
				}
				else
				{
					//important to release connection if not needed
					cc.release(2);
					futureDiscover.setFailed("FutureDiscover: We need at least the TCP connection");
					return;
				}
			}
		});
		return futureDiscover;
	}

	// ////////////////// from here, DHT calls
	// PUT
	public FutureDHT put(final Number160 locationKey, final Data data)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		return put(locationKey, data, Configurations.defaultStoreConfiguration());
	}

	public FutureDHT put(final Number160 locationKey, final Data data, ConfigurationStore config)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		final Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
		dataMap.put(config.getContentKey(), data);
		return put(locationKey, dataMap, config);
	}

	public FutureDHT put(final Number160 locationKey, final Map<Number160, Data> dataMap,
			final ConfigurationStore config)
	{
		
		int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getRequestP2PConfiguration().getParallel());
		final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
		
		final FutureDHT futureDHT = getDHT().put(locationKey, config.getDomain(), dataMap,
				config.getRoutingConfiguration(), config.getRequestP2PConfiguration(), config.isStoreIfAbsent(),
				config.isProtectDomain(), config.isSignMessage(), config.getFutureCreate(), cc);
		if (config.getRefreshSeconds() > 0)
		{
			ScheduledFuture<?> tmp = schedulePut(locationKey, dataMap, config, futureDHT);
			futureDHT.setScheduledFuture(tmp, scheduledFutures);
		}
		Utils.addReleaseListenerAll(futureDHT, cc);
		return futureDHT;
	}

	private ScheduledFuture<?> schedulePut(final Number160 locationKey, final Map<Number160, Data> dataMap,
			final ConfigurationStore config, final FutureDHT futureDHT)
	{
		
		
		Runnable runner = new Runnable()
		{
			@Override
			public void run()
			{
				int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getRequestP2PConfiguration().getParallel());
				final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
				
				FutureDHT futureDHT2 = getDHT().put(locationKey, config.getDomain(), dataMap,
						config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
						config.isStoreIfAbsent(), config.isProtectDomain(), config.isSignMessage(),
						config.getFutureCreate(), cc);
				futureDHT.created(futureDHT2);
				Utils.addReleaseListenerAll(futureDHT2, cc);
			}
		};
		ScheduledFuture<?> tmp = scheduledExecutorServiceReplication.scheduleAtFixedRate(runner,
				config.getRefreshSeconds(), config.getRefreshSeconds(), TimeUnit.SECONDS);
		scheduledFutures.add(tmp);
		return tmp;
	}

	// ADD
	public FutureDHT add(final Number160 locationKey, final Data data)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		return add(locationKey, data, Configurations.defaultStoreConfiguration());
	}

	public FutureDHT add(final Number160 locationKey, final Data data, final ConfigurationStore config)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		final Collection<Data> dataCollection = new ArrayList<Data>();
		dataCollection.add(data);
		return add(locationKey, dataCollection, config);
	}

	public FutureDHT add(final Number160 locationKey, final Collection<Data> dataCollection,
			final ConfigurationStore config)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		if (config.getContentKey() != null && !config.getContentKey().equals(Number160.ZERO))
			logger.warn("Warning, setting a content key in add() does not have any effect");
		if (!config.isSignMessage())
		{
			for (Data data : dataCollection)
			{
				if (data.isProtectedEntry())
				{
					config.setSignMessage(true);
					break;
				}
			}
		}
		int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getRequestP2PConfiguration().getParallel());
		final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
		
		final FutureDHT futureDHT = getDHT().add(locationKey, config.getDomain(), dataCollection,
				config.getRoutingConfiguration(), config.getRequestP2PConfiguration(), config.isProtectDomain(),
				config.isSignMessage(), config.getFutureCreate(), cc);
		if (config.getRefreshSeconds() > 0)
		{
			ScheduledFuture<?> tmp = scheduleAdd(locationKey, dataCollection, config, futureDHT);
			futureDHT.setScheduledFuture(tmp, scheduledFutures);
		}
		Utils.addReleaseListenerAll(futureDHT, cc);
		return futureDHT;
	}

	private ScheduledFuture<?> scheduleAdd(final Number160 locationKey, final Collection<Data> dataCollection,
			final ConfigurationStore config, final FutureDHT futureDHT)
	{
		Runnable runner = new Runnable()
		{
			@Override
			public void run()
			{
				int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getRequestP2PConfiguration().getParallel());
				final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
				
				FutureDHT futureDHT2 = getDHT().add(locationKey, config.getDomain(), dataCollection,
						config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
						config.isProtectDomain(), config.isSignMessage(), config.getFutureCreate(), cc);
				futureDHT.created(futureDHT2);
				Utils.addReleaseListenerAll(futureDHT2, cc);
			}
		};
		ScheduledFuture<?> tmp = scheduledExecutorServiceReplication.scheduleAtFixedRate(runner,
				config.getRefreshSeconds(), config.getRefreshSeconds(), TimeUnit.SECONDS);
		scheduledFutures.add(tmp);
		return tmp;
	}

	// GET
	public FutureDHT getAll(final Number160 locationKey)
	{
		return get(locationKey, null, Configurations.defaultGetConfiguration());
	}

	public FutureDHT getAll(final Number160 locationKey, final ConfigurationGet config)
	{
		return get(locationKey, null, config);
	}

	public FutureDHT get(final Number160 locationKey)
	{
		return get(locationKey, Configurations.defaultGetConfiguration());
	}

	public FutureDHT get(final Number160 locationKey, final ConfigurationGet config)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		Set<Number160> keyCollection = new HashSet<Number160>();
		keyCollection.add(config.getContentKey());
		return get(locationKey, keyCollection, config);
	}

	public FutureDHT get(final Number160 locationKey, Set<Number160> keyCollection, final ConfigurationGet config)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getRequestP2PConfiguration().getParallel());
		final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
		
		final FutureDHT futureDHT = getDHT().get(locationKey, config.getDomain(), keyCollection, config.getPublicKey(),
				config.getRoutingConfiguration(), config.getRequestP2PConfiguration(), config.getEvaluationScheme(),
				config.isSignMessage(), cc);
		Utils.addReleaseListenerAll(futureDHT, cc);
		return futureDHT;
	}

	// REMOVE
	public FutureDHT removeAll(final Number160 locationKey)
	{
		return remove(locationKey, null, Configurations.defaultRemoveConfiguration());
	}

	public FutureDHT removeAll(final Number160 locationKey, ConfigurationRemove config)
	{
		return remove(locationKey, null, config);
	}

	public FutureDHT remove(final Number160 locationKey)
	{
		return remove(locationKey, Configurations.defaultRemoveConfiguration());
	}

	public FutureDHT remove(final Number160 locationKey, ConfigurationRemove config)
	{
		Set<Number160> keyCollection = new HashSet<Number160>();
		keyCollection.add(config.getContentKey());
		return remove(locationKey, keyCollection, config);
	}

	public FutureDHT remove(final Number160 locationKey, final Set<Number160> keyCollection,
			final ConfigurationRemove config)
	{
		if (keyCollection != null)
		{
			for (Number160 contentKey : keyCollection)
				getPeerBean().getStorage().remove(new Number480(locationKey, config.getDomain(), contentKey),
						keyPair.getPublic());
		}
		else
			getPeerBean().getStorage().remove(new Number320(locationKey, config.getDomain()), keyPair.getPublic());
		
		int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getRequestP2PConfiguration().getParallel());
		final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
					
		final FutureDHT futureDHT = getDHT().remove(locationKey, config.getDomain(), keyCollection,
				config.getRoutingConfiguration(), config.getRequestP2PConfiguration(), config.isReturnResults(),
				config.isSignMessage(), config.getFutureCreate(), cc);
		if (config.getRefreshSeconds() > 0 && config.getRepetitions() > 0)
		{
			ScheduledFuture<?> tmp = scheduleRemove(locationKey, keyCollection, config, futureDHT);
			futureDHT.setScheduledFuture(tmp, scheduledFutures);
		}
		Utils.addReleaseListenerAll(futureDHT, cc);
		return futureDHT;
	}

	private ScheduledFuture<?> scheduleRemove(final Number160 locationKey, final Set<Number160> keyCollection,
			final ConfigurationRemove config, final FutureDHT futureDHT)
	{
		final int repetion = config.getRepetitions();
		final class MyRunnable implements Runnable
		{
			private ScheduledFuture<?> future;
			private boolean canceled = false;
			private int counter = 0;

			@Override
			public void run()
			{
				int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getRequestP2PConfiguration().getParallel());
				final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
				
				FutureDHT futureDHT2 = getDHT().remove(locationKey, config.getDomain(), keyCollection,
						config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
						config.isReturnResults(), config.isSignMessage(), config.getFutureCreate(), cc);
				futureDHT.created(futureDHT2);
				Utils.addReleaseListenerAll(futureDHT2, cc);
				if (++counter >= repetion)
				{
					synchronized (this)
					{
						canceled = true;
						if (future != null)
							future.cancel(false);
					}
				}
			}

			public void setFuture(ScheduledFuture<?> future)
			{
				synchronized (this)
				{
					if (canceled == true)
						future.cancel(false);
					else
						this.future = future;
				}
			}
		}
		MyRunnable myRunnable = new MyRunnable();
		final ScheduledFuture<?> tmp = scheduledExecutorServiceReplication.scheduleAtFixedRate(myRunnable,
				config.getRefreshSeconds(), config.getRefreshSeconds(), TimeUnit.SECONDS);
		myRunnable.setFuture(tmp);
		scheduledFutures.add(tmp);
		return tmp;
	}

	// Direct
	public FutureDHT send(final Number160 locationKey, final ChannelBuffer buffer)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		return send(locationKey, buffer, Configurations.defaultConfigurationDirect());
	}

	public FutureDHT send(final Number160 locationKey, final ChannelBuffer buffer, final ConfigurationDirect config)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getRequestP2PConfiguration().getParallel());
		final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
		
		final FutureDHT futureDHT = getDHT().direct(locationKey, buffer, true, config.getRoutingConfiguration(),
				config.getRequestP2PConfiguration(), config.getFutureCreate(), config.isCancelOnFinish(), cc);
		if (config.getRefreshSeconds() > 0 && config.getRepetitions() > 0)
		{
			ScheduledFuture<?> tmp = scheduleSend(locationKey, buffer, config, futureDHT);
			futureDHT.setScheduledFuture(tmp, scheduledFutures);
		}
		Utils.addReleaseListenerAll(futureDHT, cc);
		return futureDHT;
	}

	public FutureDHT send(final Number160 locationKey, final Object object) throws IOException
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		return send(locationKey, object, Configurations.defaultConfigurationDirect());
	}

	public FutureDHT send(final Number160 locationKey, final Object object, final ConfigurationDirect config)
			throws IOException
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		byte[] me = Utils.encodeJavaObject(object);
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(me);
		int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getRequestP2PConfiguration().getParallel());
		final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
		
		final FutureDHT futureDHT = getDHT().direct(locationKey, buffer, false, config.getRoutingConfiguration(),
				config.getRequestP2PConfiguration(), config.getFutureCreate(), config.isCancelOnFinish(), cc);
		if (config.getRefreshSeconds() > 0 && config.getRepetitions() > 0)
		{
			ScheduledFuture<?> tmp = scheduleSend(locationKey, buffer, config, futureDHT);
			futureDHT.setScheduledFuture(tmp, scheduledFutures);
		}
		Utils.addReleaseListenerAll(futureDHT, cc);
		return futureDHT;
	}

	private ScheduledFuture<?> scheduleSend(final Number160 locationKey, final ChannelBuffer buffer,
			final ConfigurationDirect config, final FutureDHT futureDHT)
	{
		final int repetion = config.getRepetitions();
		final class MyRunnable implements Runnable
		{
			private ScheduledFuture<?> future;
			private boolean canceled = false;
			private int counter = 0;

			@Override
			public void run()
			{
				int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getRequestP2PConfiguration().getParallel());
				final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
				
				final FutureDHT futureDHT2 = getDHT().direct(locationKey, buffer, false,
						config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
						config.getFutureCreate(), config.isCancelOnFinish(), cc);
				futureDHT.created(futureDHT2);
				Utils.addReleaseListenerAll(futureDHT2, cc);
				if (++counter >= repetion)
				{
					synchronized (this)
					{
						canceled = true;
						if (future != null)
							future.cancel(false);
					}
				}
			}

			public void setFuture(ScheduledFuture<?> future)
			{
				synchronized (this)
				{
					if (canceled == true)
						future.cancel(false);
					else
						this.future = future;
				}
			}
		}
		MyRunnable myRunnable = new MyRunnable();
		final ScheduledFuture<?> tmp = scheduledExecutorServiceReplication.scheduleAtFixedRate(myRunnable,
				config.getRefreshSeconds(), config.getRefreshSeconds(), TimeUnit.SECONDS);
		myRunnable.setFuture(tmp);
		scheduledFutures.add(tmp);
		return tmp;
	}

	// GET TRACKER
	public FutureTracker getFromTracker(Number160 locationKey, ConfigurationTrackerGet config)
	{
		// make a good guess based on the config and the maxium tracker that can
		// be found
		return getFromTracker(locationKey, config, new SimpleBloomFilter<Number160>(BLOOMFILTER_SIZE, 200));
	}

	public FutureTracker getFromTrackerCreateBloomfilter1(Number160 locationKey, ConfigurationTrackerGet config,
			Collection<PeerAddress> knownPeers)
	{
		// make a good guess based on the config and the maxium tracker that can
		// be found
		SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(BLOOMFILTER_SIZE, 200);
		if (!knownPeers.isEmpty())
		{
			for (PeerAddress peerAddress : knownPeers)
			{
				bloomFilter.add(peerAddress.getID());
			}
		}
		return getFromTracker(locationKey, config, bloomFilter);
	}

	public FutureTracker getFromTrackerCreateBloomfilter2(Number160 locationKey, ConfigurationTrackerGet config,
			Collection<Number160> knownPeers)
	{
		SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(BLOOMFILTER_SIZE, 200);
		//System.err.println("FP-Rate:"+bloomFilter.expectedFalsePositiveProbability());
		if (!knownPeers.isEmpty())
		{
			for (Number160 number160 : knownPeers)
			{
				bloomFilter.add(number160);
			}
		}
		return getFromTracker(locationKey, config, bloomFilter);
	}

	public FutureTracker getFromTracker(Number160 locationKey, ConfigurationTrackerGet config,
			Set<Number160> knownPeers)
	{
		int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getTrackerConfiguration().getParallel());
		final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
		
		FutureTracker futureTracker = getTracker().getFromTracker(locationKey, config.getDomain(), config.getRoutingConfiguration(),
				config.getTrackerConfiguration(), config.isExpectAttachement(), config.getEvaluationScheme(),
				config.isSignMessage(), config.isUseSecondaryTrackers(), knownPeers, cc);
		Utils.addReleaseListenerAll(futureTracker, cc);
		return futureTracker;
	}

	public FutureTracker addToTracker(final Number160 locationKey, final ConfigurationTrackerStore config)
	{
		// make a good guess based on the config and the maxium tracker that can
		// be found
		SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(BLOOMFILTER_SIZE, 1024);
		// add myself to my local tracker, since we use a mesh we are part of the tracker mesh as well.
		getPeerBean().getTrackerStorage().put(locationKey, config.getDomain(), getPeerAddress(), getPeerBean().getKeyPair().getPublic(), config.getAttachement());
		int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getTrackerConfiguration().getParallel());
		final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
		
		final FutureTracker futureTracker = getTracker().addToTracker(locationKey, config.getDomain(),
				config.getAttachement(), config.getRoutingConfiguration(), config.getTrackerConfiguration(),
				config.isSignMessage(), config.getFutureCreate(), bloomFilter, cc);
		if (getPeerBean().getTrackerStorage().getTrackerTimoutSeconds() > 0)
		{
			ScheduledFuture<?> tmp = scheduleAddTracker(locationKey, config, futureTracker);
			futureTracker.setScheduledFuture(tmp, scheduledFutures);
		}
		if (config.getWaitBeforeNextSendSeconds() > 0)
		{
			ScheduledFuture<?> tmp = schedulePeerExchange(locationKey, config, futureTracker);
			futureTracker.setScheduledFuture(tmp, scheduledFutures);
		}
		Utils.addReleaseListenerAll(futureTracker, cc);
		return futureTracker;
	}

	private ScheduledFuture<?> scheduleAddTracker(final Number160 locationKey, final ConfigurationTrackerStore config,
			final FutureTracker futureTracker)
	{
		Runnable runner = new Runnable()
		{
			@Override
			public void run()
			{
				// make a good guess based on the config and the maxium tracker
				// that can be found
				SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(BLOOMFILTER_SIZE, 1024);
				int conn=Math.max(config.getRoutingConfiguration().getParallel(), config.getTrackerConfiguration().getParallel());
				final ChannelCreator cc=getConnectionBean().getReservation().reserve(conn);
				
				FutureTracker futureTracker2 = getTracker().addToTracker(locationKey, config.getDomain(),
						config.getAttachement(), config.getRoutingConfiguration(), config.getTrackerConfiguration(),
						config.isSignMessage(), config.getFutureCreate(), bloomFilter, cc);
				futureTracker.repeated(futureTracker2);
				Utils.addReleaseListenerAll(futureTracker2, cc);
			}
		};
		int refresh = getPeerBean().getTrackerStorage().getTrackerTimoutSeconds() * 3 / 4;
		ScheduledFuture<?> tmp = scheduledExecutorServiceReplication.scheduleAtFixedRate(runner, refresh, refresh,
				TimeUnit.SECONDS);
		scheduledFutures.add(tmp);
		return tmp;
	}

	private ScheduledFuture<?> schedulePeerExchange(final Number160 locationKey,
			final ConfigurationTrackerStore config, final FutureTracker futureTracker)
	{
		Runnable runner = new Runnable()
		{
			@Override
			public void run()
			{
				final ChannelCreator cc=getConnectionBean().getReservation().reserve(TrackerStorage.TRACKER_SIZE);
				FutureForkJoin<FutureResponse> futureForkJoin = getTracker().startPeerExchange(locationKey,
						config.getDomain(), cc);
				futureTracker.repeated(futureForkJoin);
				Utils.addReleaseListenerAll(futureForkJoin, cc);
			}
		};
		int refresh = config.getWaitBeforeNextSendSeconds();
		ScheduledFuture<?> tmp = scheduledExecutorServiceReplication.scheduleAtFixedRate(runner, refresh, refresh,
				TimeUnit.SECONDS);
		scheduledFutures.add(tmp);
		return tmp;
	}

	private class Maintenance2 implements Runnable
	{
		final int max;
		final Map<PeerAddress, FutureResponse> result = new HashMap<PeerAddress, FutureResponse>();
		final private PeerMap peerMap;
		final private HandshakeRPC handshakeRPC;

		public Maintenance2(PeerMap peerMap, HandshakeRPC handshakeRPC, P2PConfiguration config)
		{
			this.peerMap = peerMap;
			this.handshakeRPC = handshakeRPC;
			this.max = config.getMaintenanceThreads();
		}

		@Override
		public void run()
		{
			Collection<PeerAddress> nas = peerMap.peersForMaintenance();
			if (logger.isDebugEnabled())
				logger.debug("numbe of peers for maintenance: " + nas.size());
			
			for (PeerAddress na : nas)
			{
				ChannelCreator cc = getConnectionBean().getReservation().reserve(1);
				FutureResponse futureResponse = handshakeRPC.pingUDP(na, cc);
				Utils.addReleaseListener(futureResponse, cc, 1);
				result.put(na, futureResponse);
				if (result.size() >= max)
				{
					if (!waitFor())
					{
						cleanUp();
						return;
					}
				}
				if (Thread.interrupted())
				{
					cleanUp();
					return;
				}
			}
			if (!waitFor())
			{
				cleanUp();
				return;
			}
		}

		private void cleanUp()
		{
			for (FutureResponse futureResponse : result.values())
				futureResponse.cancel();
		}

		private boolean waitFor()
		{
			try
			{
				for (Map.Entry<PeerAddress, FutureResponse> entry : result.entrySet())
				{
					entry.getValue().await();
					if (logger.isDebugEnabled())
					{
						logger.debug("Maintenance: peer " + entry.getKey() + " online=" + entry.getValue());
					}
				}
				result.clear();
				// don't stress
				Thread.sleep(5000);
			}
			catch (InterruptedException e)
			{
				return false;
			}
			return true;
		}
	}

	public ConnectionConfiguration getConnectionConfiguration()
	{
		return connectionConfiguration;
	}

	public P2PConfiguration getP2PConfiguration()
	{
		return peerConfiguration;
	}
}
