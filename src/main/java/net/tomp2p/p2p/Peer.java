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
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionHandler;
import net.tomp2p.connection.DiscoverNetworks;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Cancellable;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureCleanup;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureData;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.futures.FutureWrappedBootstrap;
import net.tomp2p.natpmp.NatPmpException;
import net.tomp2p.p2p.DistributedHashMap.Operation;
import net.tomp2p.p2p.config.ConfigurationBaseDHT;
import net.tomp2p.p2p.config.ConfigurationBootstrap;
import net.tomp2p.p2p.config.ConfigurationDirect;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.ConfigurationRemove;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.ConfigurationTrackerGet;
import net.tomp2p.p2p.config.ConfigurationTrackerStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapKadImpl;
import net.tomp2p.replication.DefaultStorageReplication;
import net.tomp2p.replication.Replication;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.HandshakeRPC;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.PeerExchangeRPC;
import net.tomp2p.rpc.QuitRPC;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.rpc.RequestHandlerUDP;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.rpc.TaskRPC;
import net.tomp2p.rpc.TrackerRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.task.AsyncTask;
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
	// As soon as the user calls listen, this connection handler is set
	private ConnectionHandler connectionHandler;
	// the id of this node
	final private Number160 peerId;
	// the p2p network identifier, two different networks can have the same
	// ports
	private final int p2pID;
	private final KeyPair keyPair;
	// Distributed 
	private DistributedHashMap dht;
	private DistributedTracker tracker;
	private DistributedRouting routing;
	private AsyncTask asyncTask;
	// RPC
	private HandshakeRPC handshakeRCP;
	private StorageRPC storageRPC;
	private NeighborRPC neighborRPC;
	private QuitRPC quitRCP;
	private PeerExchangeRPC peerExchangeRPC;
	private DirectDataRPC directDataRPC;
	private TrackerRPC trackerRPC;
	private TaskRPC taskRPC;
	//
	private Bindings bindings;
	//
	final private ConnectionConfiguration configuration;
	// for maintenannce
	private ScheduledExecutorService scheduledExecutorServiceMaintenance;
	private ScheduledExecutorService scheduledExecutorServiceReplication;
	final private Map<BaseFuture, Long> pendingFutures = Collections
			.synchronizedMap(new CacheMap<BaseFuture, Long>(1000));
	private boolean masterFlag = true;
	private List<ScheduledFuture<?>> scheduledFutures = Collections
			.synchronizedList(new ArrayList<ScheduledFuture<?>>());
	final private List<PeerListener> listeners = new ArrayList<PeerListener>();
	private Timer timer;
	final public static int BLOOMFILTER_SIZE = 1024;
	//
	final private int maintenanceThreads;
	final private int replicationThreads;
	final private int replicationRefreshMillis;
	
	final private PeerMapKadImpl peerMap;


	Peer(final int p2pID, final Number160 nodeId, final KeyPair keyPair,
			int maintenanceThreads, int replicationThreads, int replicationRefreshMillis,
			ConnectionConfiguration configuration, PeerMapKadImpl peerMap)
	{
		this.p2pID = p2pID;
		this.peerId = nodeId;
		this.configuration = configuration;
		this.keyPair = keyPair;
		this.maintenanceThreads = maintenanceThreads;
		this.replicationThreads = replicationThreads;
		this.replicationRefreshMillis = replicationRefreshMillis;
		this.peerMap = peerMap;
	}

	/**
	 * Adds a listener to peer events. The events being triggered are: startup,
	 * shutdown, change of peer address. The change of the peer address is due
	 * to the discovery process. Since this process runs in an other thread,
	 * this method is thread safe.
	 * 
	 * @param listener The listener
	 */
	public void addPeerListener(PeerListener listener)
	{
		if (isRunning())
		{
			listener.notifyOnStart();
		}
		synchronized (listeners)
		{
			listeners.add(listener);
		}	
	}

	/**
	 * Removes a peer listener. This method is thread safe.
	 * 
	 * @param listener The listener
	 */
	public void removePeerListener(PeerListener listener)
	{
		synchronized (listeners)
		{
			listeners.remove(listener);
		}
	}
	
	public  List<PeerListener> getListeners()
	{
		return listeners;
	}

	/**
	 * Closes all connections of this node
	 * 
	 * @throws InterruptedException
	 */
	public void shutdown()
	{
		logger.info("begin shutdown in progres at "+System.nanoTime());
		synchronized (scheduledFutures)
		{
			for (ScheduledFuture<?> scheduledFuture : scheduledFutures)
				scheduledFuture.cancel(true);
		}
		//don't send any new requests
		if(masterFlag)
		{
			getConnectionBean().getSender().shutdown();
		}
		if (masterFlag && timer != null)
			timer.stop();
		if (masterFlag && scheduledExecutorServiceMaintenance != null)
			scheduledExecutorServiceMaintenance.shutdownNow();
		if (masterFlag && scheduledExecutorServiceReplication != null)
			scheduledExecutorServiceReplication.shutdownNow();
		getConnectionHandler().shutdown();
		//listeners may be called from other threads
		synchronized (listeners)
		{
			for (PeerListener listener : listeners)
				listener.notifyOnShutdown();
		}
		getPeerBean().getStorage().close();
		connectionHandler = null;
	}

	/**
	 * Lets this node listen on a port
	 * 
	 * @param udpPort the UDP port to listen on
	 * @param tcpPort the TCP port to listen on
	 * @param bindInformation contains IP addresses to listen on
	 * @param replication
	 * @param statServer
	 * @throws Exception
	 */
	ConnectionHandler listen(final int udpPort, final int tcpPort, final Bindings bindings,
			final File fileMessageLogger) throws IOException
	{
		// I'm the master
		masterFlag = true;
		this.timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS, 10);
		this.bindings = bindings;
		this.scheduledExecutorServiceMaintenance = Executors
				.newScheduledThreadPool(maintenanceThreads);
		this.scheduledExecutorServiceReplication = Executors
				.newScheduledThreadPool(replicationThreads);
		ConnectionHandler connectionHandler = new ConnectionHandler(udpPort, tcpPort, peerId, bindings, getP2PID(),
				configuration, fileMessageLogger, keyPair, peerMap, timer);
		logger.debug("listen done");
		this.connectionHandler = connectionHandler;
		return connectionHandler;
	}

	ConnectionHandler listen(final Peer master) throws IOException
	{
		// I'm a slave
		masterFlag = false;
		this.timer = master.timer;
		this.bindings = master.bindings;
		this.scheduledExecutorServiceMaintenance = master.scheduledExecutorServiceMaintenance;
		this.scheduledExecutorServiceReplication = master.scheduledExecutorServiceReplication;
		//listen to the masters peermap
		ConnectionHandler connectionHandler = new ConnectionHandler(master.getConnectionHandler(), peerId, keyPair, peerMap);
		logger.debug("listen done");
		this.connectionHandler = connectionHandler;
		return connectionHandler;
	}

	public void setDefaultStorageReplication()
	{
		Replication replicationStorage = getPeerBean().getReplicationStorage();
		DefaultStorageReplication defaultStorageReplication = new DefaultStorageReplication(this,
				getPeerBean().getStorage(), storageRPC, pendingFutures, configuration.isForceStorageUDP());
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
		connectionHandler.getConnectionBean().getScheduler().startMaintainance(
				getPeerBean().getPeerMap(), getHandshakeRPC(), getConnectionBean().getConnectionReservation(), 5);
	}

	public void customLoggerMessage(String customMessage)
	{
		getConnectionHandler().customLoggerMessage(customMessage);
	}

	
	public HandshakeRPC getHandshakeRPC()
	{
		if (handshakeRCP == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return handshakeRCP;
	}
	
	public void setHandshakeRPC(HandshakeRPC handshakeRPC)
	{
		this.handshakeRCP = handshakeRPC;
	}

	public StorageRPC getStoreRPC()
	{
		if (storageRPC == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return storageRPC;
	}
	
	public void setStorageRPC(StorageRPC storageRPC)
	{
		this.storageRPC = storageRPC;
	}
	
	public NeighborRPC getNeighborRPC()
	{
		if (neighborRPC == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return neighborRPC;
	}
	
	public void setNeighborRPC(NeighborRPC neighborRPC)
	{
		this.neighborRPC = neighborRPC;
	}

	public QuitRPC getQuitRPC()
	{
		if (quitRCP == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return quitRCP;
	}
	
	public void setQuitRPC(QuitRPC quitRCP)
	{
		this.quitRCP = quitRCP;
	}

	public PeerExchangeRPC getPeerExchangeRPC()
	{
		if (peerExchangeRPC == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return peerExchangeRPC;
	}
	
	public void setPeerExchangeRPC(PeerExchangeRPC peerExchangeRPC)
	{
		this.peerExchangeRPC = peerExchangeRPC;
	}

	public DirectDataRPC getDirectDataRPC()
	{
		if (directDataRPC == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return directDataRPC;
	}
	
	public void setDirectDataRPC(DirectDataRPC directDataRPC)
	{
		this.directDataRPC = directDataRPC;
	}

	public TrackerRPC getTrackerRPC()
	{
		if (trackerRPC == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return trackerRPC;
	}
	
	public void setTrackerRPC(TrackerRPC trackerRPC)
	{
		this.trackerRPC = trackerRPC;
	}
	
	public TaskRPC getTaskRPC()
	{
		if(taskRPC == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return taskRPC;
	}
	
	public void setTaskRPC(TaskRPC taskRPC)
	{
		this.taskRPC = taskRPC;
	}

	public DistributedRouting getRouting()
	{
		if (routing == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return routing;
	}
	
	public void setDistributedRouting(DistributedRouting routing)
	{
		this.routing = routing;
	}
	
	public DistributedHashMap getDHT()
	{
		if (dht == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return dht;
	}
	
	public void setDistributedHashMap(DistributedHashMap dht)
	{
		this.dht = dht;
	}
	
	public DistributedTracker getTracker()
	{
		if (tracker == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return tracker;
	}
	
	public void setDistributedTracker(DistributedTracker tracker)
	{
		this.tracker = tracker;
	}
	
	public AsyncTask getAsyncTask()
	{
		if(asyncTask == null)
		{
			throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
		}
		return asyncTask;
	}
	
	public void setAsyncTask(AsyncTask asyncTask)
	{
		this.asyncTask = asyncTask;
	}

	public ScheduledFuture<?> addIndirectReplicaiton(Runnable runnable)
	{
		return scheduledExecutorServiceReplication.scheduleWithFixedDelay(runnable,
				replicationRefreshMillis, replicationRefreshMillis, TimeUnit.MILLISECONDS);
	}

	public ConnectionHandler getConnectionHandler()
	{
		if (connectionHandler == null)
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		else
			return connectionHandler;
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
	
	public int getP2PID()
	{
		return p2pID;
	}

	public PeerAddress getPeerAddress()
	{
		return getPeerBean().getServerPeerAddress();
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
	 * Opens a TCP connection and keeps it open. The user can provide the idle
	 * timeout, which means that the connection gets closed after that time of
	 * inactivity. If the other peer goes offline or closes the connection (due
	 * to inactivity), further requests with this connections reopens the
	 * connection. This methods blocks until a connection can be reserver.
	 * 
	 * @param destination The end-point to connect to
	 * @param idleSeconds time in seconds after a connection gets closed if
	 *        idle, -1 if it should remain always open until the user closes the
	 *        connection manually.
	 * @return A class that needs to be passed to those methods that should use
	 *         the already open connection. If the connection could not be reserved, 
	 *         maybe due to a shutdown, null is returned.
	 */
	public PeerConnection createPeerConnection(PeerAddress destination, int idleTCPMillis)
	{
		final FutureChannelCreator fcc = getConnectionBean().getConnectionReservation().reserve(1, true, "PeerConnection");
		fcc.awaitUninterruptibly();
		if(fcc.isFailed())
		{
			return null;
		}
		final ChannelCreator cc = fcc.getChannelCreator();
		final PeerConnection peerConnection = new PeerConnection(destination, getConnectionBean().getConnectionReservation(), cc, idleTCPMillis);
		return peerConnection;
	}

	// custom message
	public FutureData send(final PeerAddress remotePeer, final ChannelBuffer requestBuffer)
	{
		return send(remotePeer, requestBuffer, true);
	}

	public FutureData send(final PeerConnection connection, final ChannelBuffer requestBuffer)
	{
		return send(connection, requestBuffer, true);
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
		byte[] me = Utils.encodeJavaObject(object);
		return send(connection, ChannelBuffers.wrappedBuffer(me), false);
	}

	private FutureData send(final PeerConnection connection, final ChannelBuffer requestBuffer,
			boolean raw)
	{
		RequestHandlerTCP<FutureData> request = getDirectDataRPC().prepareSend(connection.getDestination(),
				requestBuffer.slice(), raw);
		request.setKeepAlive(true);
		// since we keep one connection open, we need to make sure that we do
		// not send anything in parallel.
		try
		{
			connection.aquireSingleConnection();
		}
		catch (InterruptedException e)
		{
			request.getFutureResponse().setFailed("Interupted " + e);
		}
		request.sendTCP(connection.getChannelCreator(), connection.getIdleTCPMillis());
		request.getFutureResponse().addListener(new BaseFutureAdapter<FutureResponse>()
		{
			@Override
			public void operationComplete(FutureResponse future) throws Exception
			{
				connection.releaseSingleConnection();
			}
		});
		return request.getFutureResponse();
	}

	private FutureData send(final PeerAddress remotePeer, final ChannelBuffer requestBuffer,
			boolean raw)
	{
		final RequestHandlerTCP<FutureData> request = getDirectDataRPC().prepareSend(remotePeer, requestBuffer.slice(),
				raw);
		getConnectionBean().getConnectionReservation().reserve(1).addListener(new BaseFutureAdapter<FutureChannelCreator>()
		{
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception
			{
				if(future.isSuccess())
				{
					FutureData futureData = request.sendTCP(future.getChannelCreator());
					Utils.addReleaseListenerAll(futureData, getConnectionBean().getConnectionReservation(), future.getChannelCreator());
				}
				else
				{
					request.getFutureResponse().setFailed(future);
				}
			}
		});
		return request.getFutureResponse();
	}

	// Boostrap and ping

	public FutureBootstrap bootstrapBroadcast()
	{
		return bootstrapBroadcast(Bindings.DEFAULT_PORT);
	}

	public FutureBootstrap bootstrapBroadcast(int port)
	{
		final FutureWrappedBootstrap<FutureBootstrap> result = new FutureWrappedBootstrap<FutureBootstrap>();
		// limit after
		final FutureLateJoin<FutureResponse> tmp = pingBroadcast(port);
		tmp.addListener(new BaseFutureAdapter<FutureLateJoin<FutureResponse>>()
		{
			@Override
			public void operationComplete(final FutureLateJoin<FutureResponse> future)
					throws Exception
			{
				if (future.isSuccess())
				{
					FutureResponse futureResponse = future.getLastSuceessFuture();
					if(futureResponse==null)
					{
						result.setFailed("no futures found");
						return;
					}
					final PeerAddress sender = futureResponse.getResponse().getSender();
					Collection<PeerAddress> bootstrapTo = new ArrayList<PeerAddress>(1);
					bootstrapTo.add(sender);
					result.setBootstrapTo(bootstrapTo);
					result.waitFor(bootstrap(sender));
				}
				else
				{
					result.setFailed("could not reach anyone with the broadcast (1)");
				}
			}
		});
		return result;
	}

	FutureLateJoin<FutureResponse> pingBroadcast(final int port)
	{
		final int size = bindings.getBroadcastAddresses().size();
		final FutureLateJoin<FutureResponse> futureLateJoin = new FutureLateJoin<FutureResponse>(size, 1);
		if (size > 0)
		{
			getConnectionBean().getConnectionReservation().reserve(size).addListener(new BaseFutureAdapter<FutureChannelCreator>()
			{
				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception
				{
					if(future.isSuccess())
					{
						FutureResponse validBroadcast = null;
						for (int i = 0; i < size ; i++)
						{
							final InetAddress broadcastAddress = bindings.getBroadcastAddresses().get(i);
							final PeerAddress peerAddress = new PeerAddress(Number160.ZERO, broadcastAddress,
									port, port);
							validBroadcast = getHandshakeRPC().pingBroadcastUDP(peerAddress, future.getChannelCreator());
							Utils.addReleaseListener(validBroadcast, getConnectionBean().getConnectionReservation(), future.getChannelCreator(), 1);
							if(logger.isDebugEnabled())
							{
								logger.debug("ping broadcast to " + broadcastAddress);
							}
							if(!futureLateJoin.add(validBroadcast))
							{
								//the late join future is fininshed if the add returns false
								break;
							}
						}
					}
					else
					{
						futureLateJoin.setFailed(future);
					}				
				}
			});
		}
		else
		{
			futureLateJoin.setFailed("No broadcast address found. Cannot ping nothing");
		}
		return futureLateJoin;
	}
	
	/**
	 * Pings a peer. Default is to use UDP
	 * 
	 * @param address The address of the remote peer.
	 * @return The future response
	 */
	public FutureResponse ping(final InetSocketAddress address)
	{
		return ping(address, true);
	}

	/**
	 * Pings a peer.
	 * 
	 * @param address The address of the remote peer.
	 * @param isUDP Set to true if UDP should be used, false for TCP.
	 * @return The future response
	 */
	public FutureResponse ping(final InetSocketAddress address, boolean isUDP)
	{
		if (isUDP)
		{
			final RequestHandlerUDP<FutureResponse> request = getHandshakeRPC().pingUDP(new PeerAddress(Number160.ZERO, address));
			getConnectionBean().getConnectionReservation().reserve(1).addListener(new BaseFutureAdapter<FutureChannelCreator>()
			{
				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception
				{
					if (future.isSuccess())
					{
						FutureResponse futureResponse = request.sendUDP(future.getChannelCreator());
						Utils.addReleaseListener(futureResponse, getConnectionBean().getConnectionReservation(), future.getChannelCreator(), 1);
					}
					else
					{
						request.getFutureResponse().setFailed(future);
					}
				}
			});
			return request.getFutureResponse();
		}
		else
		{
			final RequestHandlerTCP<FutureResponse> request = getHandshakeRPC().pingTCP(new PeerAddress(Number160.ZERO, address));
			getConnectionBean().getConnectionReservation().reserve(1).addListener(new BaseFutureAdapter<FutureChannelCreator>()
			{
				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception
				{
					if (future.isSuccess())
					{
						FutureResponse futureResponse = request.sendTCP(future.getChannelCreator());
						Utils.addReleaseListener(futureResponse, getConnectionBean().getConnectionReservation(), future.getChannelCreator(), 1);
					}
					else
					{
						request.getFutureResponse().setFailed(future);
					}
				}
			});
			return request.getFutureResponse();
		}
	}

	/**
	 * Boostraps to a known peer. First the ID of the peer is searched. Then
	 * channels are reserved, then #discover(PeerAddress) is called to verify
	 * this Internet connection settings. Then the routing is initiated to the
	 * same peer as used for {@link #discover(PeerAddress)}.
	 * 
	 * @param address InetAddress and the UDP port of the peer to boostrap to
	 * @return The future bootstrap
	 */
	public FutureBootstrap bootstrap(final InetSocketAddress address)
	{
		final FutureWrappedBootstrap<FutureBootstrap> result = new FutureWrappedBootstrap<FutureBootstrap>();
		final FutureResponse tmp = ping(address, true);
		tmp.addListener(new BaseFutureAdapter<FutureResponse>()
		{
			@Override
			public void operationComplete(final FutureResponse future) throws Exception
			{
				if (future.isSuccess())
				{
					final PeerAddress sender = future.getResponse().getSender();
					Collection<PeerAddress> bootstrapTo = new ArrayList<PeerAddress>(1);
					bootstrapTo.add(sender);
					result.setBootstrapTo(bootstrapTo);
					result.waitFor(bootstrap(sender));
				}
				else
				{
					result.setFailed("could not reach anyone with the broadcast (2)");
				}
			}
		});
		return result;
	}

	/**
	 * Boostraps to a known peer. First channels are reserved, then
	 * #discover(PeerAddress) is called to verify this Internet connection
	 * settings. Then the routing is initiated to the same peer as used for
	 * {@link #discover(PeerAddress)}. Please be aware that in order to boostrap
	 * you need to know the peer ID of the remote peer. Passing Number160.ZERO
	 * does *not* work. If the peerID is not known, use
	 * {@link #bootstrap(InetSocketAddress)}.
	 * 
	 * @param peerAddress The peer address to bootstrap to
	 * @return The future bootstrap
	 * @throws IllegalArgumentException If the peer ID is Number160.ZERO,
	 *         because routing would fail.
	 */
	public FutureBootstrap bootstrap(final PeerAddress peerAddress)
	{
		if(peerAddress.getID() == Number160.ZERO)
		{
			if(logger.isWarnEnabled())
			{
				logger.warn("The peer ID is Number160.ZERO, " +
						"which cannot be used to bootstrap. If the ID is unknown, use bootstrap(InetSocketAddress)");
			}
			throw new IllegalArgumentException("The peer ID is Number160.ZERO, " +
					"which cannot be used to bootstrap. If the ID is unknown, use bootstrap(InetSocketAddress)");
		}
		Collection<PeerAddress> bootstrapTo = new ArrayList<PeerAddress>(1);
		bootstrapTo.add(peerAddress);
		return bootstrap(peerAddress, bootstrapTo, Configurations.defaultBootstrapConfiguration());
	}
	
	/**
	 * Boostraps to a known peer. First channels are reserved, then
	 * #discover(PeerAddress) is called to verify this Internet connection
	 * settings using the argument peerAddress. Then the routing is initiated to
	 * the peers specified in bootstrapTo. Please be aware that in order to
	 * boostrap you need to know the peer ID of all peers in the collection
	 * bootstrapTo. Passing Number160.ZERO does *not* work.
	 * 
	 * @param discoveryPeerAddress The peer address to use for discovery
	 * @param bootstrapTo The peers used to bootstrap
	 * @param config The configuration
	 * @return The future bootstrap
	 */
	public FutureBootstrap bootstrap(final PeerAddress discoveryPeerAddress,
			final Collection<PeerAddress> bootstrapTo, final ConfigurationBootstrap config)
	{
		final FutureWrappedBootstrap<FutureRouting> result = new FutureWrappedBootstrap<FutureRouting>();
		result.setBootstrapTo(bootstrapTo);
		int conn = Math.max(config.getRoutingConfiguration().getParallel(), config
				.getRequestP2PConfiguration().getParallel());
		getConnectionBean().getConnectionReservation().reserve(conn).addListener(new BaseFutureAdapter<FutureChannelCreator>()
		{
			@Override
			public void operationComplete(final FutureChannelCreator futureChannelCreator) throws Exception
			{
				if(futureChannelCreator.isSuccess())
				{
					FutureRouting futureBootstrap = routing.bootstrap(bootstrapTo, config
							.getRoutingConfiguration().getMaxNoNewInfo(config.getRequestP2PConfiguration().getMinimumResults()),
							config.getRoutingConfiguration().getMaxFailures(), config.getRoutingConfiguration().getMaxSuccess(),
							config.getRoutingConfiguration().getParallel(), false, 
							config.isForceRoutingOnlyToSelf(), futureChannelCreator.getChannelCreator());
					Utils.addReleaseListenerAll(futureBootstrap, getConnectionBean().getConnectionReservation(), 
							futureChannelCreator.getChannelCreator());
					result.waitFor(futureBootstrap);
				}
				else
				{
					result.setFailed(futureChannelCreator);
				}
			}
		});
		return result;
	}

	/**
	 * The Dynamic and/or Private Ports are those from 49152 through 65535
	 * (http://www.iana.org/assignments/port-numbers)
	 * 
	 * @param internalHost
	 * @param port
	 * @return
	 */
	public boolean setupPortForwanding(String internalHost)
	{
		int portUDP = bindings.getOutsideUDPPort();
		int portTCP = bindings.getOutsideTCPPort();
		boolean success = false;

		try
		{
			success = connectionHandler.getNATUtils().mapUPNP(internalHost, getPeerAddress().portUDP(),
					getPeerAddress().portTCP(), portUDP, portTCP);
			// connectionHandler.getNATUtils().mapPMP(getPeerAddress().portUDP(),
			// getPeerAddress().portTCP(), portUDP, portTCP);
		}
		catch (IOException e)
		// catch (NatPmpException e)
		{
		}

		if (!success)
		{
			logger.warn("cannot find UPNP devices");
			try
			{
				success = connectionHandler.getNATUtils().mapPMP(getPeerAddress().portUDP(),
						getPeerAddress().portTCP(), portUDP, portTCP);
				if (!success)
				{
					logger.warn("cannot find NAT-PMP devices");
				}
			}
			catch (NatPmpException e1)
			{
				logger.warn("cannot find NAT-PMP devices " + e1);
			}
		}
		return success;
	}
	
	/**
	 * Discover attempts to find the external IP address of this peer. This is
	 * done by first trying to set UPNP with port forwarding (gives us the
	 * external address), query UPNP for the external address, and pinging a
	 * well known peer. The fallback is NAT-PMP.
	 * 
	 * @param inetAddress The Internet address of the peer to send the discover request
	 * @param portUDP The UDP port of the peer to send the discover request
	 * @param portTCP The TCP port of the peer to send the discover request
	 * @return The future discover. This future holds also the real ID of the
	 *         peer we send the discover request
	 */
	public FutureDiscover discover(final InetAddress inetAddress, int portUDP, int portTCP)
	{
		PeerAddress peerAddress = new PeerAddress(Number160.ZERO, inetAddress, portTCP, portUDP);
		return discover(peerAddress);
	}

	/**
	 * Discover attempts to find the external IP address of this peer. This is
	 * done by first trying to set UPNP with port forwarding (gives us the
	 * external address), query UPNP for the external address, and pinging a
	 * well known peer. The fallback is NAT-PMP.
	 * 
	 * @param peerAddress The peer address. Since pings are used the peer ID can
	 *        be Number160.ZERO
	 * @return The future discover. This future holds also the real ID of the
	 *         peer we send the discover request
	 */
	public FutureDiscover discover(final PeerAddress peerAddress)
	{
		final FutureDiscover futureDiscover = new FutureDiscover();
		getConnectionBean().getConnectionReservation().reserve(3).addListener(new BaseFutureAdapter<FutureChannelCreator>()
		{
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception
			{
				if(future.isSuccess())
				{
					discover(futureDiscover, peerAddress, future.getChannelCreator());
				}
				else
				{
					futureDiscover.setFailed(future);
				}
			}
		});
		return futureDiscover;
	}
	
	/**
	 * Needs 3 connections. Cleans up ChannelCreator, which means they will be released.
	 * 
	 * @param peerAddress
	 * @param cc
	 * @return
	 */
	private void discover(final FutureDiscover futureDiscover, final PeerAddress peerAddress, final ChannelCreator cc)
	{
		final FutureResponse futureResponseTCP = getHandshakeRPC().pingTCPDiscover(peerAddress, cc);
		Utils.addReleaseListener(futureResponseTCP, getConnectionBean().getConnectionReservation(), cc, 1);
		addPeerListener(new PeerListener()
		{
			private boolean changedUDP = false;
			private boolean changedTCP = false;

			@Override
			public void serverAddressChanged(PeerAddress peerAddress, PeerAddress reporter, boolean tcp)
			{
				if (tcp)
				{
					changedTCP = true;
					futureDiscover.setDiscoveredTCP();
				}
				else
				{
					changedUDP = true;
					futureDiscover.setDiscoveredUDP();
				}
				if (changedTCP && changedUDP)
				{
					futureDiscover.done(peerAddress, reporter);
				}
			}

			@Override
			public void notifyOnStart()
			{}

			@Override
			public void notifyOnShutdown()
			{}
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
						logger.info("I'm seen as " + seenAs + " by peer " + peerAddress+" I see myself as "+getPeerAddress().getInetAddress());
						if (!getPeerAddress().getInetAddress().equals(seenAs.getInetAddress()))
						{
							// check if we have this interface in that we can listen to
							Bindings bindings2 = new Bindings(seenAs.getInetAddress());
							String status = DiscoverNetworks.discoverInterfaces(bindings2);
							logger.info("2nd interface discovery: "+status);
							if (bindings2.getFoundAddresses().size() > 0 &&  bindings2.getFoundAddresses().contains(seenAs.getInetAddress()))
							{
								serverAddress = serverAddress.changeAddress(seenAs.getInetAddress());
								getPeerBean().setServerPeerAddress(serverAddress);
							}
							else
							{
								// now we know our internal IP, where we receive packets
								if (setupPortForwanding(futureResponseTCP.getResponse().getRecipient()
									.getInetAddress().getHostAddress()))
								{
									serverAddress = serverAddress.changePorts(bindings.getOutsideUDPPort(),
											bindings.getOutsideTCPPort());
									serverAddress = serverAddress.changeAddress(seenAs.getInetAddress());
									getPeerBean().setServerPeerAddress(serverAddress);
								}
							}
						}
						// else -> we announce exactly how the other peer sees us
						FutureResponse fr1 = getHandshakeRPC().pingTCPProbe(peerAddress, cc);
						FutureResponse fr2 = getHandshakeRPC().pingUDPProbe(peerAddress, cc);
						Utils.addReleaseListener(fr1, getConnectionBean().getConnectionReservation(), cc, 1);
						Utils.addReleaseListener(fr2, getConnectionBean().getConnectionReservation(), cc, 1);
						// from here we probe, set the timeout here
						futureDiscover.setTimeout(timer, configuration.getDiscoverTimeoutSec());
						return;
					}
					else
					{
						// important to release connection if not needed
						getConnectionBean().getConnectionReservation().release(cc, 2);
						//System.err.println("release 2"+cc);
						futureDiscover.setFailed("Peer " + peerAddress
								+ " did not report our IP address");
						return;
					}
				}
				else
				{
					// important to release connection if not needed
					getConnectionBean().getConnectionReservation().release(cc, 2);
					//System.err.println("release 2"+cc);
					futureDiscover.setFailed("FutureDiscover: We need at least the TCP connection: "+futureResponseTCP.getFailedReason());
					return;
				}
			}
		});
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
		return put(locationKey, data, config, reserve(config));
	}

	public FutureDHT put(final Number160 locationKey, final Data data, ConfigurationStore config,
			final FutureChannelCreator channelCreator)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		final Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
		dataMap.put(config.getContentKey(), data);
		return put(locationKey, dataMap, config, channelCreator);
	}

	public FutureDHT put(final Number160 locationKey, final Map<Number160, Data> dataMap,
			final ConfigurationStore config)
	{
		return put(locationKey, dataMap, config, reserve(config));
	}
	/**
	 * Stores values in the DHT. First the closest n peers are found, then the
	 * values provided in the dataMap are stored on those peers. A future object
	 * will track the progress since this method returns immediately and the
	 * operations are performed in an other thread.
	 * 
	 * @param locationKey The location in the DHT
	 * @param dataMap If multiple keys and values are provided, then this method
	 *        behaves as putAll()
	 * @param config The configuration, which be used to configure the
	 *        protection mode, putIfAbsent, and repetitions
	 * @param channelCreator The future channel creator
	 * @return The future state of this operation.
	 */
	public FutureDHT put(final Number160 locationKey, final Map<Number160, Data> dataMap,
			final ConfigurationStore config, final FutureChannelCreator channelCreator)
	{
		config.setRequestP2PConfiguration(adjustConfiguration(config.getRequestP2PConfiguration(),
				getPeerBean().getPeerMap()));
		final FutureDHT futureDHT = getDHT().put(locationKey, config.getDomain(), dataMap,
				config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
				config.isStoreIfAbsent(), config.isProtectDomain(), config.isSignMessage(), 
				config.isAutomaticCleanup(), config.getFutureCreate(), channelCreator, getConnectionBean().getConnectionReservation());
		if (config.getRefreshSeconds() > 0)
		{
			final ScheduledFuture<?> tmp = schedulePut(locationKey, dataMap, config, futureDHT);
			setupCancel(futureDHT, tmp);
		}
		return futureDHT;
	}

	private ScheduledFuture<?> schedulePut(final Number160 locationKey,
			final Map<Number160, Data> dataMap,
			final ConfigurationStore config, final FutureDHT futureDHT)
	{

		Runnable runner = new Runnable()
		{
			@Override
			public void run()
			{
				final FutureChannelCreator futureChannelCreator = reserve(config);
				FutureDHT futureDHT2 = getDHT().put(locationKey, config.getDomain(), dataMap,
						config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
						config.isStoreIfAbsent(), config.isProtectDomain(), config.isSignMessage(), true,
						config.getFutureCreate(), futureChannelCreator, getConnectionBean().getConnectionReservation());
				futureDHT.repeated(futureDHT2);
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
	
	public FutureDHT add(final Number160 locationKey, final Data data,
			final ConfigurationStore config)
	{
		return add(locationKey, data, config, reserve(config));
	}

	public FutureDHT add(final Number160 locationKey, final Data data,
			final ConfigurationStore config, final FutureChannelCreator channelCreator)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		final Collection<Data> dataCollection = new ArrayList<Data>();
		dataCollection.add(data);
		return add(locationKey, dataCollection, config, channelCreator);
	}
	
	public FutureDHT add(final Number160 locationKey, final Collection<Data> dataCollection,
			final ConfigurationStore config)
	{
		return add(locationKey, dataCollection, config, reserve(config));
	}

	public FutureDHT add(final Number160 locationKey, final Collection<Data> dataCollection,
			final ConfigurationStore config, final FutureChannelCreator channelCreator)
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
		config.setRequestP2PConfiguration(adjustConfiguration(config.getRequestP2PConfiguration(),
				getPeerBean().getPeerMap()));
		final FutureDHT futureDHT = getDHT().add(locationKey, config.getDomain(), dataCollection,
				config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
				config.isProtectDomain(), config.isSignMessage(), config.isAutomaticCleanup(), 
				config.getFutureCreate(), channelCreator, getConnectionBean().getConnectionReservation());
		if (config.getRefreshSeconds() > 0)
		{
			final ScheduledFuture<?> tmp = scheduleAdd(locationKey, dataCollection, config,
					futureDHT);
			setupCancel(futureDHT, tmp);
		}
		return futureDHT;
	}

	private ScheduledFuture<?> scheduleAdd(final Number160 locationKey,
			final Collection<Data> dataCollection,
			final ConfigurationStore config, final FutureDHT futureDHT)
	{
		Runnable runner = new Runnable()
		{
			@Override
			public void run()
			{
				final FutureChannelCreator futureChannelCreator = reserve(config);
				FutureDHT futureDHT2 = getDHT().add(locationKey, config.getDomain(),
						dataCollection, config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
						config.isProtectDomain(), config.isSignMessage(), true, config.getFutureCreate(),
						futureChannelCreator, getConnectionBean().getConnectionReservation());
				futureDHT.repeated(futureDHT2);
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
		return get(locationKey, null, config, reserve(config));
	}
	
	public FutureDHT getAll(final Number160 locationKey, final ConfigurationGet config,
			final FutureChannelCreator channelCreator)
	{
		return get(locationKey, null, config, channelCreator);
	}

	public FutureDHT get(final Number160 locationKey)
	{
		return get(locationKey, Configurations.defaultGetConfiguration());
	}
	
	public FutureDHT get(final Number160 locationKey, final ConfigurationGet config)
	{
		return get(locationKey, config, reserve(config));
	}

	public FutureDHT get(final Number160 locationKey, final ConfigurationGet config,
			final FutureChannelCreator channelCreator)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		Set<Number160> keyCollection = new HashSet<Number160>();
		keyCollection.add(config.getContentKey());
		return get(locationKey, keyCollection, config, channelCreator);
	}
	
	public FutureDHT get(final Number160 locationKey, Set<Number160> keyCollection,
			final ConfigurationGet config)
	{
		return get(locationKey, keyCollection, config, reserve(config));
	}

	public FutureDHT get(final Number160 locationKey, Set<Number160> keyCollection,
			final ConfigurationGet config, final FutureChannelCreator channelCreator)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		final FutureDHT futureDHT = getDHT().get(locationKey, config.getDomain(), keyCollection,
				config.getPublicKey(),
				config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
				config.getEvaluationScheme(),
				config.isSignMessage(), false, config.isAutomaticCleanup(), channelCreator, getConnectionBean().getConnectionReservation());
		return futureDHT;
	}
	
	//----------------- parallel request
	
	public FutureDHT parallelRequests(final Number160 locationKey, final ConfigurationBaseDHT config, 
			final boolean cancleOnFinish, final NavigableSet<PeerAddress> queue, final Operation operation)
	{
		return parallelRequests(locationKey, config, reserve(config), cancleOnFinish, queue, operation);
	}
	
	public FutureDHT parallelRequests(final Number160 locationKey, final ConfigurationBaseDHT config, 
			final FutureChannelCreator channelCreator, final boolean cancleOnFinish, 
			final NavigableSet<PeerAddress> queue, final Operation operation)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		
		final FutureDHT futureDHT = getDHT().parallelRequests(config.getRequestP2PConfiguration(), 
				queue, cancleOnFinish, channelCreator, getConnectionBean().getConnectionReservation(), 
				config.isAutomaticCleanup(), operation);
		return futureDHT;
	}
	
	//----------------- digest
	
	public FutureDHT digestAll(final Number160 locationKey)
	{
		return digest(locationKey, null, Configurations.defaultGetConfiguration());
	}
	
	public FutureDHT digestAll(final Number160 locationKey, final ConfigurationGet config)
	{
		return digestAll(locationKey, config, reserve(config));
	}

	public FutureDHT digestAll(final Number160 locationKey, final ConfigurationGet config, 
			final FutureChannelCreator channelCreator)
	{
		return digest(locationKey, null, config, channelCreator);
	}
	
	public FutureDHT digest(final Number160 locationKey)
	{
		return digest(locationKey, Configurations.defaultGetConfiguration());
	}
	
	public FutureDHT digest(final Number160 locationKey, final ConfigurationGet config)
	{
		return digest(locationKey, config, reserve(config));
	}

	public FutureDHT digest(final Number160 locationKey, final ConfigurationGet config, 
			final FutureChannelCreator channelCreator)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		Set<Number160> keyCollection = new HashSet<Number160>();
		keyCollection.add(config.getContentKey());
		return digest(locationKey, keyCollection, config, channelCreator);
	}
	
	public FutureDHT digest(final Number160 locationKey, Set<Number160> keyCollection,
			final ConfigurationGet config)
	{
		return digest(locationKey, keyCollection, config, reserve(config));
	}
	
	public FutureDHT digest(final Number160 locationKey, Set<Number160> keyCollection,
			final ConfigurationGet config, final FutureChannelCreator channelCreator)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		final FutureDHT futureDHT = getDHT().get(locationKey, config.getDomain(), keyCollection,
				config.getPublicKey(),
				config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
				config.getEvaluationScheme(),
				config.isSignMessage(), true, config.isAutomaticCleanup(), channelCreator, getConnectionBean().getConnectionReservation());
		return futureDHT;
	}

	// REMOVE
	public FutureDHT removeAll(final Number160 locationKey)
	{
		return removeAll(locationKey, Configurations.defaultRemoveConfiguration());
	}
	
	public FutureDHT removeAll(final Number160 locationKey, ConfigurationRemove config)
	{
		return removeAll(locationKey, config, reserve(config));
	}

	public FutureDHT removeAll(final Number160 locationKey, ConfigurationRemove config, 
			final FutureChannelCreator channelCreator)
	{
		return remove(locationKey, null, config, channelCreator);
	}

	public FutureDHT remove(final Number160 locationKey)
	{
		return remove(locationKey, Configurations.defaultRemoveConfiguration());
	}
	
	public FutureDHT remove(final Number160 locationKey, ConfigurationRemove config)
	{
		return remove(locationKey, config, reserve(config));
	}

	public FutureDHT remove(final Number160 locationKey, ConfigurationRemove config, 
			final FutureChannelCreator channelCreator)
	{
		Set<Number160> keyCollection = new HashSet<Number160>();
		keyCollection.add(config.getContentKey());
		return remove(locationKey, keyCollection, config, channelCreator);
	}

	public FutureDHT remove(final Number160 locationKey, final Number160 contentKey)
	{
		Set<Number160> keyCollection = new HashSet<Number160>();
		keyCollection.add(contentKey);
		return remove(locationKey, keyCollection, Configurations.defaultRemoveConfiguration());
	}
	
	public FutureDHT remove(final Number160 locationKey, final Set<Number160> keyCollection,
			final ConfigurationRemove config)
	{
		return remove(locationKey, keyCollection, config, reserve(config));
	}

	public FutureDHT remove(final Number160 locationKey, final Set<Number160> keyCollection,
			final ConfigurationRemove config, final FutureChannelCreator channelCreator)
	{
		if (keyCollection != null)
		{
			for (Number160 contentKey : keyCollection)
				getPeerBean().getStorage().remove(
						locationKey, config.getDomain(), contentKey,
						keyPair.getPublic());
		}
		else
		{
			getPeerBean().getStorage().remove(locationKey, config.getDomain(), Number160.ZERO, Number160.MAX_VALUE,
					keyPair.getPublic());
		}
		config.setRequestP2PConfiguration(adjustConfiguration(config.getRequestP2PConfiguration(),
				getPeerBean().getPeerMap()));
		final FutureDHT futureDHT = getDHT().remove(locationKey, config.getDomain(), keyCollection,
				config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
				config.isReturnResults(),
				config.isSignMessage(), config.isAutomaticCleanup(), config.getFutureCreate(), channelCreator, getConnectionBean().getConnectionReservation());
		if (config.getRefreshSeconds() > 0 && config.getRepetitions() > 0)
		{
			final ScheduledFuture<?> tmp = scheduleRemove(locationKey, keyCollection, config,
					futureDHT);
			setupCancel(futureDHT, tmp);
		}
		return futureDHT;
	}

	private ScheduledFuture<?> scheduleRemove(final Number160 locationKey,
			final Set<Number160> keyCollection,
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
				final FutureChannelCreator futureChannelCreator = reserve(config);
				FutureDHT futureDHT2 = getDHT().remove(locationKey, config.getDomain(),
						keyCollection,
						config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
						config.isReturnResults(), config.isSignMessage(), true, config.getFutureCreate(),
						futureChannelCreator, getConnectionBean().getConnectionReservation());
				futureDHT.repeated(futureDHT2);
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
		final ScheduledFuture<?> tmp = scheduledExecutorServiceReplication.scheduleAtFixedRate(
				myRunnable,
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
	
	public FutureDHT send(final Number160 locationKey, final ChannelBuffer buffer,
			final ConfigurationDirect config)
	{
		return send(locationKey, buffer, config, reserve(config));
	}

	public FutureDHT send(final Number160 locationKey, final ChannelBuffer buffer,
			final ConfigurationDirect config, final FutureChannelCreator channelCreator)
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		config.setRequestP2PConfiguration(adjustConfiguration(config.getRequestP2PConfiguration(),
				getPeerBean().getPeerMap()));
		final FutureDHT futureDHT = getDHT().direct(locationKey, buffer, true,
				config.getRoutingConfiguration(),
				config.getRequestP2PConfiguration(), config.getFutureCreate(),
				config.isCancelOnFinish(), config.isAutomaticCleanup(), channelCreator, getConnectionBean().getConnectionReservation());
		if (config.getRefreshSeconds() > 0 && config.getRepetitions() > 0)
		{
			final ScheduledFuture<?> tmp = scheduleSend(locationKey, buffer, config, futureDHT);
			setupCancel(futureDHT, tmp);
		}
		return futureDHT;
	}

	public FutureDHT send(final Number160 locationKey, final Object object) throws IOException
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		return send(locationKey, object, Configurations.defaultConfigurationDirect());
	}
	
	public FutureDHT send(final Number160 locationKey, final Object object,
			final ConfigurationDirect config)
			throws IOException
	{
		return send(locationKey, object, config, reserve(config));
	}

	public FutureDHT send(final Number160 locationKey, final Object object,
			final ConfigurationDirect config, final FutureChannelCreator channelCreator)
			throws IOException
	{
		if (locationKey == null)
			throw new IllegalArgumentException("null in get not allowed in locationKey");
		byte[] me = Utils.encodeJavaObject(object);
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(me);
		config.setRequestP2PConfiguration(adjustConfiguration(config.getRequestP2PConfiguration(),
				getPeerBean().getPeerMap()));
		final FutureDHT futureDHT = getDHT().direct(locationKey, buffer, false,
				config.getRoutingConfiguration(),
				config.getRequestP2PConfiguration(), config.getFutureCreate(),
				config.isCancelOnFinish(), config.isAutomaticCleanup(), channelCreator, getConnectionBean().getConnectionReservation());
		if (config.getRefreshSeconds() > 0 && config.getRepetitions() > 0)
		{
			final ScheduledFuture<?> tmp = scheduleSend(locationKey, buffer, config, futureDHT);
			setupCancel(futureDHT, tmp);
		}
		return futureDHT;
	}

	private ScheduledFuture<?> scheduleSend(final Number160 locationKey,
			final ChannelBuffer buffer,
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
				final FutureChannelCreator futureChannelCreator = reserve(config);
				config.setRequestP2PConfiguration(adjustConfiguration(
						config.getRequestP2PConfiguration(), getPeerBean().getPeerMap()));
				final FutureDHT futureDHT2 = getDHT().direct(locationKey, buffer, false,
						config.getRoutingConfiguration(), config.getRequestP2PConfiguration(),
						config.getFutureCreate(), config.isCancelOnFinish(), true, 
						futureChannelCreator, getConnectionBean().getConnectionReservation());
				futureDHT.repeated(futureDHT2);
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
		final ScheduledFuture<?> tmp = scheduledExecutorServiceReplication.scheduleAtFixedRate(
				myRunnable,
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
		return getFromTracker(locationKey, config, new SimpleBloomFilter<Number160>(
				BLOOMFILTER_SIZE, 200));
	}

	public FutureTracker getFromTrackerCreateBloomfilter1(Number160 locationKey,
			ConfigurationTrackerGet config,
			Collection<PeerAddress> knownPeers)
	{
		// make a good guess based on the config and the maxium tracker that can
		// be found
		SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(
				BLOOMFILTER_SIZE, 200);
		if (!knownPeers.isEmpty())
		{
			for (PeerAddress peerAddress : knownPeers)
			{
				bloomFilter.add(peerAddress.getID());
			}
		}
		return getFromTracker(locationKey, config, bloomFilter);
	}

	public FutureTracker getFromTrackerCreateBloomfilter2(Number160 locationKey,
			ConfigurationTrackerGet config,
			Collection<Number160> knownPeers)
	{
		SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(
				BLOOMFILTER_SIZE, 200);
		// System.err.println("FP-Rate:"+bloomFilter.expectedFalsePositiveProbability());
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
		int conn = Math.max(config.getRoutingConfiguration().getParallel(), config
				.getTrackerConfiguration().getParallel());
		final FutureChannelCreator futureChannelCreator = getConnectionBean().getConnectionReservation().reserve(conn);

		FutureTracker futureTracker = getTracker().getFromTracker(locationKey, config.getDomain(),
				config.getRoutingConfiguration(), config.getTrackerConfiguration(),
				config.isExpectAttachement(), config.getEvaluationScheme(), config.isSignMessage(),
				config.isUseSecondaryTrackers(), knownPeers, futureChannelCreator,
				getConnectionBean().getConnectionReservation());
		return futureTracker;
	}

	public FutureTracker addToTracker(final Number160 locationKey,
			final ConfigurationTrackerStore config)
	{
		// make a good guess based on the config and the maxium tracker that can
		// be found
		SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(
				BLOOMFILTER_SIZE, 1024);
		// add myself to my local tracker, since we use a mesh we are part of
		// the tracker mesh as well.
		getPeerBean().getTrackerStorage().put(locationKey, config.getDomain(), getPeerAddress(),
				getPeerBean().getKeyPair().getPublic(), config.getAttachement());
		int conn = Math.max(config.getRoutingConfiguration().getParallel(), config
				.getTrackerConfiguration().getParallel());
		final FutureChannelCreator futureChannelCreator = getConnectionBean().getConnectionReservation().reserve(conn);

		final FutureTracker futureTracker = getTracker().addToTracker(locationKey,
				config.getDomain(),
				config.getAttachement(), config.getRoutingConfiguration(),
				config.getTrackerConfiguration(),
				config.isSignMessage(), config.getFutureCreate(), bloomFilter, futureChannelCreator, 
				getConnectionBean().getConnectionReservation());
		if (getPeerBean().getTrackerStorage().getTrackerTimoutSeconds() > 0)
		{
			final ScheduledFuture<?> tmp = scheduleAddTracker(locationKey, config, futureTracker);
			setupCancel(futureTracker, tmp);
		}
		if (config.getWaitBeforeNextSendSeconds() > 0)
		{
			final ScheduledFuture<?> tmp = schedulePeerExchange(locationKey, config, futureTracker);
			setupCancel(futureTracker, tmp);
		}
		return futureTracker;
	}

	private void setupCancel(final FutureCleanup futureTracker, final ScheduledFuture<?> tmp)
	{
		scheduledFutures.add(tmp);
		futureTracker.addCleanup(new Cancellable()
		{
			@Override
			public void cancel()
			{
				tmp.cancel(true);
				scheduledFutures.remove(tmp);
			}
		});
	}

	private ScheduledFuture<?> scheduleAddTracker(final Number160 locationKey,
			final ConfigurationTrackerStore config,
			final FutureTracker futureTracker)
	{
		Runnable runner = new Runnable()
		{
			@Override
			public void run()
			{
				// make a good guess based on the config and the maxium tracker
				// that can be found
				SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(
						BLOOMFILTER_SIZE, 1024);
				int conn = Math.max(config.getRoutingConfiguration().getParallel(), config
						.getTrackerConfiguration().getParallel());
				final FutureChannelCreator futureChannelCreator = getConnectionBean().getConnectionReservation().reserve(conn);

				FutureTracker futureTracker2 = getTracker().addToTracker(locationKey,
						config.getDomain(), config.getAttachement(),
						config.getRoutingConfiguration(), config.getTrackerConfiguration(),
						config.isSignMessage(), config.getFutureCreate(), bloomFilter,
						futureChannelCreator, getConnectionBean().getConnectionReservation());
				futureTracker.repeated(futureTracker2);
			}
		};
		int refresh = getPeerBean().getTrackerStorage().getTrackerTimoutSeconds() * 3 / 4;
		ScheduledFuture<?> tmp = scheduledExecutorServiceReplication.scheduleAtFixedRate(runner,
				refresh, refresh,
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
				final FutureChannelCreator futureChannelCreator = getConnectionBean().getConnectionReservation().reserve(
						TrackerStorage.TRACKER_SIZE);
				FutureLateJoin<FutureResponse> futureLateJoin = getTracker().startPeerExchange(
						locationKey, config.getDomain(), futureChannelCreator, 
						getConnectionBean().getConnectionReservation(), config.getTrackerConfiguration().isForceTCP());
				futureTracker.repeated(futureLateJoin);
			}
		};
		int refresh = config.getWaitBeforeNextSendSeconds();
		ScheduledFuture<?> tmp = scheduledExecutorServiceReplication.scheduleAtFixedRate(runner,
				refresh, refresh,
				TimeUnit.SECONDS);
		scheduledFutures.add(tmp);
		return tmp;
	}

	public ConnectionConfiguration getConfiguration()
	{
		return configuration;
	}

	public static RequestP2PConfiguration adjustConfiguration(
			RequestP2PConfiguration p2pConfiguration, PeerMap peerMap)
	{
		int size = peerMap.size() + 1;
		int requested = p2pConfiguration.getMinimumResults();
		if (size >= requested)
		{
			return p2pConfiguration;
		}
		else
		{
			return new RequestP2PConfiguration(size, p2pConfiguration.getMaxFailure(),
					p2pConfiguration.getParallelDiff());
		}
	}
		
	/**
	 * Reserves a connection for a routing and DHT operation. This call blocks
	 * until connections have been reserved.
	 * 
	 * @param configurationBaseDHT The information about the routing and the DHT
	 *        operation
	 * @param name The name of the ChannelCreator, used for easier debugging
	 * @return A ChannelCreator that can create channel according to
	 *         routingConfiguration and requestP2PConfiguration
	 */
	public FutureChannelCreator reserve(final ConfigurationBaseDHT configurationBaseDHT, String name)
	{
		return reserve(configurationBaseDHT.getRoutingConfiguration(),
				configurationBaseDHT.getRequestP2PConfiguration(), name);
	}
	
	/**
	 * Reserves a connection for a routing and DHT operation. This call blocks
	 * until connections have been reserved.
	 * 
	 * @param configurationBaseDHT The information about the routing and the DHT
	 *        operation
	 * @return A ChannelCreator that can create channel according to
	 *         routingConfiguration and requestP2PConfiguration
	 */
	public FutureChannelCreator reserve(final ConfigurationBaseDHT configurationBaseDHT)
	{
		return reserve(configurationBaseDHT, "default");
	}
	
	/**
	 * Reserves a connection for a routing and DHT operation. This call does not
	 * blocks. At least one of the arguments routingConfiguration or
	 * requestP2PConfiguration must not be null.
	 * 
	 * @param routingConfiguration The information about the routing
	 * @param requestP2PConfiguration The information about the DHT operation
	 * @param name The name of the ChannelCreator, used for easier debugging
	 * @return A ChannelCreator that can create channel according to
	 *         routingConfiguration and requestP2PConfiguration
	 * @throws IllegalArgumentException If both arguments routingConfiguration
	 *         and requestP2PConfiguration are null
	 */
	public FutureChannelCreator reserve(final RoutingConfiguration routingConfiguration,
			RequestP2PConfiguration requestP2PConfiguration, String name)
	{
		if(routingConfiguration == null && requestP2PConfiguration == null)
		{
			throw new IllegalArgumentException("Both routingConfiguration and requestP2PConfiguration cannot be null");
		}
		final int nrConnections;
		if(routingConfiguration == null)
		{
			nrConnections = requestP2PConfiguration.getParallel();
		}
		else if(requestP2PConfiguration == null)
		{
			nrConnections = routingConfiguration.getParallel();
		}
		else
		{
			nrConnections = Math.max(routingConfiguration.getParallel(),
				requestP2PConfiguration.getParallel());
		}
		return getConnectionBean().getConnectionReservation().reserve(nrConnections, name);
	}
	
	/**
	 * Release a ChannelCreator. The permits will be returned so that they can
	 * be used again. This is a wrapper for ConnectionReservation.
	 * 
	 * @param channelCreator The ChannelCreator that is not used anymore
	 */
	public void release(ChannelCreator channelCreator)
	{
		getConnectionBean().getConnectionReservation().release(channelCreator);
	}
	
	/**
	 * Sets a timeout for this future. If the timeout passes, the future fails
	 * with the reason provided
	 * 
	 * @param baseFuture The future to set the timeout
	 * @param millis The time in milliseconds until this future is considered a
	 *        failure.
	 * @param reason The reason why this future failed
	 */
	public void setFutureTimeout(BaseFuture baseFuture, int millis, String reason)
	{
		getConnectionBean().getScheduler().scheduleTimeout(baseFuture, millis, reason);
	}
}
