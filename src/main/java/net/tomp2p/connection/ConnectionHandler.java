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
package net.tomp2p.connection;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.tomp2p.message.TomP2PDecoderTCP;
import net.tomp2p.message.TomP2PDecoderUDP;
import net.tomp2p.message.TomP2PEncoderStage1;
import net.tomp2p.message.TomP2PEncoderStage2;
import net.tomp2p.p2p.PeerListener;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.upnp.InternetGatewayDevice;
import net.tomp2p.upnp.UPNPResponseException;
import net.tomp2p.utils.GlobalTrafficShapingHandler;

import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the server connections of a node, i.e. the connections a node is
 * listening on.
 * 
 * @author Thomas Bocek
 * 
 */
public class ConnectionHandler
{
	final private static Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);
	// Stores the node information about this node
	final private ConnectionBean connectionBean;
	final private PeerBean peerBean;
	final public static int UDP_LIMIT = 1400;
	// Used to calculate the throughput
	final private static PerformanceFilter performanceFilter = new PerformanceFilter();
	final private List<ConnectionHandler> childConnections = new ArrayList<ConnectionHandler>();
	final private Map<InternetGatewayDevice, InetSocketAddress> internetGatewayDevicesUDP = new HashMap<InternetGatewayDevice, InetSocketAddress>();
	final private Map<InternetGatewayDevice, InetSocketAddress> internetGatewayDevicesTCP = new HashMap<InternetGatewayDevice, InetSocketAddress>();
	final private TCPChannelCache channelChache;
	final private Timer timer;
	final private boolean master;
	final public static String THREAD_NAME = "Netty thread (non-blocking)/ ";
	final private static TomP2PEncoderStage1 encoder1 = new TomP2PEncoderStage1();
	final private static TomP2PEncoderStage2 encoder2 = new TomP2PEncoderStage2();
	static
	{
		ThreadRenamingRunnable.setThreadNameDeterminer(new ThreadNameDeterminer()
		{
			@Override
			public String determineThreadName(String currentThreadName, String proposedThreadName) throws Exception
			{
				return THREAD_NAME + currentThreadName;
			}
		});
	}
	final private ExecutionHandler executionHandlerSend;
	final private ExecutionHandler executionHandlerRcv;
	//
	final private ChannelFactory udpChannelFactory;
	final private ChannelFactory tcpServerChannelFactory;
	final private ChannelFactory tcpClientChannelFactory;
	//
	final private MessageLogger messageLoggerFilter;
	final private ConnectionConfiguration configuration;
	final private GlobalTrafficShapingHandler globalTrafficShapingHandler;

	public ConnectionHandler(int udpPort, int tcpPort, Number160 id, Bindings bindings, int p2pID,
			ConnectionConfiguration configuration, File messageLogger, KeyPair keyPair, PeerMap peerMap,
			List<PeerListener> listeners) throws Exception
	{
		this.configuration = configuration;
		this.timer = new HashedWheelTimer();
		ThreadPoolExecutor t1 = new ThreadPoolExecutor(5, configuration.getMaxIncomingThreads(), 60L, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(configuration.getMaxIncomingThreads(), true));
		t1.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		// for sending, this should never be blocking!
		Executor t2 = Executors.newCachedThreadPool();
		executionHandlerRcv = new ExecutionHandler(t1);
		executionHandlerSend = new ExecutionHandler(t2);
		//
		udpChannelFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
		tcpServerChannelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		tcpClientChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		globalTrafficShapingHandler = new GlobalTrafficShapingHandler(Executors.newCachedThreadPool(),
				configuration.getWriteLimit(), configuration.getReadLimit(), 500);
		//
		String status = bindings.discoverLocalInterfaces();
		logger.info("Status of interface search: " + status);
		InetAddress outsideAddress = bindings.getOutsideAddress();
		PeerAddress self;
		if (outsideAddress != null)
		{
			self = new PeerAddress(id, outsideAddress, bindings.getOutsideTCPPort(), bindings.getOutsideUDPPort(),
					true, true, true);
		}
		else
		{
			if (bindings.getAddresses().size() == 0)
				throw new IOException("Not listening to anything. Maybe your binding information is wrong.");
			outsideAddress = bindings.getAddresses().get(0);
			self = new PeerAddress(id, outsideAddress, tcpPort, udpPort);
		}
		peerBean = new PeerBean(keyPair);
		peerBean.setServerPeerAddress(self);
		peerBean.setPeerMap(peerMap);
		logger.info("Visible address to other peers: " + self);
		ChannelGroup channelGroup = new DefaultChannelGroup("TomP2P ConnectionHandler");
		ConnectionCollector connectionPool = new ConnectionCollector(tcpClientChannelFactory, udpChannelFactory,
				configuration, executionHandlerSend, globalTrafficShapingHandler);
		// Dispatcher setup start
		this.channelChache = new TCPChannelCache(connectionPool, timer, channelGroup);
		DispatcherRequest dispatcherRequest = new DispatcherRequest(p2pID, peerBean, configuration.getIdleUDPMillis(),
				configuration.getTimeoutTCPMillis(), channelGroup, peerMap, listeners, channelChache);
		this.channelChache.setDispatcherRequest(dispatcherRequest);
		// Dispatcher setup stop
		messageLoggerFilter = messageLogger == null ? null : new MessageLogger(messageLogger);
		Sender sender = new Sender(connectionPool, configuration, channelChache, timer);
		connectionBean = new ConnectionBean(p2pID, dispatcherRequest, sender, channelGroup);
		if (bindings.isListenBroadcast())
		{
			logger.info("Listening for broadcasts on port udp: " + udpPort + " and tcp:" + tcpPort);
			if (!startupTCP(new InetSocketAddress(tcpPort), dispatcherRequest, configuration.getMaxMessageSize())
					|| !startupUDP(new InetSocketAddress(udpPort), dispatcherRequest))
				throw new IOException("cannot bind TCP or UDP");
		}
		else
		{
			for (InetAddress addr : bindings.getAddresses())
			{
				logger.info("Listening on address: " + addr + " on port udp: " + udpPort + " and tcp:" + tcpPort);
				if (!startupTCP(new InetSocketAddress(addr, tcpPort), dispatcherRequest,
						configuration.getMaxMessageSize())
						|| !startupUDP(new InetSocketAddress(addr, udpPort), dispatcherRequest))
					throw new IOException("cannot bind TCP or UDP");
			}
		}
		master = true;
	}

	/**
	 * Attaches a peer to an existing connection and use existing information
	 * 
	 * @param parent
	 * @param id
	 */
	public ConnectionHandler(ConnectionHandler parent, Number160 id, KeyPair keyPair, PeerMap peerMap)
	{
		parent.childConnections.add(this);
		this.connectionBean = parent.connectionBean;
		PeerAddress self = new PeerAddress(id, parent.getPeerBean().getServerPeerAddress());
		this.peerBean = new PeerBean(keyPair);
		this.peerBean.setServerPeerAddress(self);
		this.peerBean.setPeerMap(peerMap);
		this.executionHandlerSend = parent.executionHandlerSend;
		this.executionHandlerRcv = parent.executionHandlerRcv;
		this.messageLoggerFilter = parent.messageLoggerFilter;
		this.udpChannelFactory = parent.udpChannelFactory;
		this.tcpServerChannelFactory = parent.tcpServerChannelFactory;
		this.tcpClientChannelFactory = parent.tcpClientChannelFactory;
		this.channelChache = parent.channelChache;
		this.configuration = parent.configuration;
		this.timer = parent.timer;
		this.globalTrafficShapingHandler = parent.globalTrafficShapingHandler;
		this.master = false;
	}

	public ConnectionBean getConnectionBean()
	{
		return connectionBean;
	}

	public boolean startupUDP(InetSocketAddress listenAddressesUDP, final DispatcherRequest dispatcher)
			throws Exception
	{
		ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(udpChannelFactory);
		bootstrap.setPipelineFactory(new ChannelPipelineFactory()
		{
			@Override
			public ChannelPipeline getPipeline() throws Exception
			{
				ChannelPipeline p = Channels.pipeline();
				p.addLast("encoder2", encoder2);
				if (messageLoggerFilter != null)
					p.addLast("loggerDownstream", messageLoggerFilter.getChannelDownstreamHandler());
				p.addLast("encoder1", encoder1);
				p.addLast("decoder", new TomP2PDecoderUDP());
				if (messageLoggerFilter != null)
					p.addLast("loggerUpstream", messageLoggerFilter.getChannelUpstreamHandler());
				p.addLast("performance", performanceFilter);
				p.addLast("executor", executionHandlerRcv);
				p.addLast("handler", dispatcher);
				return p;
			}
		});
		bootstrap.setOption("broadcast", "false");
		Channel channel = bootstrap.bind(listenAddressesUDP);
		logger.info("Listening on UDP socket: " + listenAddressesUDP);
		connectionBean.getChannelGroup().add(channel);
		return channel.isBound();
	}

	/**
	 * Creates TCP channels and listens on them
	 * 
	 * @param listenAddressesTCP
	 *            the addresses which we will listen on
	 * @throws Exception
	 */
	public boolean startupTCP(InetSocketAddress listenAddressesTCP, DispatcherRequest dispatcher, int maxMessageSize)
			throws Exception
	{
		ServerBootstrap bootstrap = new ServerBootstrap(tcpServerChannelFactory);
		setupBootstrapTCP(bootstrap, dispatcher, maxMessageSize);
		Channel channel = bootstrap.bind(listenAddressesTCP);
		connectionBean.getChannelGroup().add(channel);
		logger.info("Listening on TCP socket: " + listenAddressesTCP);
		return channel.isBound();
	}

	private void setupBootstrapTCP(Bootstrap bootstrap, final DispatcherRequest dispatcher, final int maxMessageSize)
	{
		bootstrap.setPipelineFactory(new ChannelPipelineFactory()
		{
			@Override
			public ChannelPipeline getPipeline() throws Exception
			{
				ChannelPipeline p = Channels.pipeline();
				IdleStateHandler timeoutHandler = new IdleStateHandler(timer, configuration.getIdleTCPMillis(),
						TimeUnit.MILLISECONDS);
				p.addLast("timeout", timeoutHandler);
				p.addLast("encoder2", encoder2);
				if (messageLoggerFilter != null)
					p.addLast("loggerDownstream", messageLoggerFilter.getChannelDownstreamHandler());
				p.addLast("encoder1", encoder1);
				p.addLast("decoder", new TomP2PDecoderTCP(maxMessageSize));
				if (messageLoggerFilter != null)
					p.addLast("loggerUpstream", messageLoggerFilter.getChannelUpstreamHandler());
				p.addLast("performance", performanceFilter);
				if (globalTrafficShapingHandler.hasLimit()) p.addLast("trafficShaping", globalTrafficShapingHandler);
				p.addLast("executor", executionHandlerRcv);
				DispatcherReply dispatcherReply = new DispatcherReply(timer, configuration.getIdleTCPMillis(),
						dispatcher, getConnectionBean().getChannelGroup());
				p.addLast("reply", dispatcherReply);
				return p;
			}
		});
		bootstrap.setOption("broadcast", "false");
	}

	/**
	 * Returns the node information bean of this node
	 * 
	 * @return
	 */
	public PeerBean getPeerBean()
	{
		return peerBean;
	}

	public void customLoggerMessage(String customMessage)
	{
		if (messageLoggerFilter != null) messageLoggerFilter.customMessage(customMessage);
		else logger.error("cannot write to log, as no file was provided");
	}

	/**
	 * Shuts down the dispatcher and frees resources<br />
	 * Note: does not close any channels, use {@link shutdownSharedTCP} or
	 * {@link shutdownSharedUDP} instead.
	 * 
	 * @throws InterruptedException
	 */
	public void shutdown()
	{
		if (master) logger.debug("shutdown in progress...");
		// deregister in dispatcher
		connectionBean.getDispatcherRequest().removeIoHandler(getPeerBean().getServerPeerAddress().getID());
		// shutdown all children
		for (ConnectionHandler handler : childConnections)
			handler.shutdown();
		if (master)
		{
			unmapUPNP();
			timer.stop();
			// channelChache.shutdown();
			if (messageLoggerFilter != null) messageLoggerFilter.close();
			// close server first, then all connected clients. This is only done
			// by the master, other groups are
			// empty
			connectionBean.getChannelGroup().close().awaitUninterruptibly();
			// close client
			connectionBean.getSender().shutdown();
			// release resources
			executionHandlerSend.releaseExternalResources();
			executionHandlerRcv.releaseExternalResources();
			udpChannelFactory.releaseExternalResources();
			tcpServerChannelFactory.releaseExternalResources();
			tcpClientChannelFactory.releaseExternalResources();
			globalTrafficShapingHandler.releaseExternalResources();
			logger.debug("shutdown complete");
		}
	}

	public void unmapUPNP()
	{
		for (Map.Entry<InternetGatewayDevice, InetSocketAddress> entry : internetGatewayDevicesTCP.entrySet())
		{
			try
			{
				entry.getKey().deletePortMapping(entry.getValue().getHostName(), entry.getValue().getPort(), "TCP");
			}
			catch (IOException e)
			{
				logger.warn("not removed TCP mapping " + entry.toString() + e);
			}
			catch (UPNPResponseException e)
			{
				logger.warn("not removed TCP mapping " + entry.toString() + e);
			}
			logger.info("removed TCP mapping " + entry.toString());
		}
		for (Map.Entry<InternetGatewayDevice, InetSocketAddress> entry : internetGatewayDevicesUDP.entrySet())
		{
			try
			{
				entry.getKey().deletePortMapping(entry.getValue().getHostName(), entry.getValue().getPort(), "UDP");
			}
			catch (IOException e)
			{
				logger.warn("not removed UDP mapping " + entry.toString() + e);
			}
			catch (UPNPResponseException e)
			{
				logger.warn("not removed UDP mapping " + entry.toString() + e);
			}
			logger.info("removed UDP mapping " + entry.toString());
		}
	}
	/**
	 * Adds a port forearding policy to UPNP enabled devices. Internal is the peer, while external is the router
	 * @param internalAddress
	 * @param internalPortUDP
	 * @param internalPortTCP
	 * @param externalAddress
	 * @param externalPortUDP
	 * @param externalPortTCP
	 * @throws IOException
	 * @throws UPNPResponseException
	 */
	public void mapUPNP(InetAddress internalAddress, int internalPortUDP, int internalPortTCP, InetAddress externalAddress, int externalPortUDP, int externalPortTCP) throws IOException,
			UPNPResponseException
	{
		// -1 sets the default timeout to 1500 ms
		Collection<InternetGatewayDevice> IGDs = InternetGatewayDevice.getDevices(-1);
		if (IGDs == null) return;
		for (InternetGatewayDevice igd : IGDs)
		{
			logger.info("Found device " + igd);
			if(externalPortUDP!=-1) 
			{
				boolean mappedUDP = igd.addPortMapping("TomP2P mapping UDP", "UDP", externalAddress.getHostName(), externalPortUDP,
					internalAddress.getHostAddress(), internalPortUDP, 0);
				if (mappedUDP) addMappingUDP(igd, externalAddress, externalPortUDP);
			}
			boolean mappedTCP = igd.addPortMapping("TomP2P mapping TCP", "TCP", externalAddress.getHostName(), externalPortTCP,
					internalAddress.getHostAddress(), internalPortTCP, 0);
			if (mappedTCP) addMappingTCP(igd, externalAddress, externalPortTCP);
		}
	}

	private void addMappingTCP(InternetGatewayDevice igd, InetAddress externalAddress, int externalPortTCP)
	{
		internetGatewayDevicesTCP.put(igd, new InetSocketAddress(externalAddress, externalPortTCP));
	}

	private void addMappingUDP(InternetGatewayDevice igd, InetAddress externalAddress, int externalPortUDP)
	{
		internetGatewayDevicesUDP.put(igd, new InetSocketAddress(externalAddress, externalPortUDP));
	}

	public boolean isListening()
	{
		return !getConnectionBean().getChannelGroup().isEmpty();
	}
}
