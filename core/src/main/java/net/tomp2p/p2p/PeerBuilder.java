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
import io.netty.util.concurrent.EventExecutorGroup;

import java.io.IOException;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerCreator;
import net.tomp2p.connection.PingBuilderFactory;
import net.tomp2p.connection.PipelineFilter;
import net.tomp2p.connection.Ports;
import net.tomp2p.p2p.builder.PingBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.rpc.BloomfilterFactory;
import net.tomp2p.rpc.BroadcastRPC;
import net.tomp2p.rpc.DefaultBloomfilterFactory;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.PingRPC;
import net.tomp2p.rpc.QuitRPC;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

/**
 * The builder of a {@link Peer} class.
 * 
 * @author Thomas Bocek
 * 
 */
public class PeerBuilder {
	public static final PublicKey EMPTY_PUBLICKEY = new PublicKey() {
		private static final long serialVersionUID = 4041565007522454573L;

		@Override
		public String getFormat() {
			return null;
		}

		@Override
		public byte[] getEncoded() {
			return null;
		}

		@Override
		public String getAlgorithm() {
			return null;
		}
	};

	private static final KeyPair EMPTY_KEYPAIR = new KeyPair(EMPTY_PUBLICKEY, null);
	// if the permits are chosen too high, then we might run into timeout
	// as we can't handle that many connections within the time limit
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
	private int tcpPortForwarding = -1;
	private int udpPortForwarding = -1;
	private Bindings interfaceBindings = null;
	private Bindings externalBindings = null;
	private PeerMap peerMap = null;
	private Peer masterPeer = null;
	private ChannelServerConfiguration channelServerConfiguration = null;
	private ChannelClientConfiguration channelClientConfiguration = null;
	private Boolean behindFirewall = null;
	private BroadcastHandler broadcastHandler = null;
	private BloomfilterFactory bloomfilterFactory = null;
	private ScheduledExecutorService scheduledExecutorService = null;
	private MaintenanceTask maintenanceTask = null;
	private Random random = null;
	private final List<PeerInit> toInitialize = new ArrayList<PeerInit>(1);

	// enable/disable RPC/P2P/other
	private boolean enableHandshakeRPC = true;
	private boolean enableNeighborRPC = true;
	private boolean enableDirectDataRPC = true;
	private boolean enableBroadcast = true;
	private boolean enableRouting = true;
	private boolean enableMaintenance = true;
	private boolean enableQuitRPC = true;

	/**
	 * Creates a peer builder with the provided peer ID and an empty key pair.
	 * 
	 * @param peerId
	 *            The peer ID
	 */
	public PeerBuilder(final Number160 peerId) {
		this.peerId = peerId;
	}

	/**
	 * Creates a peer builder with the provided key pair and a peer ID that is
	 * generated out of this key pair.
	 * 
	 * @param keyPair
	 *            The public private key pair
	 */
	public PeerBuilder(final KeyPair keyPair) {
		this.peerId = Utils.makeSHAHash(keyPair.getPublic().getEncoded());
		this.keyPair = keyPair;
	}

	/**
	 * Creates a peer and starts to listen for incoming connections.
	 * 
	 * @return The peer that can operate in the P2P network.
	 * @throws IOException .
	 */
	public Peer start() throws IOException {

		if (behindFirewall == null) {
			behindFirewall = false;
		}

		if (channelServerConfiguration == null) {
			channelServerConfiguration = createDefaultChannelServerConfiguration();
			channelServerConfiguration.portsForwarding(new Ports(tcpPortForwarding, udpPortForwarding));
			if (tcpPort == -1) {
				tcpPort = Ports.DEFAULT_PORT;
			}
			if (udpPort == -1) {
				udpPort = Ports.DEFAULT_PORT;
			}
			channelServerConfiguration.ports(new Ports(tcpPort, udpPort));
			channelServerConfiguration.behindFirewall(behindFirewall);
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

		if (interfaceBindings == null) {
			interfaceBindings = new Bindings();
		}
		channelServerConfiguration.bindingsIncoming(interfaceBindings);
		if (externalBindings == null) {
			externalBindings = new Bindings();
		}
		channelClientConfiguration.bindingsOutgoing(externalBindings);

		if (peerMap == null) {
			peerMap = new PeerMap(new PeerMapConfiguration(peerId));
		}

		if (masterPeer == null && scheduledExecutorService == null) {
			scheduledExecutorService = Executors.newScheduledThreadPool(1);
		}

		final PeerCreator peerCreator;
		if (masterPeer != null) {
			peerCreator = new PeerCreator(masterPeer.peerCreator(), peerId, keyPair);
		} else {
			peerCreator = new PeerCreator(p2pID, peerId, keyPair, channelServerConfiguration,
					channelClientConfiguration, scheduledExecutorService);
		}

		final Peer peer = new Peer(p2pID, peerId, peerCreator);

		PeerBean peerBean = peerCreator.peerBean();
		peerBean.addPeerStatusListener(peerMap);

		ConnectionBean connectionBean = peerCreator.connectionBean();

		peerBean.peerMap(peerMap);
		peerBean.keyPair(keyPair);

		if (bloomfilterFactory == null) {
			peerBean.bloomfilterFactory(new DefaultBloomfilterFactory());
		}

		if (broadcastHandler == null) {
			broadcastHandler = new DefaultBroadcastHandler(peer, new Random());
		}

		// set/enable RPC

		if (isEnableHandshakeRPC()) {
			PingRPC pingRPC = new PingRPC(peerBean, connectionBean);
			peer.pingRPC(pingRPC);
		}

		if (isEnableQuitRPC()) {
			QuitRPC quitRPC = new QuitRPC(peerBean, connectionBean);
			quitRPC.addPeerStatusListener(peerMap);
			peer.quitRPC(quitRPC);
		}

		if (isEnableNeighborRPC()) {
			NeighborRPC neighborRPC = new NeighborRPC(peerBean, connectionBean);
			peer.neighborRPC(neighborRPC);
		}

		if (isEnableDirectDataRPC()) {
			DirectDataRPC directDataRPC = new DirectDataRPC(peerBean, connectionBean);
			peer.directDataRPC(directDataRPC);
		}

		if (isEnableBroadcastRpc()) {
			BroadcastRPC broadcastRPC = new BroadcastRPC(peerBean, connectionBean, broadcastHandler);
			peer.broadcastRPC(broadcastRPC);
		}

		if (isEnableRoutingRpc() && isEnableNeighborRPC()) {
			DistributedRouting routing = new DistributedRouting(peerBean, peer.neighborRPC());
			peer.distributedRouting(routing);
		}

		if (maintenanceTask == null && isEnableMaintenanceRpc()) {
			maintenanceTask = new MaintenanceTask();
		}
		if (maintenanceTask != null) {
			maintenanceTask.init(peer, connectionBean.timer());
			maintenanceTask.addMaintainable(peerMap);
		}
		peerBean.maintenanceTask(maintenanceTask);

		// set the ping builder for the heart beat
		connectionBean.sender().pingBuilderFactory(new PingBuilderFactory() {
			@Override
			public PingBuilder create() {
				return peer.ping();
			}
		});

		for (PeerInit peerInit : toInitialize) {
			peerInit.init(peer);
		}
		return peer;
	}

	public static ChannelServerConfiguration createDefaultChannelServerConfiguration() {
		return new ChannelServerConfiguration()
				.bindingsIncoming(new Bindings())
				.behindFirewall(false)
				.pipelineFilter(new DefaultPipelineFilter())
				.signatureFactory(new DSASignatureFactory())
				// these two values may be overwritten in the peer builder
				.ports(new Ports(Ports.DEFAULT_PORT, Ports.DEFAULT_PORT))
				.portsForwarding(new Ports(Ports.DEFAULT_PORT, Ports.DEFAULT_PORT));
	}

	public static ChannelClientConfiguration createDefaultChannelClientConfiguration() {
		return new ChannelClientConfiguration()
				.bindingsOutgoing(new Bindings())
				.maxPermitsPermanentTCP(MAX_PERMITS_PERMANENT_TCP)
				.maxPermitsTCP(MAX_PERMITS_TCP)
				.maxPermitsUDP(MAX_PERMITS_UDP)
				.pipelineFilter(new DefaultPipelineFilter())
				.signatureFactory(new DSASignatureFactory());
	}

	public Number160 peerId() {
		return peerId;
	}

	public KeyPair keyPair() {
		return keyPair;
	}

	public PeerBuilder keyPair(KeyPair keyPair) {
		this.keyPair = keyPair;
		return this;
	}

	public int p2pId() {
		return p2pID;
	}

	public PeerBuilder p2pId(int p2pID) {
		this.p2pID = p2pID;
		return this;
	}

	public int tcpPortForwarding() {
		return tcpPortForwarding;
	}

	public PeerBuilder tcpPortForwarding(int tcpPortForwarding) {
		this.tcpPortForwarding = tcpPortForwarding;
		return this;
	}

	public int tcpPort() {
		return tcpPort;
	}

	public PeerBuilder tcpPort(int tcpPort) {
		this.tcpPort = tcpPort;
		return this;
	}

	public int udpPort() {
		return udpPort;
	}

	public PeerBuilder udpPort(int udpPort) {
		this.udpPort = udpPort;
		return this;
	}

	public int udpPortForwarding() {
		return udpPortForwarding;
	}

	public PeerBuilder udpPortForwarding(int udpPortForwarding) {
		this.udpPortForwarding = udpPortForwarding;
		return this;
	}

	/**
	 * Sets the UDP and TCP ports to the specified value.
	 * 
	 * @param port
	 * @return
	 */
	public PeerBuilder ports(int port) {
		this.udpPort = port;
		this.tcpPort = port;
		return this;
	}

	/**
	 * Sets the external UDP and TCP ports to the specified value.
	 * 
	 * @param port
	 * @return
	 */
	public PeerBuilder portsExternal(int port) {
		this.udpPortForwarding = port;
		this.tcpPortForwarding = port;
		return this;
	}

	/**
	 * Sets the interface- and external bindings to the specified value.
	 * 
	 * @param bindings
	 * @return
	 */
	public PeerBuilder bindings(Bindings bindings) {
		this.interfaceBindings = bindings;
		this.externalBindings = bindings;
		return this;
	}

	public Bindings interfaceBindings() {
		return interfaceBindings;
	}

	public PeerBuilder interfaceBindings(Bindings interfaceBindings) {
		this.interfaceBindings = interfaceBindings;
		return this;
	}

	public Bindings externalBindings() {
		return externalBindings;
	}

	public PeerBuilder externalBindings(Bindings externalBindings) {
		this.externalBindings = externalBindings;
		return this;
	}

	public PeerMap peerMap() {
		return peerMap;
	}

	public PeerBuilder peerMap(PeerMap peerMap) {
		this.peerMap = peerMap;
		return this;
	}

	public Peer masterPeer() {
		return masterPeer;
	}

	public PeerBuilder masterPeer(Peer masterPeer) {
		this.masterPeer = masterPeer;
		return this;
	}

	public ChannelServerConfiguration channelServerConfiguration() {
		return channelServerConfiguration;
	}

	public PeerBuilder channelServerConfiguration(ChannelServerConfiguration channelServerConfiguration) {
		this.channelServerConfiguration = channelServerConfiguration;
		return this;
	}

	public ChannelClientConfiguration channelClientConfiguration() {
		return channelClientConfiguration;
	}

	public PeerBuilder channelClientConfiguration(ChannelClientConfiguration channelClientConfiguration) {
		this.channelClientConfiguration = channelClientConfiguration;
		return this;
	}

	public BroadcastHandler broadcastHandler() {
		return broadcastHandler;
	}

	public PeerBuilder broadcastHandler(BroadcastHandler broadcastHandler) {
		this.broadcastHandler = broadcastHandler;
		return this;
	}

	public BloomfilterFactory bloomfilterFactory() {
		return bloomfilterFactory;
	}

	public PeerBuilder bloomfilterFactory(BloomfilterFactory bloomfilterFactory) {
		this.bloomfilterFactory = bloomfilterFactory;
		return this;
	}

	public MaintenanceTask maintenanceTask() {
		return maintenanceTask;
	}

	public PeerBuilder maintenanceTask(MaintenanceTask maintenanceTask) {
		this.maintenanceTask = maintenanceTask;
		return this;
	}

	public Random random() {
		return random;
	}

	public PeerBuilder random(Random random) {
		this.random = random;
		return this;
	}

	public PeerBuilder init(PeerInit init) {
		toInitialize.add(init);
		return this;
	}

	public PeerBuilder init(PeerInit... inits) {
		for (PeerInit init : inits) {
			toInitialize.add(init);
		}
		return this;
	}

	public ScheduledExecutorService timer() {
		return scheduledExecutorService;
	}

	public PeerBuilder timer(ScheduledExecutorService scheduledExecutorService) {
		this.scheduledExecutorService = scheduledExecutorService;
		return this;
	}

	// isEnabled methods

	public boolean isEnableHandshakeRPC() {
		return enableHandshakeRPC;
	}

	public PeerBuilder enableHandshakeRPC(boolean enableHandshakeRPC) {
		this.enableHandshakeRPC = enableHandshakeRPC;
		return this;
	}

	public boolean isEnableNeighborRPC() {
		return enableNeighborRPC;
	}

	public PeerBuilder enableNeighborRPC(boolean enableNeighborRPC) {
		this.enableNeighborRPC = enableNeighborRPC;
		return this;
	}

	public boolean isEnableDirectDataRPC() {
		return enableDirectDataRPC;
	}

	public PeerBuilder enableDirectDataRPC(boolean enableDirectDataRPC) {
		this.enableDirectDataRPC = enableDirectDataRPC;
		return this;
	}

	public boolean isEnableRoutingRpc() {
		return enableRouting;
	}

	public PeerBuilder enableRouting(boolean enableRouting) {
		this.enableRouting = enableRouting;
		return this;
	}

	public boolean isEnableMaintenanceRpc() {
		return enableMaintenance;
	}

	public PeerBuilder enableMaintenance(boolean enableMaintenance) {
		this.enableMaintenance = enableMaintenance;
		return this;
	}

	public boolean isEnableQuitRPC() {
		return enableQuitRPC;
	}

	public PeerBuilder enableQuitRPC(boolean enableQuitRPC) {
		this.enableQuitRPC = enableQuitRPC;
		return this;
	}

	public boolean isEnableBroadcastRpc() {
		return enableBroadcast;
	}

	public PeerBuilder enableBroadcast(boolean enableBroadcast) {
		this.enableBroadcast = enableBroadcast;
		return this;
	}

	/**
	 * @return True if this peer is behind a firewall and cannot be accessed
	 *         directly
	 */
	public boolean isBehindFirewall() {
		return behindFirewall == null ? false : behindFirewall;
	}

	/**
	 * @param behindFirewall
	 *            Set to true if this peer is behind a firewall and cannot be
	 *            accessed directly
	 * @return This class
	 */
	public PeerBuilder behindFirewall(final boolean behindFirewall) {
		this.behindFirewall = behindFirewall;
		return this;
	}

	/**
	 * Sets peer to be behind a firewall and not directly accessable.
	 * 
	 * @return This class
	 */
	public PeerBuilder behindFirewall() {
		return behindFirewall(true);
	}

	/**
	 * The default filter is no filter, just return the same array.
	 * 
	 * @author Thomas Bocek
	 * 
	 */
	public static class DefaultPipelineFilter implements PipelineFilter {
		@Override
		public Map<String, Pair<EventExecutorGroup, ChannelHandler>> filter(
				final Map<String, Pair<EventExecutorGroup, ChannelHandler>> channelHandlers, boolean tcp,
				boolean client) {
			return channelHandlers;
		}
	}

	/**
	 * A pipeline filter that executes handlers in a thread. If you plan to
	 * block within listeners, then use this pipeline.
	 * 
	 * @author Thomas Bocek
	 * 
	 */
	public static class EventExecutorGroupFilter implements PipelineFilter {

		private final EventExecutorGroup eventExecutorGroup;

		public EventExecutorGroupFilter(EventExecutorGroup eventExecutorGroup) {
			this.eventExecutorGroup = eventExecutorGroup;
		}

		@Override
		public Map<String, Pair<EventExecutorGroup, ChannelHandler>> filter(
				final Map<String, Pair<EventExecutorGroup, ChannelHandler>> channelHandlers, boolean tcp,
				boolean client) {
			setExecutor("handler", channelHandlers);
			setExecutor("dispatcher", channelHandlers);
			return channelHandlers;
		}

		private void setExecutor(String handlerName,
				final Map<String, Pair<EventExecutorGroup, ChannelHandler>> channelHandlers) {
			Pair<EventExecutorGroup, ChannelHandler> pair = channelHandlers.get(handlerName);
			if (pair != null) {
				channelHandlers.put(handlerName, pair.element0(eventExecutorGroup));
			}
		}
	}
}
