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

import java.io.IOException;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.DefaultSendBehavior;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerCreator;
import net.tomp2p.connection.SendBehavior;
import net.tomp2p.crypto.Crypto;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.peers.Number256;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.rpc.BloomfilterFactory;
import net.tomp2p.rpc.BroadcastRPC;
import net.tomp2p.rpc.DefaultBloomfilterFactory;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.PingRPC;
import net.tomp2p.rpc.QuitRPC;
import org.whispersystems.curve25519.Curve25519KeyPair;

/**
 * The builder of a {@link Peer} class.
 * 
 * @author Thomas Bocek
 * 
 */
@Accessors(chain = true, fluent = true)
public class PeerBuilder {
	
	static {
		System.setProperty("java.net.preferIPv4Stack" , "true");
		//or use -Djava.net.preferIPv4Stack=true in the command line. This is required to make broadcasting work. See
		//https://issues.jboss.org/browse/MODCLUSTER-327 or https://code.google.com/p/kryonet/issues/detail?id=29
	}

	// if the permits are chosen too high, then we might run into timeouts as we
	// cant handle that many connections
	// withing the time limit
	public static final int MAX_PERMITS_UDP = 250;
	public static final int MAX_PERMITS_TCP = 250;

	// required
	private final Curve25519KeyPair keyPair;

	// optional with reasonable defaults
	private int p2pID = -1;
	private int udpPortForwarding = -1;
	private PeerMap peerMap = null;
	private Peer masterPeer = null;
	@Delegate private final ChannelServerConfiguration channelServerConfiguration = new ChannelServerConfiguration();

	private BroadcastHandler broadcastHandler;
	private BloomfilterFactory bloomfilterFactory;
	private ScheduledExecutorService scheduledExecutorService = null;
	private MaintenanceTask maintenanceTask = null;
	private Random random = null;
	private List<PeerInit> toInitialize = new ArrayList<PeerInit>(1);
	private SendBehavior sendBehavior;

	// enable / disable RPC/P2P/other
	@Getter @Setter
	private boolean enableHandShakeRPC = true;
	private boolean enableNeighborRPC = true;
	private boolean enableDirectDataRPC = true;
	private boolean enableBroadcast = true;
	private boolean enableRouting = true;
	private boolean enableMaintenance = true;
	private boolean enableQuitRPC = true;
	private boolean enableAnnounceRPC = true;


	/**
	 * Creates a peer builder with a random peer ID.
	 *
	 */
	public PeerBuilder() {
		this.keyPair = Crypto.cipher.generateKeyPair();
	}

	/**
	 * Creates a peer builder with the provided key pair and a peer ID that is
	 * generated out of this key pair.
	 * 
	 * @param keyPair
	 *            The public private key pair
	 */
	public PeerBuilder(final Curve25519KeyPair keyPair) {
		this.keyPair = keyPair;

	}

	/**
	 * Creates a peer and starts to listen for incoming connections.
	 * 
	 * @return The peer that can operate in the P2P network.
	 * @throws IOException .
	 */
	public Peer start() throws IOException {

	    Number256 peerId = peerId();
		
		if (p2pID == -1) {
			p2pID = 1;
		}
		
		if (peerMap == null) {
			peerMap = new PeerMap(new PeerMapConfiguration(peerId));
		}

		if (masterPeer == null && scheduledExecutorService == null) {
			scheduledExecutorService = Executors.newScheduledThreadPool(1);
		}

		if(sendBehavior == null) {
			sendBehavior = new DefaultSendBehavior();
		}
		
		final PeerCreator peerCreator;
		if (masterPeer != null) {
			peerCreator = new PeerCreator(masterPeer.peerCreator(), peerId, keyPair);
		} else {
			peerCreator = new PeerCreator(p2pID, peerId, keyPair, channelServerConfiguration,
			        scheduledExecutorService, sendBehavior);
		}

		final Peer peer = new Peer(p2pID, peerId, peerCreator);
		//add shutdown hook to master peer
		if (masterPeer != null) {
			masterPeer.addShutdownListener(new Shutdown() {
				@Override
				public BaseFuture shutdown() {
					return peer.shutdown();
				}
			});
		}

		PeerBean peerBean = peerCreator.peerBean();
		
		peerBean.addPeerStatusListener(peerMap);
		
		ConnectionBean connectionBean = peerCreator.connectionBean();

		peerBean.peerMap(peerMap);
		peerBean.keyPair(keyPair);

		if (bloomfilterFactory == null) {
			peerBean.bloomfilterFactory(new DefaultBloomfilterFactory());
		}

		if (broadcastHandler == null) {
			broadcastHandler = new StructuredBroadcastHandler();
		}
		broadcastHandler.init(peer);
		
		// set/enable RPC

		if (isEnableHandShakeRPC()) {
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

		if (isEnableBroadcast()) {
			BroadcastRPC broadcastRPC = new BroadcastRPC(peerBean, connectionBean, broadcastHandler);
			peer.broadcastRPC(broadcastRPC);
		}
		
		if (isEnableRouting() && isEnableNeighborRPC()) {
			DistributedRouting routing = new DistributedRouting(peerBean, peer.neighborRPC());
			peer.distributedRouting(routing);
		}

		if (maintenanceTask == null && isEnableMaintenance()) {
			maintenanceTask = new MaintenanceTask();
		}

		if (maintenanceTask != null) {
			maintenanceTask.init(peer, connectionBean.timer());
			maintenanceTask.addMaintainable(peerMap);
		}
		peerBean.maintenanceTask(maintenanceTask);

				
		for (PeerInit peerInit : toInitialize) {
			peerInit.init(peer);
		}
		
		return peer;
	}

	public Number256 peerId() {
		return new Number256(keyPair.getPublicKey());
	}

	public Curve25519KeyPair keyPair() {
		return keyPair;
	}


	public int p2pId() {
		return p2pID;
	}

	public PeerBuilder p2pId(int p2pID) {
		this.p2pID = p2pID;
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
	public PeerBuilder port(int port) {
		this.channelServerConfiguration.portLocal(port);
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
		return this;
	}

	/**
	 * Sets the interface- and external bindings to the specified value.
	 *
	 * @return
	 */

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

	public boolean isEnableHandShakeRPC() {
		return enableHandShakeRPC;
	}

	public PeerBuilder enableHandShakeRPC(boolean enableHandShakeRPC) {
		this.enableHandShakeRPC = enableHandShakeRPC;
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

	public boolean isEnableRouting() {
		return enableRouting;
	}

	public PeerBuilder enableRouting(boolean enableRouting) {
		this.enableRouting = enableRouting;
		return this;
	}

	public boolean isEnableMaintenance() {
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
	
	public boolean isEnableAnnounceRPC() {
		return enableAnnounceRPC;
	}

	public PeerBuilder enableAnnounceRPC(boolean enableAnnounceRPC) {
		this.enableAnnounceRPC = enableAnnounceRPC;
		return this;
	}
	

	public boolean isEnableBroadcast() {
		return enableBroadcast;
	}

	public PeerBuilder enableBroadcast(boolean enableBroadcast) {
		this.enableBroadcast = enableBroadcast;
		return this;
	}
	
	/**
	 * Set the send behavior. If none is sent, {@link DefaultSendBehavior} is used.
	 * @param sendBehavior the custom send behavior for direct messages
	 * @return This class
	 */
	public PeerBuilder sendBehavior(SendBehavior sendBehavior) {
		this.sendBehavior = sendBehavior;
		return this;
	}
	
	/**
	 * @return the current {@link SendBehavior}
	 */
	public SendBehavior sendBehavior() {
		return sendBehavior;
	}
}
