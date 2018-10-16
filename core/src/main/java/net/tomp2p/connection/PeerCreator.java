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

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.security.KeyPair;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.peers.IP;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;

/**
 * Creates a peer and listens to incoming connections. The result of creating
 * this class is the connection bean and the peer bean. While the connection
 * bean holds information that can be shared, the peer bean holds information
 * that is unique for each peer.
 * 
 * @author Thomas Bocek
 * 
 */
public class PeerCreator {
	private static final Logger LOG = LoggerFactory.getLogger(PeerCreator.class);

	private final ConnectionBean connectionBean;
	private final PeerBean peerBean;

	private final boolean master;

	private final FutureDone<Void> futureServerDone = new FutureDone<Void>();

	/**
	 * Creates a master peer and starts UDP and TCP channels.
	 * 
	 * @param p2pId
	 *            The id of the network
	 * @param peerId
	 *            The id of this peer
	 * @param keyPair
	 *            The key pair or null
	 * @param channelServerConfiguration
	 *            The server configuration to create the channel server that is
	 *            used for listening for incoming connections
	 * @param timer
	 *            The executor service
	 * @param sendBehavior
	 * 			  The sending behavior for direct messages
	 * @throws IOException
	 *            If the startup of listening to connections failed
	 */
	public PeerCreator(final int p2pId, final Number160 peerId, final KeyPair keyPair,
	        final ChannelServerConfiguration channelServerConfiguration,
	        final ScheduledExecutorService timer, SendBehavior sendBehavior) throws IOException {
		//peer bean
		peerBean = new PeerBean().keyPair(keyPair);
		PeerAddress self = findPeerAddress(peerId, channelServerConfiguration);
		peerBean.serverPeerAddress(self);
		LOG.info("Visible address to other peers: {}", self);
		
		//start server
		Dispatcher dispatcher = new Dispatcher(p2pId, peerBean, channelServerConfiguration);
		final ChannelTransceiver channelServer = new ChannelTransceiver(channelServerConfiguration,
		        dispatcher, timer, peerBean);	
		
		//connection bean
		connectionBean = new ConnectionBean(p2pId, dispatcher, channelServer, timer);
		this.master = true;
	}

	/**
	 * Creates a slave peer that will attach itself to a master peer.
	 * 
	 * @param parent
	 *            The parent peer
	 * @param peerId
	 *            The id of this peer
	 * @param keyPair
	 *            The key pair or null
	 */
	public PeerCreator(final PeerCreator parent, final Number160 peerId, final KeyPair keyPair) {
		this.connectionBean = parent.connectionBean;
		this.peerBean = new PeerBean().keyPair(keyPair);
		PeerAddress self = parent.peerBean().serverPeerAddress().withPeerId(peerId);
		this.peerBean.serverPeerAddress(self);
		this.master = false;
	}

	/**
	 * Shutdown the peer. If the peer is a master, then also the connections and
	 * the server will be closed, otherwise its just de-registering.
	 * 
	 * @return The future when the shutdown is complete
	 */
	public FutureDone<Void> shutdown() {
		if (master) {
			LOG.debug("Shutting down...");
		}
		// de-register in dispatcher
		connectionBean.dispatcher().removeIoHandler(peerBean().serverPeerAddress().peerId());
		// shutdown running tasks for this peer
		if (peerBean.maintenanceTask() != null) {
			peerBean.maintenanceTask().shutdown();
		}
		
		// shutdown all children
		if (!master) {
			return futureServerDone.done();
		}
		// shutdown the timer
		for(Runnable runner: connectionBean.timer().shutdownNow()) {
			runner.run();
		}
		
		LOG.debug("Shutting down client...");
		connectionBean.channelServer().shutdown().addListener(new BaseFutureAdapter<FutureDone<Void>>() {		
			@Override
			public void operationComplete(final FutureDone<Void> future) throws Exception {
				futureServerDone.done();
			}
		});
			
		// this is blocking
		return futureServerDone;
	}
	


	/**
	 * @return The bean that holds information that is unique for all peers
	 */
	public PeerBean peerBean() {
		return peerBean;
	}

	/**
	 * @return The bean that holds information that may be shared among peers
	 */
	public ConnectionBean connectionBean() {
		return connectionBean;
	}

	/**
	 * Creates the {@link PeerAddress} based on the network discovery.
	 * 
	 * @param peerId
	 *            The id of this peer
	 * @return The peer address of this peer
	 * @throws IOException
	 *             If the address could not be determined
	 */
	private static PeerAddress findPeerAddress(final Number160 peerId,
	        final ChannelServerConfiguration channelServerConfiguration) throws IOException {
		final DiscoverResults discoverResults = DiscoverNetworks.discoverInterfaces(
				channelServerConfiguration.bindings());
		final String status = discoverResults.status();
		if (LOG.isInfoEnabled()) {
			LOG.info("Status of external address search: " + status);
		}
		//this is just a guess and will be changed once discovery is done
		InetAddress outsideAddress = discoverResults.foundAddress();
		if(outsideAddress == null) {
			throw new IOException("Not listening to anything. Maybe the binding information is wrong.");
		}
		
		
		final PeerSocket4Address peerSocketAddress = PeerSocket4Address.builder().ipv4(IP.fromInet4Address((Inet4Address)outsideAddress))
				.udpPort(channelServerConfiguration.portLocal())
				.build(); 
		
		final PeerAddress self = PeerAddress.builder()
				.peerId(peerId)
				.ipv4Socket(peerSocketAddress)
				.reachable4UDP(!channelServerConfiguration.behindFirewall())
				.reachable6UDP(!channelServerConfiguration.behindFirewall())
				.build();
		
		return self;
	}
}
