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

package net.tomp2p.relay.android;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.AutomaticFuture;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.rpc.RPC.Commands;

/*
 * This is a copy of TestRelay from tomp2p-nat. Maven does not allow to depend on test code from other modules:
 * http://jira.codehaus.org/browse/MNG-3559
 */

public class UtilsNAT {

	public static PeerDHT[] createNodesDHT(int nrOfPeers, Random rnd, int port) throws Exception {
		return createNodesDHT(nrOfPeers, rnd, port, null);
	}

	public static PeerDHT[] createNodesDHT(int nrOfPeers, Random rnd, int port, AutomaticFuture automaticFuture)
			throws Exception {
		return createNodesDHT(nrOfPeers, rnd, port, automaticFuture, false);
	}

	/**
	 * Creates peers for testing. The first peer (peer[0]) will be used as the master. This means that
	 * shutting down
	 * peer[0] will shut down all other peers
	 * 
	 * @param nrOfPeers
	 *            The number of peers to create including the master
	 * @param rnd
	 *            The random object to create random peer IDs
	 * @param port
	 *            The port where the master peer will listen to
	 * @return All the peers, with the master peer at position 0 -> peer[0]
	 * @throws Exception
	 *             If the creation of nodes fail.
	 */
	public static PeerDHT[] createNodesDHT(int nrOfPeers, Random rnd, int port, AutomaticFuture automaticFuture,
			boolean maintenance) throws Exception {
		if (nrOfPeers < 1) {
			throw new IllegalArgumentException("Cannot create less than 1 peer");
		}
		Bindings bindings = new Bindings();// .addInterface("lo");
		PeerDHT[] peers = new PeerDHT[nrOfPeers];
		final Peer master;
		if (automaticFuture != null) {
			Number160 peerId = new Number160(rnd);
			PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId));
			master = new PeerBuilder(peerId).ports(port).enableMaintenance(maintenance).bindings(bindings).peerMap(peerMap)
					.start().addAutomaticFuture(automaticFuture);
			peers[0] = new PeerBuilderDHT(master).start();

		} else {
			Number160 peerId = new Number160(rnd);
			PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId));
			master = new PeerBuilder(peerId).enableMaintenance(maintenance).bindings(bindings).peerMap(peerMap).ports(port)
					.start();
			peers[0] = new PeerBuilderDHT(master).start();
		}

		for (int i = 1; i < nrOfPeers; i++) {
			if (automaticFuture != null) {
				Number160 peerId = new Number160(rnd);
				PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId));
				Peer peer = new PeerBuilder(peerId).masterPeer(master).enableMaintenance(maintenance)
						.enableMaintenance(maintenance).peerMap(peerMap).bindings(bindings).start()
						.addAutomaticFuture(automaticFuture);
				peers[i] = new PeerBuilderDHT(peer).start();
			} else {
				Number160 peerId = new Number160(rnd);
				PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId).peerNoVerification());
				Peer peer = new PeerBuilder(peerId).enableMaintenance(maintenance).bindings(bindings).peerMap(peerMap)
						.masterPeer(master).start();
				peers[i] = new PeerBuilderDHT(peer).start();
			}
		}
		System.err.println("peers created.");
		return peers;
	}

	/**
	 * Perfect routing, where each neighbor has contacted each other. This means that for small number of
	 * peers, every
	 * peer knows every other peer.
	 * 
	 * @param peers
	 *            The peers taking part in the p2p network.
	 */
	public static void perfectRouting(PeerDHT... peers) {
		for (int i = 0; i < peers.length; i++) {
			for (int j = 0; j < peers.length; j++)
				peers[i].peer().peerBean().peerMap().peerFound(peers[j].peer().peerAddress(), null, null, null);
		}
		System.err.println("perfect routing done.");
	}

	public static Peer[] createNodes(int nrOfPeers, Random rnd, int port) throws Exception {
		return createNodes(nrOfPeers, rnd, port, null);
	}

	public static Peer[] createNodes(int nrOfPeers, Random rnd, int port, AutomaticFuture automaticFuture) throws Exception {
		return createNodes(nrOfPeers, rnd, port, automaticFuture, false);
	}

	/**
	 * Creates peers for testing. The first peer (peer[0]) will be used as the master. This means that
	 * shutting down
	 * peer[0] will shut down all other peers
	 * 
	 * @param nrOfPeers
	 *            The number of peers to create including the master
	 * @param rnd
	 *            The random object to create random peer IDs
	 * @param port
	 *            The port where the master peer will listen to
	 * @return All the peers, with the master peer at position 0 -> peer[0]
	 * @throws Exception
	 *             If the creation of nodes fail.
	 */
	public static Peer[] createNodes(int nrOfPeers, Random rnd, int port, AutomaticFuture automaticFuture,
			boolean maintenance) throws Exception {
		if (nrOfPeers < 1) {
			throw new IllegalArgumentException("Cannot create less than 1 peer");
		}

		Bindings bindings = new Bindings().addAddress(InetAddress.getLocalHost());
		// Bindings bindings = new Bindings().addInterface("lo0");
		Peer[] peers = new Peer[nrOfPeers];
		if (automaticFuture != null) {
			Number160 peerId = new Number160(rnd);
			PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId));
			peers[0] = new PeerBuilder(peerId).ports(port).enableMaintenance(maintenance).bindings(bindings)
					.peerMap(peerMap).start().addAutomaticFuture(automaticFuture);
		} else {
			Number160 peerId = new Number160(rnd);
			PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId));
			peers[0] = new PeerBuilder(peerId).enableMaintenance(maintenance).bindings(bindings).peerMap(peerMap)
					.ports(port).start();
		}

		for (int i = 1; i < nrOfPeers; i++) {
			if (automaticFuture != null) {
				Number160 peerId = new Number160(rnd);
				PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId));
				peers[i] = new PeerBuilder(peerId).masterPeer(peers[0]).enableMaintenance(maintenance)
						.enableMaintenance(maintenance).peerMap(peerMap).bindings(bindings).start()
						.addAutomaticFuture(automaticFuture);
			} else {
				Number160 peerId = new Number160(rnd);
				PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId).peerNoVerification());
				peers[i] = new PeerBuilder(peerId).enableMaintenance(maintenance).bindings(bindings).peerMap(peerMap)
						.masterPeer(peers[0]).start();
			}
		}
		System.err.println("peers created.");
		return peers;
	}

	/**
	 * Perfect routing, where each neighbor has contacted each other. This means that for small number of
	 * peers, every
	 * peer knows every other peer.
	 * 
	 * @param peers
	 *            The peers taking part in the p2p network.
	 */
	public static void perfectRouting(Peer... peers) {
		for (int i = 0; i < peers.length; i++) {
			for (int j = 0; j < peers.length; j++)
				peers[i].peerBean().peerMap().peerFound(peers[j].peerAddress(), null, null, null);
		}
		System.err.println("perfect routing done.");
	}

	public static PeerAddress createAddress() throws UnknownHostException {
		return createAddress(new Number160("0x5678"), "127.0.0.1", 8005, 8006, false, false);
	}

	public static PeerAddress createRandomAddress() throws UnknownHostException {
		Random rnd = new Random();
		return createAddress(new Number160(rnd), "127.0.0.1", rnd.nextInt(10000), rnd.nextInt(10000), rnd.nextBoolean(),
				rnd.nextBoolean());
	}

	public static PeerAddress createAddress(Number160 idSender, String inetSender, int tcpPortSender, int udpPortSender,
			boolean firewallUDP, boolean firewallTCP) throws UnknownHostException {
		InetAddress inetSend = InetAddress.getByName(inetSender);
		PeerSocketAddress peerSocketAddress = new PeerSocketAddress(inetSend, tcpPortSender, udpPortSender);
		PeerAddress n1 = new PeerAddress(idSender, peerSocketAddress, firewallTCP, firewallUDP, false, false, false,
				PeerAddress.EMPTY_PEER_SOCKET_ADDRESSES);
		return n1;
	}

	/**
	 * Creates a message with random content
	 */
	public static Message createRandomMessage() {
		Random rnd = new Random();

		Message message = new Message();
		message.command(Commands.values()[rnd.nextInt(Commands.values().length)].getNr());
		message.type(Type.values()[rnd.nextInt(Type.values().length)]);
		message.recipientSocket(new InetSocketAddress(1234));
		message.recipient(new PeerAddress(new Number160(rnd), message.recipientSocket()));
		message.senderSocket(new InetSocketAddress(5678));
		message.sender(new PeerAddress(new Number160(rnd), message.senderSocket()));
		return message;
	}

	public static boolean messagesEqual(Message m1, Message m2) {
		return m1.messageId() == m2.messageId()
				&& m1.hasContent() == m2.hasContent()
				&& m1.type() == m2.type()
				&& m1.command() == m2.command()
				&& m1.sender().equals(m2.sender()) 
				&& m1.recipient().equals(m2.recipient())
				&& m1.sender().tcpPort() == m2.sender().tcpPort()
				&& m1.sender().udpPort() == m2.sender().udpPort()
				&& m1.recipient().tcpPort() == m2.recipient().tcpPort()
				&& m1.recipient().udpPort() == m2.recipient().udpPort();
	}
}
