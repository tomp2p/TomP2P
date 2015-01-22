package net.tomp2p.holep.testapp;

import java.io.IOException;
import java.net.Inet4Address;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayClientConfig;
import net.tomp2p.rpc.ObjectDataReply;

public class HolePTestApp {

	private static final int port = 4001;
	private static final String PEER_1 = "peer1";
	private static final String PEER_2 = "peer2";
	private static final String PEER_3 = "peer3";
	private static final RelayClientConfig RELAY_CONFIG = RelayClientConfig.OpenTCP();

	private Peer peer;
	private PeerNAT pNAT;
	private PeerAddress masterPeerAddress;
	private PeerAddress natPeerAddress;

	public HolePTestApp() {

	}

	public void startMasterPeer() throws Exception {
		peer = new PeerBuilder(Number160.createHash("master")).ports(port).start();
		pNAT = new PeerBuilderNAT(peer).start();
		System.err.println("SERVER BOOTSTRAP SUCCESS!");
		System.err.println("IP: " + peer.peerAddress().inetAddress());
		System.err.println("ID: " + peer.peerID());

		// store own PeerAddress into storage
		HolePStaticStorage.peerAdresses().put(peer.peerID(), peer.peerAddress());

		peer.objectDataReply(new ObjectDataReply() {

			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				Object reply = null;
				int command = (Integer) request;
				switch (command) {
				case 0: {
					HolePStaticStorage.peerAdresses().put(sender.peerId(), sender);
					System.err.println("NEW PEERADDRESS STORED IN HOLEPSTATICSTORAGE!");
					System.err.println(sender);
					break;
				}
				case 1: {
					if (Number160.createHash(PEER_1).equals(sender.peerId()) || Number160.createHash(PEER_3).equals(sender.peerId())) {
						reply = HolePStaticStorage.peerAdresses().get(Number160.createHash(PEER_2));
						System.err.println("RETURNED PEERADDRESS OF PEER_2!");
						System.err.println(sender);
					} else {
						reply = HolePStaticStorage.peerAdresses().get(Number160.createHash(PEER_1));
						System.err.println("RETURNED PEERADDRESS OF PEER_1!");
						System.err.println(sender);
					}
				}
				default:
					break;
				}
				
				return reply;
			}
		});
	}

	public void startNormalPeer(String[] args) throws Exception {
		peer = new PeerBuilder(Number160.createHash(args[1])).ports(port).start();
		FutureBootstrap fb = peer.bootstrap().inetAddress(Inet4Address.getByName(args[0])).ports(port).start();
		fb.awaitUninterruptibly();
		if (!fb.isSuccess()) {
			System.err.println("ERROR WHILE NORMAL-BOOTSTRAPPING. THE APPLICATION WILL NOW SHUTDOWN!");
			System.exit(1);
		} else {
			System.err.println("NORMAL-BOOTSTRAP SUCCESS!");
		}
	}

	public void startNATPeer(String[] args) throws Exception {
		peer = new PeerBuilder(Number160.createHash(args[1])).ports(port).start();
		PeerAddress bootstrapPeerAddress = new PeerAddress(Number160.createHash("master"), Inet4Address.getByName(args[0]), port, port);
		masterPeerAddress = bootstrapPeerAddress;

		// Set the isFirewalledUDP and isFirewalledTCP flags
		PeerAddress upa = peer.peerBean().serverPeerAddress();
		upa = upa.changeFirewalledTCP(true).changeFirewalledUDP(true);
		peer.peerBean().serverPeerAddress(upa);

		// find neighbors
		FutureBootstrap futureBootstrap = peer.bootstrap().peerAddress(bootstrapPeerAddress).start();
		futureBootstrap.awaitUninterruptibly();

		// setup relay
		pNAT = new PeerBuilderNAT(peer).start();

		// set up 3 relays
		// FutureRelay futureRelay = uNat.startSetupRelay(new FutureRelay());
		// futureRelay.awaitUninterruptibly();
		FutureRelayNAT frn = pNAT.startRelay(RELAY_CONFIG, bootstrapPeerAddress);
		frn.awaitUninterruptibly();

		// find neighbors again
		FutureBootstrap fb = peer.bootstrap().peerAddress(bootstrapPeerAddress).start();
		fb.awaitUninterruptibly();
		if (!fb.isSuccess()) {
			System.err.println("ERROR WHILE NAT-BOOTSTRAPPING. THE APPLICATION WILL NOW SHUTDOWN!");
			System.exit(1);
		} else {
			System.err.println("NAT-BOOTSTRAP SUCCESS!");
		}

		// do maintenance
		// uNat.bootstrapBuilder(peer.bootstrap().peerAddress(bootstrapPeerAddress));
		// uNat.startRelayMaintenance(futureRelay);

		// store own PeerAddress on Server
		FutureDirect fd = peer.sendDirect(masterPeerAddress).object(new Integer(0)).start();
		fd.addListener(new BaseFutureAdapter<FutureDirect>() {

			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				if (future.isSuccess()) {
					System.err.println("OWN PEERADDRESS STORED ON SERVER!");
				} else {
					System.err.println("COULD NOT STORE OWN PEERADDRESS ON SERVER!");
				}
			}
		});
		
		setObjectDataReply2();
	}

	private void setObjectDataReply2() {
		peer.objectDataReply(new ObjectDataReply() {

			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				System.out.println();
				System.out.println();
				System.out.println();
				System.out.println();
				System.out.println();
				System.out.println("SUCCESS HIT");
				System.out.println("Sender: " + sender.toString());
				System.err.println("NATPEER: " + ((String) request));
				System.out.println();
				System.out.println();
				System.out.println();
				System.out.println();
				System.out.println();
				natPeerAddress = sender;
				return "Hello Successful TomP2P holepunching request";
			}
		});
	}

	private void setObjectDataReply() {
		peer.objectDataReply(new ObjectDataReply() {

			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				System.out.println("Sender: " + sender.toString());
				System.out.println("NATPEER: " + (PeerAddress) request);
				natPeerAddress = (PeerAddress) request;
				return null;
			}
		});
	}

	public void getOtherPeerAddress() throws ClassNotFoundException, IOException {
		setObjectDataReply();
		FutureDirect fDirect = peer.sendDirect(masterPeerAddress).object(new Integer(1)).start();
		fDirect.awaitUninterruptibly();

		if (fDirect.isSuccess()) {
			System.err.println("Retrieval of PeerAddress of Peer2 successfull!");
			natPeerAddress = (PeerAddress) fDirect.object();
		} else {
			System.err.println("FAILFAILFAIL!");
		}
		setObjectDataReply2();
	}

	public void sendHolePMessage(int port) throws IOException {
		setObjectDataReply();
		
		if (port == -1) {
			port = 8080;
		}

		FutureDirect fd = peer.sendDirect(natPeerAddress).object("Hello World").forceUDP(true).start();
		fd.awaitUninterruptibly();
		
		if (fd.isSuccess()) {
			System.err.println("WORKS!");
		} else {
			System.err.println("DOES NOT WORK!");
		}
	}

	public void sendDirectMessage() throws IOException {
		setObjectDataReply();

		peer.peerAddress().changeRelayed(true);
		SendDirectBuilder sdb = peer.sendDirect(masterPeerAddress.changeRelayed(true)).forceUDP(true).forceTCP(false).object("Hello World");
		FutureDirect fd = sdb.start();
		fd.awaitUninterruptibly();
		if (!fd.isSuccess()) {
			System.err.println("SENDDIRECT-MESSAGE FAIL!");
		} else {
			System.err.println("SENDDIRECT-MESSAGE SUCCESS!");
		}
	}
}
