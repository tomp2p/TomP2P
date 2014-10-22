package net.tomp2p.holep;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.Scanner;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureShutdown;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.Data;

public class HolePTestApp {
	
	private static final int port = 4001;
	private static final String PEER_1 = "peer1";
	private static final String PEER_2 = "peer2";
	
	private Peer peer;
	private PeerNAT pNAT;
//	private PeerDHT pDHT;
	private PeerAddress masterPeerAddress;
	private PeerAddress natPeerAddress;

	public HolePTestApp() {
		
	}

	public void startMasterPeer() throws Exception {
		peer = new PeerBuilder(Number160.createHash("master")).ports(port).start();
		pNAT = new PeerBuilderNAT(peer).start();
//		pDHT = new PeerBuilderDHT(peer).start();
		System.err.println("SERVER BOOTSTRAP SUCCESS!");
		System.err.println("IP: " + peer.peerAddress().inetAddress());
		System.err.println("ID: " + peer.peerID());
	}

	public void startPeer(String[] args) throws Exception {
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
		FutureRelayNAT frn = pNAT.startRelay(bootstrapPeerAddress);
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
	}

	public void setObjectDataReply() {
		peer.objectDataReply(new ObjectDataReply() {
			
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				System.out.println("SUCCESS HIT");
				System.out.println("Sender: " + sender.toString());
				String req = (String) request;
				System.err.println("Received Message: " + req);
				String reply = "reply";
				return (Object) reply;
			}
		});
	}
	
	public void runTextInterface() throws Exception {
		System.out.println("Welcome to the textinterface of HolePTestApp!");
		Scanner scan = new Scanner(System.in);
		boolean exit = false;
		
		while (!exit) {
			System.out.println("Choose a valid order. \n"
					+ "		0 = Exit process \n"
					+ "		1 = getNatPeerAddress() \n"
					+ "		2 = putNATPeerAddress() \n"
					+ "		3 = sendDirectMessage() \n"
					+ "		4 = sendDirectNATMessage() \n"
					+ "		5 = sendRelayNATMessage()");
			System.out.println();
			int order = scan.nextInt();
			System.out.println("You've entered the number " + order + ".");
			switch (order) {
			case 0: //end process
				exit = true;
				break;
			case 1: //get PeerAddress of other peer
				getNATPeerAddress();
				break;
			case 2: //put own PeerAddress of myself into DHT
				putNATPeerAddress();
			case 3: //send Message to peer not behind a NAT
				sendDirectMessage();
				break;
			case 4: //send Message to peer behind a NAT
				sendDirectNATMessage();
				break;
			case 5: //send Relay message to get to the PeerAddress of one another
				sendRelayNATMessage();
			default: //start process again
				break;
			}
		}
		// if 0 is chosen, the peer should shutdown and the program should end
		FutureShutdown fs = (FutureShutdown) peer.shutdown();
		fs.awaitUninterruptibly();
		System.exit(0);
	}

	private void sendRelayNATMessage() {
		PeerAddress p2 = new PeerAddress(Number160.createHash(PEER_2), peer.peerAddress().peerSocketAddress(), true, true, true, peer.peerAddress().peerSocketAddresses());
		p2.changeAddress(masterPeerAddress.peerSocketAddress().inetAddress());
		
		peer.objectDataReply(new ObjectDataReply() {
			
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				System.err.println(sender);
				natPeerAddress = sender;
				return null;
			}
		});
		
		FutureDirect fd = peer.sendDirect(p2).object("Hello relay World!").start();
		fd.awaitUninterruptibly(10000);
		if (fd.isFailed()) {
			System.err.println("NO RELAY SENDDIRECT COULD BE MADE!");
		} else {
			System.err.println("RELAY SENDDIRECT SUCCESS!");
		}
	}

	private void putNATPeerAddress() throws IOException {
//		//store id to DHT (important for finding other natPeer
//		pDHT = new PeerBuilderDHT(peer).start();
//		FuturePut fp = pDHT.put(peer.peerID()).object(new Data(peer.peerAddress())).start();
//		fp.awaitUninterruptibly();
//		if (!fp.isSuccess()) {
//			System.err.println("ERROR WHILE PUTTING THE PEERADDRESS INTO THE DHT!");
//		} else {
//			System.err.println("DHT PUT SUCCESS!");
//		}
		
	}
	
	private void getNATPeerAddress() {
//		if (peer.peerAddress().peerId().toString().equals(Number160.createHash(PEER_1))) {
//			getOtherNATPeerAddress(PEER_2);
//		} else {
//			getOtherNATPeerAddress(PEER_1);
//		}
	}

	private void getOtherNATPeerAddress(String peerId) {
//		FutureGet fg = pDHT.get(Number160.createHash(peerId)).start();
//		fg.awaitUninterruptibly();
//		if (!fg.isSuccess()) {
//			System.err.println("FUTUREGET FAIL WITH ARG " + peerId + "!");
//		} else {
//			System.err.println("FUTUREGET SUCCESS WITH ARG " + peerId + "!");
//		}
	}

	private void sendDirectNATMessage() throws IOException {
		setObjectDataReply();
		
		FutureDirect fd = peer.sendDirect(natPeerAddress).object("Hello World").start();
		fd.awaitUninterruptibly();
		if (!fd.isSuccess()) {
			System.err.println("SENDDIRECT-NATMESSAGE FAIL!");
		} else {
			System.err.println("SENDDIRECT-NATMESSAGE SUCCESS!");
		}
	}

	private void sendDirectMessage() throws IOException {
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
