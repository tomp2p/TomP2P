package net.tomp2p.holep;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.PutBuilder;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
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
			public FutureDone<PeerAddress> reply(PeerAddress sender, Object request) throws Exception {
				FutureDone<PeerAddress> futureDone = new FutureDone<PeerAddress>();
				int command = (Integer) request;
				switch (command) {
				case 0: {
					HolePStaticStorage.peerAdresses().put(sender.peerId(), sender);
					System.err.println("NEW PEERADDRESS STORED IN HOLEPSTATICSTORAGE!");
					System.err.println(sender);
					break;
				} 
				case 1: {
					if (Number160.createHash(PEER_1).equals(sender.peerId())) {
						futureDone.done(HolePStaticStorage.peerAdresses().get(Number160.createHash(PEER_2)));
						System.err.println("RETURNED PEERADDRESS OF PEER_2!");
						System.err.println(sender);
					} else {
						futureDone.done(HolePStaticStorage.peerAdresses().get(Number160.createHash(PEER_1)));
						System.err.println("RETURNED PEERADDRESS OF PEER_1!");
						System.err.println(sender);
					}
				}
				default:
					break;
				}
				
				return futureDone;
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

		// store own PeerAddress on Server
		FutureDirect fd = peer.sendDirect(masterPeerAddress).object(new Integer(0)).start();
		if (fd.isSuccess()) {
			System.err.println("OWN PEERADDRESS STORED ON SERVER!");
		} else {
			System.err.println("COULD NOT STORE OWN PEERADDRESS ON SERVER!");
		}
	}

	public void setObjectDataReply() {
		peer.objectDataReply(new ObjectDataReply() {

			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				System.out.println("SUCCESS HIT");
				System.out.println("Sender: " + sender.toString());
				System.out.println("NATPEER: " + (PeerAddress) request);
				natPeerAddress = (PeerAddress) request;
				return null;
			}
		});
	}

	public void runTextInterface() throws Exception {
		System.out.println("Welcome to the textinterface of HolePTestApp!");
		Scanner scan = new Scanner(System.in);
		boolean exit = false;

		while (!exit) {
			System.out.println("Choose a valid order. \n" + "		0 = Exit process \n" + "		1 = getNatPeerAddress() CURRENTLY NOT WORKING! \n"
					+ "		2 = putNATPeerAddress() CURRENTLY NOT WORKING! \n" + "		3 = sendDirectMessage() \n"
					+ "		4 = sendDirectNATMessage() \n" + "		5 = getOtherPeerAddress()");
			System.out.println();
			int command = scan.nextInt();
			System.out.println("You've entered the number " + command + ".");
			switch (command) {
			case 0: // end process
				scan.close();
				exit = true;
				break;
			case 3: // send Message to peer not behind a NAT
				sendDirectMessage();
				break;
			case 4: // send Message to peer behind a NAT
				sendDirectNATMessage();
				break;
			case 5: // send Relay message to get to the PeerAddress of one
					// another
				sendRelayNATMessage();
			default: // start process again
				break;
			}
		}
		// if 0 is chosen, the peer should shutdown and the program should end
		FutureShutdown fs = (FutureShutdown) peer.shutdown();
		fs.awaitUninterruptibly();
		System.exit(0);
	}

	private void sendRelayNATMessage(){
		setObjectDataReply();
		final FutureDone<PeerAddress> fDone = new FutureDone<PeerAddress>();
		FutureDirect fDirect = peer.sendDirect(masterPeerAddress).object(new Integer(1)).start();
		fDirect.addListener(new BaseFutureAdapter<FutureDone<PeerAddress>>() {

			@Override
			public void operationComplete(FutureDone<PeerAddress> future) throws Exception {
				if (future.isSuccess()) {
					PeerAddress peer2Address = (PeerAddress) future.object();
					System.err.println("RELAY SENDDIRECT SUCCESS!");
				} else {
					System.err.println("NO RELAY SENDDIRECT COULD BE MADE!");
				}
			}
		});
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
