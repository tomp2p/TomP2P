package net.tomp2p.rcon.prototype;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.nat.FutureNAT;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.FutureRelay;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.Data;

public class SimpleRconClient {

	private static int port = 4001;
	private static Peer peer;
	private static PeerAddress masterPeerAddress;
	private static String masterIpAddress;
	private static PeerConnection peerConnection;

	public static void start(boolean isMaster, String id) {
		// Create a peer with a random peerID, on port 4001, listening to the
		// interface eth0
		try {
			createPeer(isMaster, id);

			peer.objectDataReply(new ObjectDataReply() {

				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					System.out.println("SUCCESS HIT");

					System.out.println("Sender: " + sender.toString());

					String req = (String) request;
					System.out.println(req);

					String reply = "reply";
					return (Object) reply;
				}
			});

		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(peer.peerAddress().toString());
	}

	private static void createPeer(boolean isMaster, String id) throws IOException {
		if (isMaster) {
			peer = new PeerBuilder(Number160.createHash("master")).ports(port).start();
			new PeerNAT(peer);
		} else {
			peer = new PeerBuilder(Number160.createHash(id)).ports(port).start();
			new PeerNAT(peer);
		}
	}

	public static Peer getPeer() {
		return peer;
	}

	public static boolean usualBootstrap(String ip) throws UnknownHostException {
		boolean success = false;
		masterIpAddress = ip;

		masterPeerAddress = new PeerAddress(Number160.createHash("master"), Inet4Address.getByName(masterIpAddress),
				port, port);

		// do PeerDiscover
		FutureDiscover fd = peer.discover().peerAddress(peer.peerAddress()).start().awaitUninterruptibly();
		if (!fd.isSuccess()) {
			return success;
		}

		FutureBootstrap fb = peer.bootstrap().peerAddress(masterPeerAddress).start();
		fb.awaitUninterruptibly();
		if (fb.isSuccess()) {
			System.out.println("Bootstrap success!");
			success = true;
		} else {
			System.out.println("Bootstrap fail!");
		}

		return success;
	}

	public static boolean sendDummy(String dummy, String id, String ip) throws IOException {
		boolean success = false;
		PeerAddress recepient = null;

		if (id == null || ip == null) {
			System.out.println("MESSAGE SENT TO MASTER");
			recepient = masterPeerAddress;
		} else {
			System.out.println("DIRECTED MESSAGE TO " + ip);
			recepient = new PeerAddress(Number160.createHash(id), Inet4Address.getByName(ip), port, port);
		}

		FutureDirect fd = peer.sendDirect(recepient).object(dummy).start();
		fd.awaitUninterruptibly(10000);

		if (fd.isSuccess()) {
			System.out.println("FUTURE DIRECT SUCCESS!");
			success = true;
		} else {
			System.out.println("FUTURE DIRECT FAIL!");
		}

		return success;
	}

	public static boolean sendDummy(String message) throws UnknownHostException {
		boolean success = false;
		PeerAddress recipient = masterPeerAddress;
		recipient = recipient.changeRelayed(true);

		FutureDirect fd = peer.sendDirect(recipient).object(message).start();
		fd.awaitUninterruptibly(10000);
		
		if (fd.isSuccess()) {
			System.out.println("FUTURE DIRECT SUCCESS!");
			success = true;
		} else {
			System.out.println("FUTURE DIRECT FAIL!");
		}

		return success;
	}
	
	public static boolean sendNATDummy(String message) throws UnknownHostException {
		boolean success = false;
		PeerAddress recipient = masterPeerAddress;
		recipient = recipient.changeRelayed(true);
		recipient = recipient.changePeerId(Number160.createHash("NAT"));

		FutureDirect fd = peer.sendDirect(recipient).object(message).start();
		fd.awaitUninterruptibly(10000);
		
		if (fd.isSuccess()) {
			System.out.println("FUTURE DIRECT SUCCESS!");
			success = true;
		} else {
			System.out.println("FUTURE DIRECT FAIL!");
		}

		return success;
	}

	public static void natBootstrap(String ip) throws UnknownHostException {
		PeerAddress bootstrapPeerAddress = new PeerAddress(Number160.createHash("master"), Inet4Address.getByName(ip),
				port, port);
		masterPeerAddress = bootstrapPeerAddress;

		// Set the isFirewalledUDP and isFirewalledTCP flags
		PeerAddress upa = peer.peerBean().serverPeerAddress();
		upa = upa.changeFirewalledTCP(true).changeFirewalledUDP(true);
		peer.peerBean().serverPeerAddress(upa);

		// find neighbors
		FutureBootstrap futureBootstrap = peer.bootstrap().peerAddress(bootstrapPeerAddress).start();
		futureBootstrap.awaitUninterruptibly();

		// setup relay
		PeerNAT uNat = new PeerNAT(peer);
		// set up 3 relays
		FutureRelay futureRelay = uNat.minRelays(1).startSetupRelay();
		futureRelay.awaitUninterruptibly();

		// find neighbors again
		FutureBootstrap fb = peer.bootstrap().peerAddress(bootstrapPeerAddress).start();
		fb.awaitUninterruptibly();

		// do maintenance
		// uNat.bootstrapBuilder(peer.bootstrap().peerAddress(bootstrapPeerAddress));
		// uNat.startRelayMaintenance(futureRelay);
	}

}
