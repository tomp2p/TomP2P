package net.tomp2p.examples;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.tcp.TCPRelayClientConfig;
import net.tomp2p.rpc.ObjectDataReply;

public class ExampleHoleP {

	public static Peer master;
	public static Peer unreachable1;
	public static Peer unreachable2;
	
	public static final Random RND = new Random();
	public static final int PORT = 4001;
	public static final int NUMBER_OF_NODES = 5;
	public static final int IDLE_UDP_SECONDS = 30;

	public static int numberOfHoles = 3;
	public static int numberOfPunches = 3;

	/**
	 * args[0] = int numberOfHoles args[1] = int numberOfPunches
	 * 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		setUp(args);
		
		// in order to send a message with Hole Punching, remember to send it via UDP
		FutureDirect fd = unreachable1.sendDirect(unreachable2.peerAddress()).forceUDP(true).object("What is the meaning of life?").start();
		System.err.println(unreachable1.peerID().toString() + ": Request sent to peer with peerId = " + unreachable2.peerID().toString());
		fd.awaitUninterruptibly();
		
		if (fd.isSuccess()) {
			System.err.println(unreachable1.peerID().toString() + ": Answer received from peer with peerId = " + unreachable2.peerID().toString() + "!");
			System.err.println(unreachable1.peerID().toString() + ": Message = " + ((String) fd.object()));
		} else {
			throw new Exception(fd.failedReason());
		}
		
		shutdown();
	}

	private static void setUp(String[] args) throws IOException {
		if (args.length > 0) {
			if (args.length > 1) {
				try {
					numberOfHoles = Integer.parseInt(args[0]);
					numberOfPunches = Integer.parseInt(args[1]);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			} else {
				try {
					numberOfHoles = Integer.parseInt(args[0]);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
		}

		// create and set up relay peer
		master = new PeerBuilder(Number160.createHash(RND.nextInt())).ports(PORT).start();
		new PeerBuilderNAT(master).start();
		System.err.println("Relay peer with PeerAddress = " + master.peerAddress().toString() + "bootstrapped!");
		
		// create and set up nat peers
		unreachable1 = setUpRelayingWithNewPeer();
		unreachable2 = setUpRelayingWithNewPeer();
		
		System.err.println("Bootstrap and Relay Setup Done!");
	}
	
	public static Peer setUpRelayingWithNewPeer() throws IOException {
		// Bootstrap natpeer
		Peer unreachable = new PeerBuilder(Number160.createHash(RND.nextInt())).ports(PORT + 1).start();
		PeerAddress pa = unreachable.peerBean().serverPeerAddress();
		pa = pa.changeFirewalledTCP(true).changeFirewalledUDP(true);
		unreachable.peerBean().serverPeerAddress(pa);

		// find neighbors
		FutureBootstrap futureBootstrap = unreachable.bootstrap().peerAddress(master.peerAddress()).start();
		futureBootstrap.awaitUninterruptibly();

		// setup relay, check NATType, and specify number of holes and punches
		PeerNAT uNat = new PeerBuilderNAT(unreachable).holePNumberOfHolePunches(numberOfPunches).holePNumberOfHoles(numberOfHoles).start();
		FutureRelayNAT frn = uNat.startRelay(new TCPRelayClientConfig(), master.peerAddress());
		frn.awaitUninterruptibly();

		System.err.println("unreachable peer with PeerAddress = " + unreachable.peerAddress().toString() + " bootstrapped!");
		
		setObjectDataReply(unreachable);
		return unreachable;
	}
	
	public static void setObjectDataReply(final Peer unreachable) {
		unreachable.objectDataReply(new ObjectDataReply() {
			
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				System.err.println(unreachable.peerID().toString() + ": New request received from Peer with peerId = " + sender.peerId().toString() + "!");
				System.err.println(unreachable.peerID().toString() + ": Message = " + ((String) request));
				return (Object) "42";
			}
		});
	}

	// don't care if there are errors when the shutdown is already done
	public static void shutdown() {
		System.err.println("shutdown initiated!");
		BaseFuture bf0 = master.shutdown();
		bf0.awaitUninterruptibly();
		System.err.println("shutdown master done!");
		
		BaseFuture bf1 = unreachable1.shutdown();
		bf1.awaitUninterruptibly();
		System.err.println("shutdown unreachable1 done!");

		BaseFuture bf2 = unreachable2.shutdown();
		bf2.awaitUninterruptibly();
		System.err.println("shutdown unreachable 2 done!");
	}
}
