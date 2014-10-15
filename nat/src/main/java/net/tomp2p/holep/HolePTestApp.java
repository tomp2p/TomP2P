package net.tomp2p.holep;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class HolePTestApp {
	
	private static final Logger LOG = LoggerFactory.getLogger(HolePTestApp.class);
	
	private static Random rand = new Random(42L);
	
	private Peer peer;
	private PeerAddress serverPeerAddress;
	private PeerAddress client1PeerAddress;
	private PeerAddress client2PeerAddress;
	private PeerBuilderNAT pNAT;

	public HolePTestApp() {
		
	}

	public boolean startServer() throws Exception {
		peer = new PeerBuilder(Number160.createHash(rand.nextInt())).ports(4001).start();
		pNAT = new PeerBuilderNAT(peer);
		FutureBootstrap fb = peer.bootstrap().ports(4001).start();
		fb.awaitUninterruptibly();
		if (!fb.isSuccess()) {
			System.err.println("SERVER BOOTSTRAP FAIL!");
			return false;
		}
		serverPeerAddress = peer.peerAddress();
		System.err.println("SERVER BOOTSTRAP SUCCESS!");
		return true;
	}

	public boolean startClient() throws Exception {
		peer = new PeerBuilder(Number160.createHash(rand.nextInt())).ports(4001).start();
		pNAT = new PeerBuilderNAT(peer);
		FutureBootstrap fb = peer.bootstrap().peerAddress(serverPeerAddress).start();
		fb.awaitUninterruptibly();
		if  (!fb.isSuccess()) {
			System.err.println("CLIENT BOOTSTRAP FAIL!");
			return false;
		}
		client1PeerAddress = peer.peerAddress();
		
		System.err.println("CLIENT BOOTSTRAP SUCCESS!");
		return true;
	}
	
	public void setObjectDataReply() {
		
	}
	
	public void runTextInterface() throws Exception {
		System.out.println("Welcome to the textinterface of HolePTestApp!");
		Scanner scan = new Scanner(System.in);
		boolean exit = false;
		
		while (!exit) {
			System.out.println("Choose a valid order. \n 0 = Exit process \n 1 = sendDirectMessage() \n 2 = sendDirectNATMessage()");
			int order = scan.nextInt();
			System.out.println("You've entered the number " + order + ".");
			switch (order) {
			case 0: //end process
				exit = true;
				break;
			case 1: //send Message to peer not behind a NAT
				sendDirectMessage();
				break;
			case 2: //send Message to peer behind a NAT
				sendDirectNATMessage();
				break;
			default: //start process again
				break;
			}
		}
	}

	private void sendDirectNATMessage() throws IOException {
		FutureDirect fd = peer.sendDirect(client2PeerAddress).object(new Data("Hello World")).start();
		fd.awaitUninterruptibly();
		if (!fd.isSuccess()) {
			System.err.println("SENDDIRECT-NATMESSAGE FAIL!");
		} else {
			System.err.println("SENDDIRECT-NATMESSAGE SUCCESS");
		}
	}

	private void sendDirectMessage() throws IOException {
		FutureDirect fd = peer.sendDirect(client2PeerAddress).object(new Data("Hello World")).start();
		fd.awaitUninterruptibly();
		if (!fd.isSuccess()) {
			System.err.println("SENDDIRECT-MESSAGE FAIL!");
		} else {
			System.err.println("SENDDIRECT-MESSAGE SUCCESS");
		}
	}
}
