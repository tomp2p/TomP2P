package net.tomp2p;

import java.io.IOException;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class Pings {
	
	private static Peer receiver = null;

	public static void startJavaPingReceiver() throws IOException {
		
		// setup a receiver and write its PeerAddress to System.out

		try {
			receiver = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
			
			PeerAddress address = receiver.peerAddress();
			System.out.println(String.format("[---%s-%s---]", address.isIPv6() ? "1" : "0", address));

		} finally {
			stopJavaPingReceiver();
		}
	}
	
	public static void stopJavaPingReceiver()
	{
		if (receiver != null) {
			receiver.shutdown();
		}
	}
}
