package net.tomp2p;

import java.io.IOException;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

public class Pings {
	
	private static Peer receiver = null;

	public static void startJavaPingReceiver(String argument) throws IOException {
		
		// setup a receiver, write it's address to harddisk and notify via System.out

		try {
			receiver = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
			
			byte[] result = receiver.peerAddress().toByteArray();
			InteropUtil.writeToFile(argument, result);

			System.out.println("[---RESULT-READY---]");

		} catch (Exception ex) {
			System.err.println("Exception during startJavaPingReceiver.");
			stopJavaPingReceiver();
			throw ex;
		}
	}
	
	public static void stopJavaPingReceiver()
	{
		if (receiver != null) {
			receiver.shutdown();
		}
	}
}
