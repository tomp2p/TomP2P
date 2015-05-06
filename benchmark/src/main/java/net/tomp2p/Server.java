package net.tomp2p;

import java.io.IOException;
import java.util.Scanner;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.InteropRandom;

public class Server {

	public static void setup() throws InterruptedException, IOException {
		
		Peer server = null;
		try {
			Peer[] peers = BenchmarkUtil.createNodes(1, new InteropRandom(456), 5432, false, false);
			server = peers[0];
			server.rawDataReply(new ServerRawDataReply());
			
			PeerAddress pa = server.peerAddress();
			System.out.println(String.format("Server Peer: %s.", pa));
			System.out.println("--------------------------------------------------------------------------------------");
			System.out.println(String.format("Copy Arguments: %s %s %s %s.", pa.peerId(), pa.inetAddress(), pa.tcpPort(), pa.udpPort()));
			System.out.println("--------------------------------------------------------------------------------------");
			System.out.println("Press Enter to shut server down...");
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
            scanner.close();
			
		} finally {
			if (server != null) {
				server.shutdown().await();
			}
		}
	}
}