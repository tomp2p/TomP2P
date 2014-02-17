package net.tomp2p.p2p.real;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;

import org.junit.Ignore;
import org.junit.Test;

public class TestShutdown {
	private Random rnd = new Random();
	private Peer peer;

	public TestShutdown() throws IOException {
		boolean isConnected = false;
		PeerMaker pMaker = new PeerMaker(new Number160(rnd));
		do {
			try {
				peer = pMaker.makeAndListen();
				isConnected = true;
			} catch (IOException ex) {
				System.out.println("Port " + pMaker.tcpPort() + " busy");
				pMaker.ports(pMaker.tcpPort() + 1);
			}
		} while (!isConnected);
	}

	public void shutdown() {
		peer.shutdown();
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		TestShutdown p1 = new TestShutdown();
		System.out.println("Peer 1 created");

		TestShutdown p2 = new TestShutdown();
		System.out.println("Peer 2 created");

		System.out.println("Press any key to dispose peers");
		Thread.sleep(250);

		p1.shutdown();
		System.out.println("Peer 1 disposed");

		p2.shutdown();
		System.out.println("Peer 1 disposed");
	}

	/**
	 * This test checks wheter we can shutdown clean. JUnit stops the threads
	 * when its finished, thus this must be tested differently, or run it
	 * manually
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Ignore
	@Test
	public void test() throws IOException, InterruptedException {
		main(new String[] {});
	}
}
