package net.tomp2p.holep;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.futures.FutureAnnounce;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Add the following lines to sudoers
 * username ALL=(ALL) NOPASSWD: <location>/nat-net.sh
 * username ALL=(ALL) NOPASSWD: /usr/bin/ip
 * 
 * Make sure the network namespaces can resolve the hostname, otherwise huge delays are to be expected.
 * 
 * This testcase runs on a single machine and tests the two widely used NAT settings (port-preserving and symmetric). 
 * However, most likely this will never run on travis-ci as this requires some extra setup on the machine itself. 
 * Thus, this test-case is disabled by default and tests have to be performed manully.
 * 
 * @author Thomas Bocek
 *
 */
@Ignore
public class TestNATLocal {
	final static private Random RND = new Random(42);
	static private Peer relayPeer = null;
	static private Number160 relayPeerId = new Number160(RND);
	
	@Before
	public void before() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("start", "0", "sym");
	}

	@After
	public void after() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("stop", "0");
	}

	

	/**
	 * If you have a terrible lag in InetAddress.getLocalHost(), make sure the hostname resolves in the other network domain.
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		Peer peer = null;
		System.err.println("started on " +InetAddress.getLocalHost());
		try {
			Bindings b0 = new Bindings();
			int nr = Integer.parseInt(args[0]);
			int ip = Integer.parseInt(args[2]);
			Random rnd = new Random(args[2].hashCode());
			b0.addAddress(InetAddress.getByName("10.0." + nr + "." + ip));
			peer = new PeerBuilder(new Number160(rnd)).ports(4000).bindings(b0).start();
			System.out.println("started " + peer.peerID());
			System.err.println("started " + peer.peerID());
			String command = LocalNATUtils.read(System.in, "command");
			if (command.equals("announce")) {
				
				FutureAnnounce res = peer.localAnnounce().port(4000).start();
				res.awaitUninterruptibly();
				int size = peer.peerBean().localMap().size();
				
				
				for(int i=0;i<10;i++) {
					System.err.println("bootstrap to "+ args[1]);
					PeerAddress relayP = new PeerAddress(relayPeerId, args[1], 5002, 5002);
					peer.bootstrap().peerAddress(relayP).start().awaitUninterruptibly();
					Thread.sleep(10*1000);
				}
				
				System.out.println("done " + size);
				
			} else {
				System.out.println("empty");
			}
		} finally {
			System.out.println("finish");
			if(peer != null) {
				peer.shutdown().awaitUninterruptibly();
			}
		}
	}

	@Test
	public void testLocal() throws Exception {
		relayPeer = null;
		Process unr1 = null;
		Process unr2 = null;
		try {
			relayPeer = LocalNATUtils.createRealNode(relayPeerId, "eth1");
			InetAddress relayAddress = relayPeer.peerAddress().inetAddress();
			unr1 = LocalNATUtils.executePeer(TestNATLocal.class, "0", relayAddress.getHostAddress(), "2");
			unr2 = LocalNATUtils.executePeer(TestNATLocal.class, "0", relayAddress.getHostAddress(), "3");
			String result1 = LocalNATUtils.waitForLineOrDie(unr1, "done", "command announce");
			String result2 = LocalNATUtils.waitForLineOrDie(unr2, "done", "command announce");
			
			Assert.assertEquals("1", result1);
			Assert.assertEquals("2", result2);
			
		} finally {
			System.err.print("shutdown.");
			if (relayPeer != null) {
				relayPeer.shutdown().awaitUninterruptibly();
				relayPeer = null;
			}
			System.err.print(".");
			if (unr1 != null) {
				LocalNATUtils.killPeer(unr1);
			}
			System.err.println(".");
			if (unr2 != null) {
				LocalNATUtils.killPeer(unr2);
			}
		}
	}

	
}
