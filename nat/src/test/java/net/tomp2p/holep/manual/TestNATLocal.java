package net.tomp2p.holep.manual;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.futures.FutureAnnounce;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FuturePing;
import net.tomp2p.nat.FutureNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;

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
//@Ignore
public class TestNATLocal implements Serializable {
	
	private static final long serialVersionUID = 1L;

	//### CHANGE THIS TO YOUR INTERFACE###
	final static private String INF = "enp0s25";
	
	final static private Random RND = new Random(42);
	static private Peer relayPeer = null;
	static private Number160 relayPeerId = new Number160(RND);
	
	@Before
	public void before() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("start", "0");
		LocalNATUtils.executeNatSetup("upnp", "0");
	}

	@After
	public void after() throws IOException, InterruptedException {
		//LocalNATUtils.executeNatSetup("stop", "0");
	}

	
	@SuppressWarnings("serial")
	@Test
	public void testLocalSend() throws Exception {
		Peer relayPeer = null;
		RemotePeer unr1 = null;
		try {
			relayPeer = LocalNATUtils.createRealNode(relayPeerId, INF);
			final PeerSocketAddress relayAddress = relayPeer.peerAddress().peerSocketAddress();
			
			
			unr1 = LocalNATUtils.executePeer(0, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					System.err.println("test");
					Peer peer1 = LocalNATUtils.init("10.0.0.2", 5000, 0);
					Peer peer2 = LocalNATUtils.init("10.0.0.3", 5001, 1);
					put("p1", peer1);
					put("p2", peer2);
					
					FutureAnnounce fa1 = peer1.localAnnounce().start().awaitUninterruptibly();
					FutureAnnounce fa2 = peer2.localAnnounce().start().awaitUninterruptibly();
					
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					FutureDiscover fd2 = peer2.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					PeerNAT pn1 = new PeerBuilderNAT(peer1).start();
					PeerNAT pn2 = new PeerBuilderNAT(peer2).start();
					FutureNAT fn1 = pn1.startSetupPortforwarding(fd1).awaitUninterruptibly();
					FutureNAT fn2 = pn2.startSetupPortforwarding(fd2).awaitUninterruptibly();
					if(fn1.isSuccess() && fn2.isSuccess()) {
						
						// now peer1 and peer2 know each other locally.
						PeerAddress punr2 = new PeerAddress(Number160.createHash(1), relayAddress.inetAddress(), 
								5001, 5001);
						FuturePing fp1 = peer1.ping().peerAddress(punr2).start().awaitUninterruptibly();
						System.err.println(fp1.failedReason() + " /" + fp1.remotePeer());
					} else {
						System.err.println("failed: "+ fn1.failedReason() + fn2.failedReason());
						return fn1.failedReason() + fn2.failedReason();
					}
					
					return "done";
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"), (Peer)get("p2"));
				}
			});
			unr1.waitFor();
		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(relayPeer);
			System.out.print(".");
			LocalNATUtils.shutdown(unr1);
			System.out.println(".");
		}
	}

	@Test
	public void testLocal() throws Exception {
//		relayPeer = null;
//		Process unr1 = null;
//		Process unr2 = null;
//		try {
//			relayPeer = LocalNATUtils.createRealNode(relayPeerId, "eth1");
//			InetAddress relayAddress = relayPeer.peerAddress().inetAddress();
//			unr1 = LocalNATUtils.executePeer(TestNATLocal.class, "0", relayAddress.getHostAddress(), "2");
//			unr2 = LocalNATUtils.executePeer(TestNATLocal.class, "0", relayAddress.getHostAddress(), "3");
//			String result1 = LocalNATUtils.waitForLineOrDie(unr1, "done", "command announce");
//			String result2 = LocalNATUtils.waitForLineOrDie(unr2, "done", "command announce");
//			//
//			
//			Assert.assertEquals("1", result1);
//			Assert.assertEquals("1", result2);
//			
//			String result3 = LocalNATUtils.waitForLineOrDie(unr1, "done", "command ping 0 3");
//			Assert.assertEquals("/10.0.0.3", result3);
//			
//			
//		} finally {
//			System.err.print("shutdown.");
//			if (relayPeer != null) {
//				relayPeer.shutdown().awaitUninterruptibly();
//				relayPeer = null;
//			}
//			System.err.print(".");
//			if (unr1 != null) {
//				LocalNATUtils.killPeer(unr1);
//			}
//			System.err.println(".");
//			if (unr2 != null) {
//				LocalNATUtils.killPeer(unr2);
//			}
//		}
	}

	
}
