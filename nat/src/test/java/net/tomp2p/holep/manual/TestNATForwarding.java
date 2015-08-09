package net.tomp2p.holep.manual;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;

public class TestNATForwarding implements Serializable {
	
	final static private Random RND = new Random(42);
	static private Number160 relayPeerId = new Number160(RND);
	//### CHANGE THIS TO YOUR INTERFACE###
	final static private String INF = "eth1";
	
	@Before
	public void before() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("start", "0", "sym");
		LocalNATUtils.executeNatSetup("start", "1", "sym");
		LocalNATUtils.executeNatSetup("forward", "0", "4000", "10.0.0.2", "5000");
		LocalNATUtils.executeNatSetup("forward", "1", "4000", "10.0.1.2", "5000");
	}

	@After
	public void after() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("stop", "0");
		LocalNATUtils.executeNatSetup("stop", "1");
	}
	
	@Test
	public void testForward() throws Exception {
		Peer relayPeer = null;
		
		//System.exit(1);
		
		RemotePeer unr1 = null;
		RemotePeer unr2 = null;
		try {
			relayPeer = LocalNATUtils.createRealNode(relayPeerId, INF, 5002);
			
			final PeerSocketAddress relayAddress = relayPeer.peerAddress().peerSocketAddress();
			final PeerAddress relay = relayPeer.peerAddress();
			System.out.println("relay peer at: "+relay);
			
			//final Peer relayPeer1Copy = relayPeer1;
			
			unr1 = LocalNATUtils.executePeer(0, new Command() {
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.0.2", 5000, 0, 4000, "peer1");
					put("p1", peer1);
					
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					Assert.assertTrue(fd1.isDiscoveredTCP());
					Thread.sleep(2000);
					System.out.println("relay peer at1: "+relay);
					BaseFuture fb = peer1.bootstrap().peerAddress(relay).start().awaitUninterruptibly();
					Thread.sleep(2000);
					System.err.println(fb.failedReason());
					Assert.assertTrue(fb.isSuccess());
					return "done startup1";
				}
				
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					final Peer peer1 = (Peer) get("p1");
					
					//discover the 2nd relay
					//peer1.discover().peerSocketAddress(relayAddress2).start().awaitUninterruptibly();
					//System.out.println("now we know peer realy2 ");
					//final CountDownLatch cl2 = (CountDownLatch) get("cl2");
					//cl2.await();
					return "done";
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					System.err.println("shutdown");
					return LocalNATUtils.shutdown((Peer)get("p1"));
					//return "d";
				}
			});
			
			unr2 = LocalNATUtils.executePeer(1, new Command() {
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.1.2", 5000, 1, 4000, "peer2");
					put("p1", peer1);
					
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					Assert.assertTrue(fd1.isDiscoveredTCP());
					Thread.sleep(2000);
					System.out.println("relay peer at2: "+relay);
					BaseFuture fb = peer1.bootstrap().peerAddress(relay).start().awaitUninterruptibly();
					Thread.sleep(2000);
					Assert.assertTrue(fb.isSuccess());
					return "done startup1";
				}
				
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					final Peer peer1 = (Peer) get("p1");
					
					//discover the 2nd relay
					//peer1.discover().peerSocketAddress(relayAddress2).start().awaitUninterruptibly();
					//System.out.println("now we know peer realy2 ");
					//final CountDownLatch cl2 = (CountDownLatch) get("cl2");
					//cl2.await();
					return "done";
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					System.err.println("shutdown");
					return LocalNATUtils.shutdown((Peer)get("p1"));
					//return "d";
				}
			});
			
			unr1.waitFor();
			unr2.waitFor();
			
			Assert.assertEquals("done", unr1.getResult(1));
			
			} finally {
				System.out.print("LOCAL> shutdown.");
				LocalNATUtils.shutdown(relayPeer);
				System.out.print(".");
				LocalNATUtils.shutdown(unr1);
				System.out.println(".");
			}
	}
}
