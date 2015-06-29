package net.tomp2p.holep.manual;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TestNATRelay implements Serializable {

	private static final long serialVersionUID = 1L;
	final static private Random RND = new Random(42);
	static private Number160 relayPeerId = new Number160(RND);
	//### CHANGE THIS TO YOUR INTERFACE###
	final static private String INF = "eth1";

	@Before
	public void before() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("start", "0", "sym");
		LocalNATUtils.executeNatSetup("start", "1", "sym");
	}

	@After
	public void after() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("stop", "0");
		LocalNATUtils.executeNatSetup("stop", "1");
	}
	
	@SuppressWarnings("serial")
	@Test
	public void testRealRelayDifferentNAT() throws Exception {
		Peer relayPeer = null;
		RemotePeer unr1 = null;
		RemotePeer unr2 = null;
		try {
			relayPeer = LocalNATUtils.createRealNode(relayPeerId, INF);
			final PeerSocketAddress relayAddress = relayPeer.peerAddress().peerSocketAddress();
			
			unr1 = LocalNATUtils.executePeer(0, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.0.2", 5000, 0);
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					PeerNAT pn1 = new PeerBuilderNAT(peer1).start();
					//setup relay
					return "tbd";
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			
			unr2 = LocalNATUtils.executePeer(1, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.1.2", 5000, 0);
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					PeerNAT pn1 = new PeerBuilderNAT(peer1).start();
					//setup relay
					return "tbd";
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			unr1.waitFor();
			unr2.waitFor();
			
		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(relayPeer);
			System.out.print(".");
			LocalNATUtils.shutdown(unr1, unr2);
			System.out.println(".");
		}
	}
	
	@SuppressWarnings("serial")
	@Test
	public void testRealRelaySameNAT() throws Exception {
		Peer relayPeer = null;
		RemotePeer unr1 = null;
		try {
			relayPeer = LocalNATUtils.createRealNode(relayPeerId, INF);
			final PeerSocketAddress relayAddress = relayPeer.peerAddress().peerSocketAddress();
			
			unr1 = LocalNATUtils.executePeer(0, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.0.2", 5000, 0);
					Peer peer2 = LocalNATUtils.init("10.0.0.3", 5001, 1);
					put("p1", peer1);
					put("p2", peer2);
					
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					FutureDiscover fd2 = peer2.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					PeerNAT pn1 = new PeerBuilderNAT(peer1).start();
					PeerNAT pn2 = new PeerBuilderNAT(peer2).start();
					//setup relay
					
					
					return "tbd";
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
}
