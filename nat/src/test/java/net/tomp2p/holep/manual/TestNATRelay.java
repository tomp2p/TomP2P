package net.tomp2p.holep.manual;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.relay.BaseRelayServer;
import net.tomp2p.relay.RelayCallback;
import net.tomp2p.rpc.ObjectDataReply;

//@Ignore
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
			PeerNAT rn1 = new PeerBuilderNAT(relayPeer).start();
			final PeerSocketAddress relayAddress = relayPeer.peerAddress().peerSocketAddress();
			
			unr1 = LocalNATUtils.executePeer(0, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.0.2", 5000, 0);
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					final CountDownLatch cl = new CountDownLatch(1);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(new RelayCallback() {
						@Override
						public void onRelayRemoved(PeerAddress candidate, PeerConnection object) {}
						@Override
						public void onRelayAdded(PeerAddress candidate, PeerConnection object) {cl.countDown();}
					}).start();
					//setup relay
					
					pn1.startRelay();
					cl.await();
					
					peer1.objectDataReply(new ObjectDataReply() {
						@Override
						public Object reply(PeerAddress sender, Object request) throws Exception {
							return "me1";
						}
					});

					PeerAddress peer2 = LocalNATUtils.peerAddress("10.0.1.2", 5000, 1);
					Collection<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>();
					psa.add(relayAddress);
					peer2 = peer2.changePeerSocketAddresses(psa);
					peer2 = peer2.changeFirewalledTCP(true).changeFirewalledUDP(true).changeRelayed(true);
					FutureDirect fdir1 = peer1.sendDirect(peer2).object("test").start().awaitUninterruptibly();
					System.out.println(fdir1.failedReason());
					Assert.assertTrue(fdir1.isSuccess());
					System.err.println("DONE1");
					return "me2".equals(fdir1.object()) ? "TRUE" : "FALSE";
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					System.err.println("shutdown0");
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			
			unr2 = LocalNATUtils.executePeer(1, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.1.2", 5000, 1);
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					final CountDownLatch cl = new CountDownLatch(1);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(new RelayCallback() {
						@Override
						public void onRelayRemoved(PeerAddress candidate, PeerConnection object) {}
						@Override
						public void onRelayAdded(PeerAddress candidate, PeerConnection object) {cl.countDown();}
					}).start();
					//setup relay
					
					pn1.startRelay();
					cl.await();
					
					
					peer1.objectDataReply(new ObjectDataReply() {
						@Override
						public Object reply(PeerAddress sender, Object request) throws Exception {
							return "me2";
						}
					});

					PeerAddress peer2 = LocalNATUtils.peerAddress("10.0.0.2", 5000, 0);
					Collection<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>();
					psa.add(relayAddress);
					peer2 = peer2.changePeerSocketAddresses(psa);
					peer2 = peer2.changeFirewalledTCP(true).changeFirewalledUDP(true).changeRelayed(true);
					FutureDirect fdir1 = peer1.sendDirect(peer2).object("test").start().awaitUninterruptibly();
					System.out.println(fdir1.failedReason());
					Assert.assertTrue(fdir1.isSuccess());
					System.err.println("DONE2");
					return "me1".equals(fdir1.object()) ? "TRUE" : "FALSE";
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					System.err.println("shutdown1");
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			unr1.waitFor();
			unr2.waitFor();
			Assert.assertEquals("TRUE", unr1.getResult(0));
			Assert.assertEquals("TRUE", unr2.getResult(0));
			
		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(relayPeer);
			System.out.print(".");
			LocalNATUtils.shutdown(unr1, unr2);
			System.out.println(".");
		}
	}
	
	
	@Test
	public void testRealRelaySameNATNoRelay() throws Exception {
		
		RemotePeer unr1 = null;
		try {
			unr1 = LocalNATUtils.executePeer(0, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.0.2", 5000, 0);
					Peer peer2 = LocalNATUtils.init("10.0.0.3", 5001, 1);
					
					//send message from p1 to p2
					peer2.objectDataReply(new ObjectDataReply() {
						@Override
						public Object reply(PeerAddress sender, Object request) throws Exception {
							return "me";
						}
					});
					
					put("p1", peer1);
					put("p2", peer2);
					
					PeerNAT pn1 = new PeerBuilderNAT(peer1).start();
					PeerNAT pn2 = new PeerBuilderNAT(peer2).start();
					//setup relay
					pn1.startRelay();
					pn2.startRelay();
					
					
					FutureDirect fdir1 = peer1.sendDirect(peer2.peerAddress()).object("test").start().awaitUninterruptibly();
					Assert.assertEquals("me", fdir1.object());
					//should be direct not over relay
					Assert.assertEquals(0, BaseRelayServer.messageCounter());
					return "done";
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"), (Peer)get("p2"));
				}
			});
			unr1.waitFor();
			Assert.assertEquals("done", unr1.getResult(0));
		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(unr1);
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
			PeerNAT rn1 = new PeerBuilderNAT(relayPeer).start();
			final PeerSocketAddress relayAddress = relayPeer.peerAddress().peerSocketAddress();
			
			unr1 = LocalNATUtils.executePeer(0, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.0.2", 5000, 0);
					Peer peer2 = LocalNATUtils.init("10.0.0.3", 5001, 1);
					put("p1", peer1);
					put("p2", peer2);
					
					peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					peer2.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					
					final CountDownLatch cl = new CountDownLatch(2);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(new RelayCallback() {
						@Override
						public void onRelayRemoved(PeerAddress candidate, PeerConnection object) {}
						@Override
						public void onRelayAdded(PeerAddress candidate, PeerConnection object) {cl.countDown();}
					}).start();
					PeerNAT pn2 = new PeerBuilderNAT(peer2).relayCallback(new RelayCallback() {
						@Override
						public void onRelayRemoved(PeerAddress candidate, PeerConnection object) {}
						@Override
						public void onRelayAdded(PeerAddress candidate, PeerConnection object) {cl.countDown();}
					}).start();
					//setup relay
					pn1.startRelay();
					pn2.startRelay();
					cl.await();
					
					//send message from p1 to p2
					peer2.objectDataReply(new ObjectDataReply() {
						@Override
						public Object reply(PeerAddress sender, Object request) throws Exception {
							return "me";
						}
					});
					FutureDirect fdir1 = peer1.sendDirect(peer2.peerAddress()).object("test").start().awaitUninterruptibly();
					Assert.assertEquals("me", fdir1.object());
					//should be direct not over relay
					Assert.assertEquals(0, BaseRelayServer.messageCounter());
					return "done";
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"), (Peer)get("p2"));
				}
			});
			unr1.waitFor();
			Assert.assertEquals("done", unr1.getResult(0));
		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(relayPeer);
			System.out.print(".");
			LocalNATUtils.shutdown(unr1);
			System.out.println(".");
		}
	}
}
