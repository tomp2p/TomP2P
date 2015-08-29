package net.tomp2p.holep.manual;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

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
import net.tomp2p.relay.Forwarder;
import net.tomp2p.relay.RelayCallback;
import net.tomp2p.rpc.ObjectDataReply;

//@Ignore
public class TestNATRelay implements Serializable {

	private static final long serialVersionUID = 1L;
	final static private Random RND = new Random(42);
	static private Number160 relayPeerId1 = new Number160(RND);
	static private Number160 relayPeerId2 = new Number160(RND);
	static private Number160 relayPeerId3 = new Number160(RND);
	static private Number160 relayPeerId4 = new Number160(RND);
	static private Number160 relayPeerId5 = new Number160(RND);
	//### CHANGE THIS TO YOUR INTERFACE###
	final static private String INF = "enp0s25";

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
	
	/**
	 * Test if a relay goes offline to contact a new relay. The following cases are tested:
	 * 
	 * 1 relay, 1 goes offline, 1 new relay joins, find new relay, add this relay. This test is time sensitive.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRelayFailover1() throws Exception {
		Peer relayPeer1 = null;
		Peer relayPeer2 = null;
		RemotePeer unr1 = null;
		try {
			relayPeer1 = createRelay(relayPeerId1, 5002);
			relayPeer2 = createRelay(relayPeerId2, 5003);
			final PeerSocketAddress relayAddress1 = relayPeer1.peerAddress().peerSocketAddress();
			final PeerSocketAddress relayAddress2 = relayPeer2.peerAddress().peerSocketAddress();
			
			final Peer relayPeer1Copy = relayPeer1;
			CommandSync sync = new CommandSync(1);
			
			unr1 = LocalNATUtils.executePeer(0, new RemotePeerCallback() {
				
				@Override
				public void onNull(int i) {}
				
				@Override
				public void onFinished(int i) {
					if(i==0) {
						//shutdown relay1
						System.out.println("go offline!");
						relayPeer1Copy.shutdown().awaitUninterruptibly();
					}
				}
			}, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.0.2", 5000, 0, "me1");
					put("p1", peer1);
					
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress1).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					
					final CountDownLatch cl1 = new CountDownLatch(1);
					final CountDownLatch cl2 = new CountDownLatch(1);
					put("cl2", cl2);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(countDownRelayCallback2(cl1, cl2)).start();
					//setup relay
					pn1.startRelay();
					cl1.await();
					//make sure no further relays are searched in the next 5 sec. Check manually!
					
					return "shutdown relay1";
				}

				
				
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					final Peer peer1 = (Peer) get("p1");
					//discover the 2nd relay
					peer1.discover().peerSocketAddress(relayAddress2).start().awaitUninterruptibly();
					System.out.println("now we know peer realy2 ");
					final CountDownLatch cl2 = (CountDownLatch) get("cl2");
					cl2.await();
					return "done";
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					System.err.println("shutdown0");
					Thread.sleep(500);
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr1.waitFor();
			Assert.assertEquals("done", unr1.getResult(1));
			
			} finally {
				System.out.print("LOCAL> shutdown.");
				LocalNATUtils.shutdown(relayPeer1, relayPeer2);
				System.out.print(".");
				LocalNATUtils.shutdown(unr1);
				System.out.println(".");
			}
	}
	
	/**
	 * Test if a relay goes offline to contact a new relay. The following cases are tested:
	 * 
	 * 2 relays, 1 goes offline, switch to the other one
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRelayFailover2() throws Exception {
		Peer relayPeer1 = null;
		Peer relayPeer2 = null;
		RemotePeer unr1 = null;
		try {
			relayPeer1 = createRelay(relayPeerId1, 5002);
			final PeerSocketAddress relayAddress1 = relayPeer1.peerAddress().peerSocketAddress();
			relayPeer2 = createRelay(relayPeerId2, 5003);
			final PeerSocketAddress relayAddress2 = relayPeer2.peerAddress().peerSocketAddress();
			
			final Peer relayPeer1Copy = relayPeer1;
			final Peer relayPeer2Copy = relayPeer2;
			final AtomicBoolean test = new AtomicBoolean(false);
			CommandSync sync = new CommandSync(1);
			unr1 = LocalNATUtils.executePeer(0, new RemotePeerCallback() {
				
				@Override
				public void onNull(int i) {}
				
				@Override
				public void onFinished(int i) {
					if(i==0) {
						//shutdown relay1
						System.out.println("go offline!");
						relayPeer1Copy.shutdown().awaitUninterruptibly();
					} else if(i==1) {
						Forwarder fw = relayPeer2Copy.connectionBean().dispatcher().searchHandler(Forwarder.class, relayPeer2Copy.peerID() , Number160.createHash(0));
						System.out.println("SIIIZE: " + fw.getPeerMap().size());
						test.set(fw.getPeerMap().size() == 1);
					}
					
				}
			}, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.0.2", 5000, 0, "me1");
					put("p1", peer1);
					
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress1).start().awaitUninterruptibly();
					FutureDiscover fd2 = peer1.discover().peerSocketAddress(relayAddress2).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					Assert.assertFalse(fd2.isDiscoveredTCP());
					
					final CountDownLatch cl = new CountDownLatch(2);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(countDownRelayCallback(cl)).start();
					//setup relay
					pn1.startRelay();
					cl.await();
					//make sure no further relays are searched in the next 5 sec. Check manually!
					
					return "shutdown relay1";
				}

				
				
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					System.out.println("wait 5 sec");
					Thread.sleep(7000);
					System.out.println("done wait 5 sec");
					//now relay1 is shutdown, check if we updated our data
					return "check relay2";
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					System.err.println("shutdown0");
					Thread.sleep(500);
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr1.waitFor();
			Assert.assertTrue(test.get());
			
		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(relayPeer1, relayPeer2);
			System.out.print(".");
			LocalNATUtils.shutdown(unr1);
			System.out.println(".");
		}
	}
	
	/**
	 * Test if we wait if we have all relay peers
	 * @throws Exception
	 */
	@Test
	public void testFullRelay() throws Exception {
		Peer relayPeer1 = null;
		Peer relayPeer2 = null;
		Peer relayPeer3 = null;
		Peer relayPeer4 = null;
		Peer relayPeer5 = null;
		RemotePeer unr1 = null;
		try {
			relayPeer1 = createRelay(relayPeerId1, 5002);
			final PeerSocketAddress relayAddress1 = relayPeer1.peerAddress().peerSocketAddress();
			relayPeer2 = createRelay(relayPeerId2, 5003);
			final PeerSocketAddress relayAddress2 = relayPeer2.peerAddress().peerSocketAddress();
			relayPeer3 = createRelay(relayPeerId3, 5004);
			final PeerSocketAddress relayAddress3 = relayPeer3.peerAddress().peerSocketAddress();
			relayPeer4 = createRelay(relayPeerId4, 5005);
			final PeerSocketAddress relayAddress4 = relayPeer4.peerAddress().peerSocketAddress();
			relayPeer5 = createRelay(relayPeerId5, 5006);
			final PeerSocketAddress relayAddress5 = relayPeer5.peerAddress().peerSocketAddress();
			CommandSync sync = new CommandSync(1);
			unr1 = LocalNATUtils.executePeer(0, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.0.2", 5000, 0, "me1");
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress1).start().awaitUninterruptibly();
					FutureDiscover fd2 = peer1.discover().peerSocketAddress(relayAddress2).start().awaitUninterruptibly();
					FutureDiscover fd3 = peer1.discover().peerSocketAddress(relayAddress3).start().awaitUninterruptibly();
					FutureDiscover fd4 = peer1.discover().peerSocketAddress(relayAddress4).start().awaitUninterruptibly();
					FutureDiscover fd5 = peer1.discover().peerSocketAddress(relayAddress5).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					Assert.assertFalse(fd2.isDiscoveredTCP());
					Assert.assertFalse(fd3.isDiscoveredTCP());
					Assert.assertFalse(fd4.isDiscoveredTCP());
					Assert.assertFalse(fd5.isDiscoveredTCP());
					
					
					final CountDownLatch cl = new CountDownLatch(1);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(countDownRelayCallbackFull(cl)).start();
					//setup relay
					pn1.startRelay();
					cl.await();
					//make sure no further relays are searched in the next 5 sec. Check manually!
					Thread.sleep(5000);
					return "done";
				}

				
				
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					System.err.println("shutdown0");
					Thread.sleep(500);
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr1.waitFor();
			Assert.assertEquals("done", unr1.getResult(0));
			
		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(relayPeer1, relayPeer2, relayPeer3, relayPeer4, relayPeer5);
			System.out.print(".");
			LocalNATUtils.shutdown(unr1);
			System.out.println(".");
		}
	}
	
	/**
	 * Test if a new relay that was discovered is added
	 * @throws Exception
	 */
	@Test
	public void testLateRelay() throws Exception {
		Peer relayPeer1 = null;
		Peer relayPeer2 = null;
		RemotePeer unr1 = null;
		try {
			relayPeer1 = createRelay(relayPeerId1, 5002);
			final PeerSocketAddress relayAddress1 = relayPeer1.peerAddress().peerSocketAddress();
			relayPeer2 = createRelay(relayPeerId2, 5003);
			final PeerSocketAddress relayAddress2 = relayPeer2.peerAddress().peerSocketAddress();
			CommandSync sync = new CommandSync(1);
			
			unr1 = LocalNATUtils.executePeer(0, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.0.2", 5000, 0, "me1");
					put("p1", peer1);
					//get to know both relays
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress1).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					
					
					final CountDownLatch cl = new CountDownLatch(2);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(countDownRelayCallback(cl)).start();
					//setup relay
					
					pn1.startRelay();
					Thread.sleep(500);
					FutureDiscover fd2 = peer1.discover().peerSocketAddress(relayAddress2).start().awaitUninterruptibly();
					Assert.assertFalse(fd2.isDiscoveredTCP());
					
					cl.await();
					
					return "done";
				}

				
				
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					System.err.println("shutdown0");
					Thread.sleep(500);
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr1.waitFor();
			Assert.assertEquals("done", unr1.getResult(0));
			
		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(relayPeer1, relayPeer2);
			System.out.print(".");
			LocalNATUtils.shutdown(unr1);
			System.out.println(".");
		}
	}

	private Peer createRelay(Number160 relayPeerId, int port) throws Exception {
		Peer relayPeer = LocalNATUtils.createRealNode(relayPeerId, INF, port);
		new PeerBuilderNAT(relayPeer).start();
		return relayPeer;
	}
	
	private RelayCallback countDownRelayCallback(
			final CountDownLatch cl) {
		return new RelayCallback() {
			@Override
			public void onRelayRemoved(PeerAddress candidate, PeerConnection object) {}
			@Override
			public void onRelayAdded(PeerAddress candidate, PeerConnection object) {cl.countDown();}
			@Override
			public void onFailure(Exception e) {e.printStackTrace();}
			@Override
			public void onFullRelays(int activeRelays) {}
			@Override
			public void onNoMoreRelays(int activeRelays) {}
			@Override
			public void onShutdown() {}
		};
	}
	private RelayCallback countDownRelayCallback2(
			final CountDownLatch cl1, final CountDownLatch cl2) {
		return new RelayCallback() {
			@Override
			public void onRelayRemoved(PeerAddress candidate, PeerConnection object) {}
			@Override
			public void onRelayAdded(PeerAddress candidate, PeerConnection object) {
				if(cl1.getCount() > 0 ) {
					cl1.countDown();
					} else {
						cl2.countDown();
					}
				}
			@Override
			public void onFailure(Exception e) {e.printStackTrace();}
			@Override
			public void onFullRelays(int activeRelays) {}
			@Override
			public void onNoMoreRelays(int activeRelays) {}
			@Override
			public void onShutdown() {}
		};
	}
	
	private RelayCallback countDownRelayCallbackFull(
			final CountDownLatch cl) {
		return new RelayCallback() {
			@Override
			public void onRelayRemoved(PeerAddress candidate, PeerConnection object) {}
			@Override
			public void onRelayAdded(PeerAddress candidate, PeerConnection object) {}
			@Override
			public void onFailure(Exception e) {e.printStackTrace();}
			@Override
			public void onFullRelays(int activeRelays) {cl.countDown();}
			@Override
			public void onNoMoreRelays(int activeRelays) {}
			@Override
			public void onShutdown() {}
		};
	}
	
	
	@SuppressWarnings("serial")
	@Test
	public void testRealRelayDifferentNAT() throws Exception {
		Peer relayPeer = null;
		RemotePeer unr1 = null;
		RemotePeer unr2 = null;
		try {
			relayPeer = createRelay(relayPeerId1, 5002);
			final PeerSocketAddress relayAddress = relayPeer.peerAddress().peerSocketAddress();
			CommandSync sync = new CommandSync(2);
			
			unr1 = LocalNATUtils.executePeer(0, sync, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.0.2", 5000, 0, "me1");
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					final CountDownLatch cl = new CountDownLatch(1);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(countDownRelayCallback(cl)).start();
					//setup relay
					
					pn1.startRelay();
					cl.await();
					Thread.sleep(500);

					PeerAddress peer2 = LocalNATUtils.peerAddress("10.0.1.2", 5000, 1);
					Collection<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>();
					psa.add(relayAddress);
					peer2 = peer2.changePeerSocketAddresses(psa);
					peer2 = peer2.changeFirewalledTCP(true).changeFirewalledUDP(true).changeRelayed(true);
					FutureDirect fdir1 = peer1.sendDirect(peer2).object("test").start().awaitUninterruptibly();
					System.out.println(fdir1.failedReason());
					Assert.assertTrue(fdir1.isSuccess());
					String result = fdir1.object().toString();
					System.out.println("DONE1" + result);
					return "me2".equals(result) ? "TRUE" : "FALSE";
				}

				
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					System.err.println("shutdown0");
					Thread.sleep(500);
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			
			unr2 = LocalNATUtils.executePeer(1, sync, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.1.2", 5000, 1, "me2");
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					final CountDownLatch cl = new CountDownLatch(1);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(countDownRelayCallback(cl)).start();
					//setup relay
					
					pn1.startRelay();
					cl.await();
					Thread.sleep(500);	

					PeerAddress peer2 = LocalNATUtils.peerAddress("10.0.0.2", 5000, 0);
					Collection<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>();
					psa.add(relayAddress);
					peer2 = peer2.changePeerSocketAddresses(psa);
					peer2 = peer2.changeFirewalledTCP(true).changeFirewalledUDP(true).changeRelayed(true);
					FutureDirect fdir1 = peer1.sendDirect(peer2).object("test").start().awaitUninterruptibly();
					System.out.println(fdir1.failedReason());
					Assert.assertTrue(fdir1.isSuccess());
					String result = fdir1.object().toString();
					System.out.println("DONE2 " + result);
					return "me1".equals(result) ? "TRUE" : "FALSE";
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					System.err.println("shutdown1");
					Thread.sleep(500);
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
			CommandSync sync = new CommandSync(1);
			unr1 = LocalNATUtils.executePeer(0, sync, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.0.2", 5000, 0, "n/a");
					put("p1", peer1);
					
					Peer peer2 = LocalNATUtils.createNattedPeer("10.0.0.3", 5001, 1, "me");
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
			relayPeer = createRelay(relayPeerId1, 5002);
			final PeerSocketAddress relayAddress = relayPeer.peerAddress().peerSocketAddress();
			CommandSync sync = new CommandSync(1);
			unr1 = LocalNATUtils.executePeer(0, sync, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.0.2", 5000, 0, "n/a");
					put("p1", peer1);
					
					Peer peer2 = LocalNATUtils.createNattedPeer("10.0.0.3", 5001, 1, "me");
					put("p2", peer2);
					
					peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					peer2.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					
					final CountDownLatch cl = new CountDownLatch(2);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(countDownRelayCallback(cl)).start();
					PeerNAT pn2 = new PeerBuilderNAT(peer2).relayCallback(countDownRelayCallback(cl)).start();
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
