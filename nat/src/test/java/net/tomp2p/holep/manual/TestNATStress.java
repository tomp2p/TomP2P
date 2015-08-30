package net.tomp2p.holep.manual;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.nat.FutureNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.relay.RelayCallback;

/**
 * 2 UPNP peers behind same NAT, 1 behind other NAT (total 3 peers) 2 port
 * forwarding peers behind same NAT, 1 behind other NAT (total 3 peers) 2
 * relayed peers behind same NAT, 1 behind other NAT (total 3 peers)
 * 
 * In total 9 peers (without relays)
 * 
 * @author Thomas Bocek
 *
 */
public class TestNATStress implements Serializable {

	final static private Random RND = new Random(42);
	static private Number160 relayPeerId1 = new Number160(RND);
	static private Number160 relayPeerId2 = new Number160(RND);
	// ### CHANGE THIS TO YOUR INTERFACE###
	final static private String INF = "eth1";

	@Before
	public void before() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("start", "0");
		LocalNATUtils.executeNatSetup("start", "1");
		LocalNATUtils.executeNatSetup("upnp", "0");
		LocalNATUtils.executeNatSetup("upnp", "1");
		LocalNATUtils.executeNatSetup("start", "2", "sym");
		LocalNATUtils.executeNatSetup("start", "3", "sym");
		LocalNATUtils.executeNatSetup("forward", "2", "4000", "10.0.2.2", "5000");
		LocalNATUtils.executeNatSetup("forward", "2", "4001", "10.0.2.3", "5000");
		LocalNATUtils.executeNatSetup("forward", "3", "4000", "10.0.3.2", "5000");
		LocalNATUtils.executeNatSetup("start", "4", "sym");
		LocalNATUtils.executeNatSetup("start", "5", "sym");
	}

	@After
	public void after() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("stop", "0");
		LocalNATUtils.executeNatSetup("stop", "1");
		LocalNATUtils.executeNatSetup("stop", "2");
		LocalNATUtils.executeNatSetup("stop", "3");
		LocalNATUtils.executeNatSetup("stop", "4");
		LocalNATUtils.executeNatSetup("stop", "5");
	}

	@Test
	public void testStress() throws Exception {
		Peer relayPeer1 = null;
		Peer relayPeer2 = null;

		PeerDHT pd = null;
		RemotePeer unr[] = new RemotePeer[9];
		CommandSync sync = new CommandSync(9);

		try {
			relayPeer1 = createRelay(relayPeerId1, 5002);
			final Peer relayPeer11 = relayPeer1;
			relayPeer2 = createRelay(relayPeerId2, 5003);
			final PeerSocketAddress relayAddress1 = relayPeer1.peerAddress().peerSocketAddress();
			final PeerSocketAddress relayAddress2 = relayPeer2.peerAddress().peerSocketAddress();
			
			System.out.println("relay 1:"+relayPeer1.peerAddress());
			System.out.println("relay 2:"+relayPeer2.peerAddress());

			unr[0] = LocalNATUtils.executePeer(0, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.0.2", 5000, 0);
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress1).start();
					PeerNAT pn = new PeerBuilderNAT(peer1).start();
					FutureNAT fn = pn.portForwarding(fd1).awaitUninterruptibly();
					return fn.isSuccess();
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr[1] = LocalNATUtils.executePeer(0, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.0.3", 5000, 1);
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress1).start();
					PeerNAT pn = new PeerBuilderNAT(peer1).start();
					FutureNAT fn = pn.portForwarding(fd1).awaitUninterruptibly();
					return fn.isSuccess();
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr[2] = LocalNATUtils.executePeer(1, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.1.2", 5000, 2);
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress1).start();
					PeerNAT pn = new PeerBuilderNAT(peer1).start();
					FutureNAT fn = pn.portForwarding(fd1).awaitUninterruptibly();
					return fn.isSuccess();
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr[3] = LocalNATUtils.executePeer(2, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.2.2", 5000, 3, 4000);
					put("p1", peer1);
					FutureDiscover fd = peer1.discover().peerSocketAddress(relayAddress1).start().awaitUninterruptibly();
					return fd.isSuccess();
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr[4] = LocalNATUtils.executePeer(2, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.2.3", 5000, 4, 4001);
					put("p1", peer1);
					FutureDiscover fd = peer1.discover().peerSocketAddress(relayAddress1).start().awaitUninterruptibly();
					return fd.isSuccess();
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr[5] = LocalNATUtils.executePeer(3, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.3.2", 5000, 5, 4000);
					put("p1", peer1);
					FutureDiscover fd = peer1.discover().peerSocketAddress(relayAddress1).start().awaitUninterruptibly();
					return fd.isSuccess();
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr[6] = LocalNATUtils.executePeer(4, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.4.2", 5000, 6);
					put("p1", peer1);
					
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress1).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					
					final CountDownLatch cl1 = new CountDownLatch(1);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(countDownRelayCallback(cl1)).start();
					//setup relay
					pn1.startRelay();
					cl1.await();
					Thread.sleep(20*1000);
					return "true";
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr[7] = LocalNATUtils.executePeer(4, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.4.3", 5000, 7);
					put("p1", peer1);
					
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress1).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					
					final CountDownLatch cl1 = new CountDownLatch(1);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(countDownRelayCallback(cl1)).start();
					//setup relay
					pn1.startRelay();
					cl1.await();
					return "true";
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			unr[8] = LocalNATUtils.executePeer(5, sync, new Command() {

				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.5.2", 5000, 8);
					put("p1", peer1);
					
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress1).start().awaitUninterruptibly();
					Assert.assertFalse(fd1.isDiscoveredTCP());
					
					final CountDownLatch cl1 = new CountDownLatch(1);
					PeerNAT pn1 = new PeerBuilderNAT(peer1).relayCallback(countDownRelayCallback(cl1)).start();
					//setup relay
					pn1.startRelay();
					cl1.await();
					return "true";
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"));
				}
			});
			
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					for(;;) {
						System.err.println("MAP SIZE: "+relayPeer11.peerBean().peerMap().all().size());
						for(PeerAddress pa:relayPeer11.peerBean().peerMap().all()) {
							System.err.println("PA: "+pa);
						}
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
					
				}
			}).start();

			for (RemotePeer u : unr) {
				u.waitFor();
			}

			for (RemotePeer u : unr) {
				for (int i = 0; i < u.resultSize(); i++) {
					Assert.assertEquals("true", u.getResult(i));
				}
			}

		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(relayPeer1, relayPeer2);
			System.out.print(".");
			LocalNATUtils.shutdown(unr);
			System.out.println(".");
		}
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
	
	private Peer createRelay(Number160 relayPeerId, int port) throws Exception {
		Peer relayPeer = LocalNATUtils.createRealNode(relayPeerId, INF, port);
		new PeerBuilderNAT(relayPeer).start();
		return relayPeer;
	}
}
