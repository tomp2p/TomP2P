package net.tomp2p.relay;

import java.net.InetAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.PutBuilder;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.message.Message;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.relay.android.AndroidForwarderRPC;
import net.tomp2p.relay.android.GCMServerCredentials;
import net.tomp2p.relay.android.MessageBufferConfiguration;
import net.tomp2p.relay.android.MessageBufferListener;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRelay {

	private static final long DEFAULT_GCM_MOCK_DELAY_MS = 100;
	private static final GCMServerCredentials gcmServerCredentials = new GCMServerCredentials().registrationId("dummy-registration-id").senderAuthenticationKey("dummy-auth-key").senderId(12345l);
	
	private final RelayConfig relayConfig;
	private long gcmMockDelayMS;

	private final MessageBufferConfiguration bufferConfig;


	@SuppressWarnings("rawtypes")
	@Parameterized.Parameters(name = "{0}")
	public static Collection data() {
		return Arrays.asList(new Object[][] { { RelayConfig.OpenTCP() }, { RelayConfig.Android(gcmServerCredentials) } });
	}
	
	public TestRelay(RelayConfig relayType) {
		this.relayConfig = relayType;
		this.gcmMockDelayMS = DEFAULT_GCM_MOCK_DELAY_MS;
		
		// create objects required for android
		this.bufferConfig = new MessageBufferConfiguration().bufferCountLimit(1);
	}
	
	/**
	 * Mocks the GCM functionality, making it able to test without an Android device
	 */
	private void mockGCM(Peer[] peers, final PeerNAT unreachablePeer) {
		if(relayConfig.type() != RelayType.ANDROID) {
			// nothing to do
			return;
		}
		
		// get all forwarders and add another listener
		Set<AndroidForwarderRPC> mockedForwarders = new HashSet<AndroidForwarderRPC>();
		for (Peer peer : peers) {
			Map<Integer, DispatchHandler> handlers = peer.connectionBean().dispatcher()
					.searchHandlerMap(peer.peerID(), unreachablePeer.peer().peerID());
			if (handlers == null) {
				continue;
			}

			for (Entry<Integer, DispatchHandler> entry : handlers.entrySet()) {
				if (entry.getValue() instanceof AndroidForwarderRPC && !mockedForwarders.contains(entry.getValue())) {
					final AndroidForwarderRPC forwarderRPC = (AndroidForwarderRPC) entry.getValue();
					// make sure every forwarder only has one of these listeners
					forwarderRPC.addBufferListener(new MessageBufferListener<Message>() {

						@Override
						public void bufferFull(List<Message> bufferedMessages) {
							System.err.println("Caught sending message over GCM from " + forwarderRPC.relayPeerId() + " to unreachable peer");
							try {
								Thread.sleep(gcmMockDelayMS);
							} catch (InterruptedException e) {
								// ignore
							}

							// start in a new thread
							new Thread(new Runnable() {
								@Override
								public void run() {
									unreachablePeer.onGCMMessageArrival(forwarderRPC.relayPeerId().toString());
								}
							}, "GCM-Mock").start();
						}
					});
					
					System.err.println("Mocked Android forwarder at " + forwarderRPC.relayPeerId() + " to " + forwarderRPC.unreachablePeerId());
					mockedForwarders.add(forwarderRPC);
				}
			}
		}
	}
	
	private void mockGCM(PeerDHT[] peersDHT, PeerNAT unreachablePeer) {
		Peer[] peers = new Peer[peersDHT.length];
		for (int i = 0; i < peersDHT.length; i++) {
			peers[i] = peersDHT[i].peer();
		}
		mockGCM(peers, unreachablePeer);
	}
	
	@Test
	public void testSetupRelayPeers() throws Exception {
		final Random rnd = new Random(42);
		final int nrOfNodes = 200;
		Peer master = null;
		Peer unreachablePeer = null;
		try {
			// setup test peers
			Peer[] peers = UtilsNAT.createNodes(nrOfNodes, rnd, 4001);
			master = peers[0];
			UtilsNAT.perfectRouting(peers);
			for (Peer peer : peers) {
				new PeerBuilderNAT(peer).bufferConfiguration(bufferConfig).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(5000).start();

			// find neighbors
			FutureBootstrap futureBootstrap = unreachablePeer.bootstrap().peerAddress(peers[0].peerAddress()).start();
			futureBootstrap.awaitUninterruptibly();
			Assert.assertTrue(futureBootstrap.isSuccess());

			// setup relay
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
			FutureRelayNAT startRelay = uNat.startRelay(relayConfig, peers[0].peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(startRelay.isSuccess());
			mockGCM(peers, uNat);

			// Check if flags are set correctly
			Assert.assertTrue(unreachablePeer.peerAddress().isRelayed());
			Assert.assertFalse(unreachablePeer.peerAddress().isFirewalledTCP());
			Assert.assertFalse(unreachablePeer.peerAddress().isFirewalledUDP());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
			if (unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
		}
	}

	@Test
	public void testBoostrap() throws Exception {
		final Random rnd = new Random(42);
		final int nrOfNodes = 10;
		Peer master = null;
		Peer unreachablePeer = null;
		try {
			// setup test peers
			Peer[] peers = UtilsNAT.createNodes(nrOfNodes, rnd, 4001);
			master = peers[0];
			UtilsNAT.perfectRouting(peers);
			for (Peer peer : peers) {
				new PeerBuilderNAT(peer).bufferConfiguration(bufferConfig).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(5000).start();
			PeerAddress upa = unreachablePeer.peerBean().serverPeerAddress();
			upa = upa.changeFirewalledTCP(true).changeFirewalledUDP(true);
			unreachablePeer.peerBean().serverPeerAddress(upa);

			// find neighbors
			FutureBootstrap futureBootstrap = unreachablePeer.bootstrap().peerAddress(peers[0].peerAddress()).start();
			futureBootstrap.awaitUninterruptibly();
			Assert.assertTrue(futureBootstrap.isSuccess());

			// setup relay
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
			FutureRelayNAT startRelay = uNat.startRelay(relayConfig.peerMapUpdateInterval(5), peers[0].peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(startRelay.isSuccess());
			mockGCM(peers, uNat);

			// find neighbors again
			futureBootstrap = unreachablePeer.bootstrap().peerAddress(peers[0].peerAddress()).start();
			futureBootstrap.awaitUninterruptibly();
			Assert.assertTrue(futureBootstrap.isSuccess());

			boolean otherPeersHaveRelay = false;

			for (Peer peer : peers) {
				if (peer.peerBean().peerMap().allOverflow().contains(unreachablePeer.peerAddress())) {
					for (PeerAddress pa : peer.peerBean().peerMap().allOverflow()) {
						if (pa.peerId().equals(unreachablePeer.peerID())) {
							if (pa.peerSocketAddresses().size() > 0) {
								otherPeersHaveRelay = true;
							}
							System.err.println("-->" + pa.peerSocketAddresses());
							System.err.println("relay=" + pa.isRelayed());
						}
					}
					System.err.println("check 1! " + peer.peerAddress());
				}

			}
			Assert.assertTrue(otherPeersHaveRelay);

			// wait for maintenance
			Thread.sleep(relayConfig.peerMapUpdateInterval() * 1000);

			boolean otherPeersMe = false;
			for (Peer peer : peers) {
				if (peer.peerBean().peerMap().all().contains(unreachablePeer.peerAddress())) {
					System.err.println("check 2! " + peer.peerAddress());
					otherPeersMe = true;
				}
			}
			Assert.assertTrue(otherPeersMe);
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
			if (unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
		}
	}

	/**
	 * Tests sending a message from an unreachable peer to another unreachable peer
	 */
	@Test
	public void testRelaySendDirect() throws Exception {
		final Random rnd = new Random(42);
		final int nrOfNodes = 100;
		Peer master = null;
		Peer unreachablePeer1 = null;
		Peer unreachablePeer2 = null;
		try {
			// setup test peers
			Peer[] peers = UtilsNAT.createNodes(nrOfNodes, rnd, 4001);
			master = peers[0];
			UtilsNAT.perfectRouting(peers);
			for (Peer peer : peers) {
				new PeerBuilderNAT(peer).bufferConfiguration(bufferConfig).start();
			}

			// Test setting up relay peers
			unreachablePeer1 = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start();
			PeerNAT uNat1 = new PeerBuilderNAT(unreachablePeer1).start();
			FutureRelayNAT fbn = uNat1.startRelay(relayConfig, master.peerAddress());
			fbn.awaitUninterruptibly();
			Assert.assertTrue(fbn.isSuccess());
			mockGCM(peers, uNat1);

         	System.out.print("Send direct message to unreachable peer " + unreachablePeer1.peerAddress());
            final String request = "Hello ";
            final String response = "World!";
            
            final AtomicBoolean test1 = new AtomicBoolean(false);
            final AtomicBoolean test2 = new AtomicBoolean(false);
            
            //final Peer unr = unreachablePeer;
            unreachablePeer1.objectDataReply(new ObjectDataReply() {
                public Object reply(PeerAddress sender, Object obj) throws Exception {
                	test1.set(obj.equals(request));
                    Assert.assertEquals(request.toString(), request);
                    test2.set(sender.inetAddress().toString().contains("0.0.0.0"));
                    System.err.println("Got sender:"+sender);
                    
                    //this is too late here, so we cannot test this here
                    //Collection<PeerSocketAddress> list = new ArrayList<PeerSocketAddress>();
                    //list.add(new PeerSocketAddress(InetAddress.getByName("101.101.101.101"), 101, 101));
                    //unr.peerBean().serverPeerAddress(unr.peerBean().serverPeerAddress().changePeerSocketAddresses(list));
                    
                    return response;
                }
            });
            
            
            unreachablePeer2 = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13338).start();
			PeerNAT uNat2 = new PeerBuilderNAT(unreachablePeer2).start();
			fbn = uNat2.startRelay(relayConfig, peers[42].peerAddress());

			
			fbn.awaitUninterruptibly();
			Assert.assertTrue(fbn.isSuccess());
			mockGCM(peers, uNat2);

			//prevent rcon
			Collection<PeerSocketAddress> list = unreachablePeer2.peerBean().serverPeerAddress().peerSocketAddresses();
			if(list.size() >= relayConfig.type().maxRelayCount()) {
				Iterator<PeerSocketAddress> iterator = list.iterator();
				iterator.next();
				iterator.remove();
			}
			list.add(new PeerSocketAddress(InetAddress.getByName("10.10.10.10"), 10, 10));
			unreachablePeer2.peerBean().serverPeerAddress(unreachablePeer2.peerBean().serverPeerAddress().changePeerSocketAddresses(list));
            
			System.err.println("unreachablePeer1: " + unreachablePeer1.peerAddress());
			System.err.println("unreachablePeer2: "+unreachablePeer2.peerAddress());
			
            FutureDirect fd = unreachablePeer2.sendDirect(unreachablePeer1.peerAddress()).object(request).start().awaitUninterruptibly();
            System.err.println("got msg from: "+fd.futureResponse().responseMessage().sender());
            Assert.assertEquals(response, fd.object());
            //make sure we did not receive it from the unreachable peer with port 13337
            //System.err.println(fd.getWrappedFuture());
            //TODO: this case is true for relay
            //Assert.assertEquals(fd.wrappedFuture().responseMessage().senderSocket().getPort(), 4001);
            //TODO: this case is true for rcon
            Assert.assertEquals(unreachablePeer1.peerID(), fd.wrappedFuture().responseMessage().sender().peerId());
            
            Assert.assertTrue(test1.get());
            Assert.assertFalse(test2.get());
            Assert.assertEquals(relayConfig.type().maxRelayCount(), fd.futureResponse().responseMessage().sender().peerSocketAddresses().size());
		} finally {
			if (unreachablePeer1 != null) {
				unreachablePeer1.shutdown().await();
			}
			if (unreachablePeer2 != null) {
				unreachablePeer2.shutdown().await();
			}
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	/**
	 * Tests sending a message from a reachable peer to an unreachable peer
	 */
	@Test
	public void testRelaySendDirect2() throws Exception {
		final Random rnd = new Random(42);
		final int nrOfNodes = 100;
		Peer master = null;
		Peer unreachablePeer = null;
		try {
			// setup test peers
			Peer[] peers = UtilsNAT.createNodes(nrOfNodes, rnd, 4001);
			master = peers[0];
			UtilsNAT.perfectRouting(peers);
			for (Peer peer : peers) {
				new PeerBuilderNAT(peer).bufferConfiguration(bufferConfig).start();
			}

			// setup relay
			unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start();
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
			FutureRelayNAT startRelay = uNat.startRelay(relayConfig, peers[0].peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(startRelay.isSuccess());
			mockGCM(peers, uNat);

			System.out.print("Send direct message to unreachable peer");
			final String request = "Hello ";
			final String response = "World!";

			unreachablePeer.objectDataReply(new ObjectDataReply() {
				public Object reply(PeerAddress sender, Object request) throws Exception {
					Assert.assertEquals(request.toString(), request);
					return response;
				}
			});

			FutureDirect fd = peers[42].sendDirect(unreachablePeer.peerAddress()).object(request).start()
					.awaitUninterruptibly();
			Assert.assertEquals(response, fd.object());
			
			// make sure we did receive it from the unreachable peer with id
			Assert.assertEquals(unreachablePeer.peerID(), fd.wrappedFuture().responseMessage().sender().peerId());
		} finally {
			if (unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	/**
	 * Tests sending a message from an unreachable peer to a reachable peer
	 */
	@Test
	public void testRelaySendDirect3() throws Exception {
		final Random rnd = new Random(42);
		final int nrOfNodes = 100;
		Peer master = null;
		Peer unreachablePeer = null;
		try {
			// setup test peers
			Peer[] peers = UtilsNAT.createNodes(nrOfNodes, rnd, 4001);
			master = peers[0];
			UtilsNAT.perfectRouting(peers);
			for (Peer peer : peers) {
				new PeerBuilderNAT(peer).bufferConfiguration(bufferConfig).start();
			}
			
			// setup relay
			unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start();
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
			FutureRelayNAT startRelay = uNat.startRelay(relayConfig, peers[0].peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(startRelay.isSuccess());
			mockGCM(peers, uNat);

			System.out.print("Send direct message from unreachable peer");
			final String request = "Hello ";
			final String response = "World!";

			Peer receiver = peers[42];
			receiver.objectDataReply(new ObjectDataReply() {
				public Object reply(PeerAddress sender, Object request) throws Exception {
					Assert.assertEquals(request.toString(), request);
					return response;
				}
			});

			FutureDirect fd = unreachablePeer.sendDirect(receiver.peerAddress()).object(request).start()
					.awaitUninterruptibly();
			Assert.assertEquals(response, fd.object());
			
			// make sure we did receive it from the unreachable peer with id
			Assert.assertEquals(receiver.peerID(), fd.wrappedFuture().responseMessage().sender().peerId());
		} finally {
			if (unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testRelayRouting() throws Exception {
		final Random rnd = new Random(42);
		final int nrOfNodes = 8; // test only works if total nr of nodes is < 8
		Peer master = null;
		Peer unreachablePeer = null;
		try {
			// setup test peers
			Peer[] peers = UtilsNAT.createNodes(nrOfNodes, rnd, 4001);
			master = peers[0];
			UtilsNAT.perfectRouting(peers);
			for (Peer peer : peers) {
				new PeerBuilderNAT(peer).bufferConfiguration(bufferConfig).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start();
			PeerAddress upa = unreachablePeer.peerBean().serverPeerAddress();
			upa = upa.changeFirewalledTCP(true).changeFirewalledUDP(true);
			unreachablePeer.peerBean().serverPeerAddress(upa);

			// find neighbors
			FutureBootstrap futureBootstrap = unreachablePeer.bootstrap().peerAddress(peers[0].peerAddress()).start();
			futureBootstrap.awaitUninterruptibly();
			Assert.assertTrue(futureBootstrap.isSuccess());

			// setup relay and lower the update interval to 5s
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
			FutureRelayNAT startRelay = uNat.startRelay(relayConfig.peerMapUpdateInterval(5), peers[0].peerAddress());
			FutureRelay frNAT = startRelay.awaitUninterruptibly().futureRelay();
			Assert.assertTrue(startRelay.isSuccess());
			mockGCM(peers, uNat);

			PeerAddress relayPeer = frNAT.relays().iterator().next().relayAddress();
			Peer found = null;
			for (Peer p : peers) {
				if (p.peerAddress().equals(relayPeer)) {
					found = p;
					break;
				}
			}
			Assert.assertNotNull(found);

			// wait for at least one map update task (5s)
			Thread.sleep(5000);

			int nrOfNeighbors = getNeighbors(found).size();
			// we have in total 9 peers, we should find 8 as neighbors
			Assert.assertEquals(8, nrOfNeighbors);

			System.err.println("neighbors: " + nrOfNeighbors);
			for (BaseRelayConnection relay : frNAT.relays()) {
				System.err.println("pc:" + relay.relayAddress());
			}

			Assert.assertEquals(relayConfig.type().maxRelayCount(), frNAT.relays().size());

			// Shut down a peer
			peers[nrOfNodes - 1].shutdown().await();
			peers[nrOfNodes - 2].shutdown().await();
			peers[nrOfNodes - 3].shutdown().await();

			/*
			 * needed because failure of a node is detected with periodic
			 * heartbeat and the routing table of the relay peers are also
			 * updated periodically
			 */
			Thread.sleep(relayConfig.peerMapUpdateInterval() * 1000);

			Assert.assertEquals(nrOfNeighbors - 3, getNeighbors(found).size());
			Assert.assertEquals(relayConfig.type().maxRelayCount(), frNAT.relays().size());
		} finally {
			if (unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testNoRelayDHT() throws Exception {
		final Random rnd = new Random(42);
		PeerDHT master = null;
		PeerDHT slave = null;
		try {
			PeerDHT[] peers = UtilsNAT.createNodesDHT(10, rnd, 4000);
			master = peers[0]; // the relay peer
			UtilsNAT.perfectRouting(peers);
			for (PeerDHT peer : peers) {
				new PeerBuilderNAT(peer.peer()).bufferConfiguration(bufferConfig).start();
			}
			PeerMapConfiguration pmc = new PeerMapConfiguration(Number160.createHash(rnd.nextInt()));
			slave = new PeerBuilderDHT(new PeerBuilder(Number160.ONE).peerMap(new PeerMap(pmc)).ports(13337).start())
					.start();
			printMapStatus(slave, peers);
			FuturePut futurePut = peers[8].put(slave.peerID()).data(new Data("hello")).start().awaitUninterruptibly();
			futurePut.futureRequests().awaitUninterruptibly();
			Assert.assertTrue(futurePut.isSuccess());
			Assert.assertFalse(slave.storageLayer().contains(
					new Number640(slave.peerID(), Number160.ZERO, Number160.ZERO, Number160.ZERO)));
			System.err.println("DONE!");
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
			if (slave != null) {
				slave.shutdown().await();
			}
		}
	}

	private void printMapStatus(PeerDHT slave, PeerDHT[] peers) {
		for (PeerDHT peer : peers) {
			if (peer.peerBean().peerMap().allOverflow().contains(slave.peerAddress())) {
				System.err.println("found relayed peer in overflow bag " + peer.peerAddress());
			}
		}

		for (PeerDHT peer : peers) {
			if (peer.peerBean().peerMap().all().contains(slave.peerAddress())) {
				System.err.println("found relayed peer in regular bag " + peer.peerAddress());
			}
		}
	}

	@Test
	public void testRelayDHTSimple() throws Exception {
		final Random rnd = new Random(42);
		PeerDHT master = null;
		PeerDHT unreachablePeer = null;
		try {
			PeerDHT[] peers = UtilsNAT.createNodesDHT(1, rnd, 4000);
			master = peers[0]; // the relay peer
			new PeerBuilderNAT(master.peer()).bufferConfiguration(bufferConfig).start();

			// Test setting up relay peers
			unreachablePeer = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start()).start();
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer.peer()).start();
			mockGCM(peers, uNat);

			FutureRelayNAT fbn = uNat.startRelay(relayConfig, master.peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(fbn.isSuccess());

			System.err.println("DONE!");

		} finally {
			if(master != null) {
				master.shutdown().await();
			}
			if(unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
		}
	}

	@Test
	public void testRelayDHT() throws Exception {
		final Random rnd = new Random(42);
		PeerDHT master = null;
		PeerDHT unreachablePeer = null;
		try {
			PeerDHT[] peers = UtilsNAT.createNodesDHT(10, rnd, 4000);
			master = peers[0]; // the relay peer
			UtilsNAT.perfectRouting(peers);
			for (PeerDHT peer : peers) {
				new PeerBuilderNAT(peer.peer()).bufferConfiguration(bufferConfig).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start())
					.start();
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer.peer()).start();

			FutureRelayNAT fbn = uNat.startRelay(relayConfig.peerMapUpdateInterval(3), master.peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(fbn.isSuccess());
			mockGCM(peers, uNat);

			// wait for maintenance to kick in
			Thread.sleep(4000);

			printMapStatus(unreachablePeer, peers);

			FuturePut futurePut = peers[8].put(unreachablePeer.peerID()).data(new Data("hello")).start().awaitUninterruptibly();
			// the relayed one is the slowest, so we need to wait for it!
			futurePut.futureRequests().awaitUninterruptibly();
			System.err.println(futurePut.failedReason());

			Assert.assertTrue(futurePut.isSuccess());
			// we cannot see the peer in futurePut.rawResult, as the relayed is the slowest and we finish
			// earlier than that.
			Assert.assertTrue(unreachablePeer.storageLayer().contains(
					new Number640(unreachablePeer.peerID(), Number160.ZERO, Number160.ZERO, Number160.ZERO)));
			System.err.println("DONE!");

		} finally {
			if (master != null) {
				master.shutdown().await();
			}
			if (unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
		}
	}

	@Test
	public void testRelayDHTPutGet() throws Exception {
		final Random rnd = new Random(42);
		PeerDHT master = null;
		PeerDHT unreachablePeer = null;
		try {
			PeerDHT[] peers = UtilsNAT.createNodesDHT(10, rnd, 4000);
			master = peers[0]; // the relay peer
			UtilsNAT.perfectRouting(peers);
			for (PeerDHT peer : peers) {
				new PeerBuilderNAT(peer.peer()).bufferConfiguration(bufferConfig).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start())
					.start();
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer.peer()).start();

			// bootstrap
			unreachablePeer.peer().bootstrap().peerAddress(master.peerAddress()).start();
			FutureRelayNAT fbn = uNat.startRelay(relayConfig.peerMapUpdateInterval(3), master.peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(fbn.isSuccess());
			mockGCM(peers, uNat);

			// wait for maintenance to kick in
			Thread.sleep(4000);

			printMapStatus(unreachablePeer, peers);

			RoutingConfiguration r = new RoutingConfiguration(5, 1, 1);
			RequestP2PConfiguration rp = new RequestP2PConfiguration(1, 1, 0);

			System.err.println("Unreachable: " + unreachablePeer.peerID());
			System.err.println("Relay: " + master.peerID());

			FuturePut futurePut = peers[8].put(unreachablePeer.peerID()).data(new Data("hello")).routingConfiguration(r)
					.requestP2PConfiguration(rp).start().awaitUninterruptibly();
			// the relayed one is the slowest, so we need to wait for it!
			futurePut.futureRequests().awaitUninterruptibly();
			System.err.println(futurePut.failedReason());

			Assert.assertTrue(futurePut.isSuccess());
			Assert.assertTrue(unreachablePeer.storageLayer().contains(
					new Number640(unreachablePeer.peerID(), Number160.ZERO, Number160.ZERO, Number160.ZERO)));

			FutureGet futureGet = peers[8].get(unreachablePeer.peerID()).routingConfiguration(r).requestP2PConfiguration(rp).start().awaitUninterruptibly();
			Assert.assertTrue(futureGet.isSuccess());

			System.err.println("DONE!");
		} finally {
			if(master != null) {
				master.shutdown().await();
			}
			if(unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
		}
	}

	@Test
	public void testRelayDHTPutGet2() throws Exception {
		final Random rnd = new Random(42);
		PeerDHT master = null;
		PeerDHT unreachablePeer1 = null;
		PeerDHT unreachablePeer2 = null;
		try {
			PeerDHT[] peers = UtilsNAT.createNodesDHT(10, rnd, 4000);
			master = peers[0]; // the relay peer

			for (PeerDHT peer : peers) {
				new PeerBuilderNAT(peer.peer()).bufferConfiguration(bufferConfig).start();
			}

			// Test setting up relay peers
			unreachablePeer1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start()).start();
			PeerNAT uNat1 = new PeerBuilderNAT(unreachablePeer1.peer()).start();
			FutureRelayNAT fbn1 = uNat1.startRelay(relayConfig.peerMapUpdateInterval(3), master.peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(fbn1.isSuccess());

			unreachablePeer2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13338).start()).start();
			PeerNAT uNat2 = new PeerBuilderNAT(unreachablePeer2.peer()).start();
			FutureRelayNAT fbn2 = uNat2.startRelay(relayConfig.peerMapUpdateInterval(3), master.peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(fbn2.isSuccess());

			peers[8] = unreachablePeer1;
			peers[9] = unreachablePeer2;
			UtilsNAT.perfectRouting(peers);
			
			// wait for relay setup
			Thread.sleep(6000);
			
			mockGCM(peers, uNat1);
			mockGCM(peers, uNat2);
			UtilsNAT.perfectRouting(peers);

			// wait for maintenance to kick in
			Thread.sleep(6000);

			printMapStatus(unreachablePeer1, peers);
			printMapStatus(unreachablePeer2, peers);

			RoutingConfiguration r = new RoutingConfiguration(5, 1, 1);
			RequestP2PConfiguration rp = new RequestP2PConfiguration(1, 1, 0);

			System.err.println(unreachablePeer1.peerID()); // f1
			System.err.println(unreachablePeer2.peerID()); // e7

			FuturePut futurePut = unreachablePeer1.put(unreachablePeer2.peerID()).data(new Data("hello"))
					.routingConfiguration(r).requestP2PConfiguration(rp).start().awaitUninterruptibly();
			// the relayed one is the slowest, so we need to wait for it!
			futurePut.futureRequests().awaitUninterruptibly();
			System.err.println(futurePut.failedReason());

			Assert.assertTrue(futurePut.isSuccess());
			Assert.assertTrue(unreachablePeer2.storageLayer().contains(
					new Number640(unreachablePeer2.peerID(), Number160.ZERO, Number160.ZERO, Number160.ZERO)));

			FutureGet futureGet = unreachablePeer1.get(unreachablePeer2.peerID()).routingConfiguration(r)
					.requestP2PConfiguration(rp).fastGet(false).start().awaitUninterruptibly();
			// TODO: try peers even if no data found with fastget
			System.err.println(futureGet.failedReason());
			Assert.assertTrue(futureGet.isSuccess());

			// we cannot see the peer in futurePut.rawResult, as the relayed is the slowest and we finish
			// earlier than that.

			System.err.println("DONE!");

		} finally {
			if(master != null) {
				master.shutdown().await();
			}
			if(unreachablePeer1 != null) {
				unreachablePeer1.shutdown().await();
			}
			if(unreachablePeer2 != null) {
				unreachablePeer2.shutdown().await();
			}
		}
	}

	@Test
    public void testRelayDHTPutGetSigned() throws Exception {
        final Random rnd = new Random(42);
         PeerDHT master = null;
         PeerDHT unreachablePeer1 = null;
         PeerDHT unreachablePeer2 = null;
         try {
        	 PeerDHT[] peers = UtilsNAT.createNodesDHT(10, rnd, 4000);
             master = peers[0]; // the relay peer
            
             for(PeerDHT peer:peers) {
 				new PeerBuilderNAT(peer.peer()).bufferConfiguration(bufferConfig).start();
             }
             
             KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
             KeyPair pair1 = gen.generateKeyPair();
             KeyPair pair2 = gen.generateKeyPair();
             
             // Test setting up relay peers
             unreachablePeer1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).keyPair(pair1).ports(13337).start()).start();
             unreachablePeer1.peer().peerBean().serverPeerAddress(unreachablePeer1.peer().peerAddress().changeFirewalledTCP(true).changeFirewalledUDP(true));
             PeerNAT uNat1 = new PeerBuilderNAT(unreachablePeer1.peer()).start();
             FutureRelayNAT fbn1 = uNat1.startRelay(relayConfig.peerMapUpdateInterval(3), master.peerAddress()).awaitUninterruptibly();
             Assert.assertTrue(fbn1.isSuccess());

             unreachablePeer2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).keyPair(pair2).ports(13338).start()).start();
             unreachablePeer2.peer().peerBean().serverPeerAddress(unreachablePeer2.peer().peerAddress().changeFirewalledTCP(true).changeFirewalledUDP(true));
             PeerNAT uNat2 = new PeerBuilderNAT(unreachablePeer2.peer()).start();
             FutureRelayNAT fbn2 = uNat2.startRelay(relayConfig.peerMapUpdateInterval(3), master.peerAddress()).awaitUninterruptibly();
             Assert.assertTrue(fbn2.isSuccess());

             peers[8] = unreachablePeer1;
 			peers[9] = unreachablePeer2;
 			UtilsNAT.perfectRouting(peers);
 			
 			// wait for relay setup
 			Thread.sleep(4000);
 			
 			mockGCM(peers, uNat1);
 			mockGCM(peers, uNat2);
 			UtilsNAT.perfectRouting(peers);

 			// wait for maintenance to kick in
 			Thread.sleep(4000);
             
             printMapStatus(unreachablePeer1, peers);
             printMapStatus(unreachablePeer2, peers);
             
             RoutingConfiguration r = new RoutingConfiguration(5, 1, 1);
             RequestP2PConfiguration rp = new RequestP2PConfiguration(1, 1, 0);
             
             System.err.println(unreachablePeer1.peerID()); //f1
             System.err.println(unreachablePeer2.peerID()); //e7
             
             FuturePut futurePut = unreachablePeer1.put(unreachablePeer2.peerID()).data(new Data("hello")).sign().routingConfiguration(r).requestP2PConfiguration(rp).start().awaitUninterruptibly();
             //the relayed one is the slowest, so we need to wait for it!
             futurePut.futureRequests().awaitUninterruptibly();
             System.err.println(futurePut.failedReason());
             
             Assert.assertTrue(futurePut.isSuccess());
             Assert.assertTrue(unreachablePeer2.storageLayer().contains(new Number640(unreachablePeer2.peerID(), Number160.ZERO, Number160.ZERO, Number160.ZERO)));
             
             FutureGet futureGet = unreachablePeer1.get(unreachablePeer2.peerID()).routingConfiguration(r).sign().requestP2PConfiguration(rp).fastGet(false).start().awaitUninterruptibly();
             //TODO: try peers even if no data found with fastget
             System.err.println(futureGet.failedReason());
             Assert.assertTrue(futureGet.isSuccess());
             
             //we cannot see the peer in futurePut.rawResult, as the relayed is the slowest and we finish earlier than that.
             
             System.err.println("DONE!");
             
         } finally {
        	 if(master != null) {
 				master.shutdown().await();
 			}
 			if(unreachablePeer1 != null) {
 				unreachablePeer1.shutdown().await();
 			}
 			if(unreachablePeer2 != null) {
 				unreachablePeer2.shutdown().await();
 			}
         }
    }
	

	@Test
	public void testRelaySlowPeer() throws Exception {
		// test is only for slow relay types
		Assume.assumeTrue(relayConfig.type().isSlow());
		
		int slowResponseTimeoutS = 3;
		this.gcmMockDelayMS = 5000;
		
		final Random rnd = new Random(42);
		PeerDHT master = null;
		PeerDHT unreachablePeer = null;
		try {
			PeerDHT[] peers = UtilsNAT.createNodesDHT(10, rnd, 4000);
			master = peers[0]; // the relay peer
			UtilsNAT.perfectRouting(peers);
			for (PeerDHT peer : peers) {
				new PeerBuilderNAT(peer.peer()).bufferConfiguration(bufferConfig).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start()).start();
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer.peer()).start();

			FutureRelayNAT fbn = uNat.startRelay(relayConfig.peerMapUpdateInterval(3), master.peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(fbn.isSuccess());
			mockGCM(peers, uNat);

			// wait for maintenance to kick in
			Thread.sleep(4000);

			RoutingConfiguration r = new RoutingConfiguration(5, 1, 1);
            RequestP2PConfiguration rp = new RequestP2PConfiguration(1, 1, 0);
            
			PutBuilder builder = peers[8].put(unreachablePeer.peerID()).data(new Data("hello")).routingConfiguration(r).requestP2PConfiguration(rp);
			// make the timeout very small, such that the mobile device is too slow to answer the request
			builder.slowResponseTimeoutSeconds(slowResponseTimeoutS);
			FuturePut futurePut = builder.start().awaitUninterruptibly();
			// the relayed one is the slowest, so we need to wait for it!
			futurePut.futureRequests().awaitUninterruptibly();

			System.err.println(futurePut.failedReason());
			// should be run into a timeout
			Assert.assertFalse(futurePut.isSuccess());

		} finally {
			if (master != null) {
				master.shutdown().await();
			}
			if (unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
		}
	}
	
	@Test
	public void testVeryFewPeers() throws Exception {
		final Random rnd = new Random(42);
		Peer master = null;
		Peer unreachablePeer = null;
		try {
			Peer[] peers = UtilsNAT.createNodes(3, rnd, 4000);
			master = peers[0]; // the relay peer
			UtilsNAT.perfectRouting(peers);
			for (Peer peer : peers) {
				new PeerBuilderNAT(peer).bufferConfiguration(bufferConfig).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start();
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
			FutureRelayNAT fbn = uNat.startRelay(relayConfig, master.peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(fbn.isSuccess());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
			if (unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
		}
	}

	private Collection<PeerAddress> getNeighbors(Peer peer) {
		if(peer == null) {
			return Collections.emptyList();
		}
		
		Map<Number320, DispatchHandler> handlers = peer.connectionBean().dispatcher().searchHandler(5);
		for (Map.Entry<Number320, DispatchHandler> entry : handlers.entrySet()) {
			if (entry.getValue() instanceof BaseRelayForwarderRPC) {
				return ((BaseRelayForwarderRPC) entry.getValue()).getPeerMap();
			}
		}
		return Collections.emptyList();
	}

}
