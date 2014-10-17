package net.tomp2p.relay;

import java.net.InetAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestRelay {

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
				new PeerBuilderNAT(peer).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(5000).start();
			PeerAddress pa = unreachablePeer.peerBean().serverPeerAddress();
			pa = pa.changeFirewalledTCP(true).changeFirewalledUDP(true);
			unreachablePeer.peerBean().serverPeerAddress(pa);
			// find neighbors
			FutureBootstrap futureBootstrap = unreachablePeer.bootstrap().peerAddress(peers[0].peerAddress()).start();
			futureBootstrap.awaitUninterruptibly();
			Assert.assertTrue(futureBootstrap.isSuccess());
			//setup relay
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
			FutureRelay fr = new FutureRelay();
			uNat.startSetupRelay(fr);
			fr.awaitUninterruptibly();
			System.err.println(fr.failedReason());
			Assert.assertTrue(fr.isSuccess());
			//Assert.assertEquals(2, fr.relays().size());

			// Check if flags are set correctly
			Assert.assertTrue(unreachablePeer.peerAddress().isRelayed());
			Assert.assertFalse(unreachablePeer.peerAddress().isFirewalledTCP());
			Assert.assertFalse(unreachablePeer.peerAddress().isFirewalledUDP());

		}
		
		finally {
			if (master != null) {
				unreachablePeer.shutdown().await();
				master.shutdown().await();
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
            for(Peer peer:peers) {
            	new PeerBuilderNAT(peer).start();
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
         	//setup relay
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
			FutureRelay fr = new FutureRelay();
			uNat.startSetupRelay(fr);
			fr.awaitUninterruptibly();
			// find neighbors again
         	futureBootstrap = unreachablePeer.bootstrap().peerAddress(peers[0].peerAddress()).start();
         	futureBootstrap.awaitUninterruptibly();
         	Assert.assertTrue(futureBootstrap.isSuccess());
            
         	boolean otherPeersHaveRelay = false;
            
            
            for(Peer peer:peers) {
            	if(peer.peerBean().peerMap().allOverflow().contains(unreachablePeer.peerAddress())) {
            		for(PeerAddress pa: peer.peerBean().peerMap().allOverflow()) {
            			if(pa.peerId().equals(unreachablePeer.peerID())) {
            				if(pa.peerSocketAddresses().size() > 0) {
            					otherPeersHaveRelay = true;
            				}
            				System.err.println("-->"+pa.peerSocketAddresses());
            				System.err.println("relay="+pa.isRelayed());
            			}
            		}
            		System.err.println("check 1! "+peer.peerAddress());
            	}
            	
            	
            	
            }
            Assert.assertTrue(otherPeersHaveRelay);
            
            //wait for maintenance
            Thread.sleep(3000);
            
            boolean otherPeersMe = false;
            for(Peer peer:peers) {
            	
            	if(peer.peerBean().peerMap().all().contains(unreachablePeer.peerAddress())) {
            		System.err.println("check 2! "+peer.peerAddress());
            		otherPeersMe = true;
            	}
            }
            Assert.assertTrue(otherPeersMe);
            

        } finally {
            if (master != null) {
                unreachablePeer.shutdown().await();
                master.shutdown().await();
            }
        }
    }
    
    @Test
    public void testRelaySendDirect() throws Exception {
        final Random rnd = new Random(42);
        final int nrOfNodes = 100;
        Peer master = null;
        Peer unreachablePeer = null;
        try {
            // setup test peers
            Peer[] peers = UtilsNAT.createNodes(nrOfNodes, rnd, 4001);
            master = peers[0];
            UtilsNAT.perfectRouting(peers);
            for(Peer peer:peers) {
            	new PeerBuilderNAT(peer).start();
            }

            // Test setting up relay peers
         	unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start();
         	PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
         	
         	FutureRelayNAT fbn = uNat.startRelay(master.peerAddress());
         	System.err.println("master: "+master.peerAddress());
         	fbn.awaitUninterruptibly();
         	Assert.assertTrue(fbn.isSuccess());
            
            
         	System.out.print("Send direct message to unreachable peer " + unreachablePeer.peerAddress());
            final String request = "Hello ";
            final String response = "World!";
            
            final AtomicBoolean test1 = new AtomicBoolean(false);
            final AtomicBoolean test2 = new AtomicBoolean(false);
            
            //final Peer unr = unreachablePeer;
            unreachablePeer.objectDataReply(new ObjectDataReply() {
                public Object reply(PeerAddress sender, Object obj) throws Exception {
                	test1.set(obj.equals(request));
                    Assert.assertEquals(request.toString(), request);
                    test2.set(sender.inetAddress().toString().indexOf("0.0.0.0")>=0);
                    System.err.println("Got sender:"+sender);
                    
                    //this is too late here, so we cannot test this here
                    //Collection<PeerSocketAddress> list = new ArrayList<PeerSocketAddress>();
                    //list.add(new PeerSocketAddress(InetAddress.getByName("101.101.101.101"), 101, 101));
                    //unr.peerBean().serverPeerAddress(unr.peerBean().serverPeerAddress().changePeerSocketAddresses(list));
                    
                    return response;
                }
            });
            
            
            
            //prevent rcon
            Collection<PeerSocketAddress> list = new ArrayList<PeerSocketAddress>();
            list.add(new PeerSocketAddress(InetAddress.getByName("10.10.10.10"), 10, 10));
            peers[42].peerBean().serverPeerAddress(peers[42].peerBean().serverPeerAddress().changeRelayed(true).changePeerSocketAddresses(list));
            System.err.println("initiator: "+peers[42].peerBean().serverPeerAddress());
            System.err.println("unreachablePeer: "+unreachablePeer.peerAddress());
            FutureDirect fd = peers[42].sendDirect(unreachablePeer.peerAddress()).object(request).start().awaitUninterruptibly();
            System.err.println("got msg from: "+fd.futureResponse().responseMessage().sender());
            Assert.assertEquals(response, fd.object());
            //make sure we did not receive it from the unreachable peer with port 13337
            //System.err.println(fd.getWrappedFuture());
            //TODO: this case is true for relay
            //Assert.assertEquals(fd.wrappedFuture().responseMessage().senderSocket().getPort(), 4001);
            //TODO: this case is true for rcon
            Assert.assertEquals(unreachablePeer.peerID(), fd.wrappedFuture().responseMessage().sender().peerId());
            Assert.assertTrue(test1.get());
            Assert.assertFalse(test2.get());
            Assert.assertEquals(fd.futureResponse().responseMessage().sender().peerSocketAddresses().size(), 5);

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
            for(Peer peer:peers) {
            	new PeerBuilderNAT(peer).start();
            }

            // Test setting up relay peers
         	unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start();
         	PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
         	
         	FutureRelayNAT fbn = uNat.startRelay(master.peerAddress());
         	fbn.awaitUninterruptibly();
         	Assert.assertTrue(fbn.isSuccess());
            
            
         	System.out.print("Send direct message to unreachable peer");
            final String request = "Hello ";
            final String response = "World!";
            
            unreachablePeer.objectDataReply(new ObjectDataReply() {
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    Assert.assertEquals(request.toString(), request);
                    return response;
                }
            });
            
            FutureDirect fd = peers[42].sendDirect(unreachablePeer.peerAddress()).object(request).start().awaitUninterruptibly();
            //fd.awaitUninterruptibly();
            if(fd.futureResponse().responseMessage() == null) {
            	System.err.println("aoeuo");
            }
            Assert.assertEquals(response, fd.object());
            //make sure we did not receive it from the unreachable peer with port 13337
            //System.err.println(fd.getWrappedFuture());
            //TODO: this case is true for relay
            //Assert.assertEquals(fd.wrappedFuture().responseMessage().senderSocket().getPort(), 4001);
            //TODO: this case is true for rcon
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

    @Test
    public void testRelayRouting() throws Exception {
        final Random rnd = new Random(42);
        final int nrOfNodes = 8; //test only works if total nr of nodes is < 8
        Peer master = null;
        Peer unreachablePeer = null;
        try {
            // setup test peers
            Peer[] peers = UtilsNAT.createNodes(nrOfNodes, rnd, 4001);
            master = peers[0];
            UtilsNAT.perfectRouting(peers);
            for(Peer peer:peers) {
            	new PeerBuilderNAT(peer).start();
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
         	//setup relay
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
			FutureRelay fr = new FutureRelay();
			uNat.startSetupRelay(fr);
			fr.awaitUninterruptibly();
			// find neighbors again
         	futureBootstrap = unreachablePeer.bootstrap().peerAddress(peers[0].peerAddress()).start();
         	futureBootstrap.awaitUninterruptibly();
         	Assert.assertTrue(futureBootstrap.isSuccess());
         	//
         	FutureRelay fr2 = new FutureRelay();
         	DistributedRelay dr = uNat.startSetupRelay(fr2);
         	Shutdown shutdown = uNat.startRelayMaintenance(fr2, unreachablePeer.bootstrap().peerAddress(peers[0].peerAddress()), dr);
         	
            PeerAddress relayPeer = fr.relays().iterator().next().remotePeer(); 
            Peer found = null;
            for(Peer p:peers) {
            	if(p.peerAddress().equals(relayPeer)) {
            		found = p;
            		break;
            	}
            }
            
            Thread.sleep(3000);

            int nrOfNeighbors = getNeighbors(found).size();
            //we have in total 9 peers, we should find 8 as neighbors
            Assert.assertEquals(8, nrOfNeighbors);
            
            System.err.println("neighbors: "+nrOfNeighbors);
            for(PeerConnection pc:fr.relays()) {
            	System.err.println("pc:"+pc.remotePeer());
            }
            Assert.assertEquals(5, fr.relays().size());

            //Shut down a peer
            Thread.sleep(3000);
            peers[nrOfNodes - 1].shutdown().await();
            peers[nrOfNodes - 2].shutdown().await();
            peers[nrOfNodes - 3].shutdown().await();

            /*
             * needed because failure of a node is detected with periodic
             * heartbeat and the routing table of the relay peers are also
             * updated periodically
             */
            Thread.sleep(15000);

            Assert.assertEquals(nrOfNeighbors - 3, getNeighbors(found).size());
            Assert.assertEquals(5, fr.relays().size());
            shutdown.shutdown();

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
    public void testRelayRPC() throws Exception {
        Peer master = null;
        Peer slave = null;
        try {
            final Random rnd = new Random(42);
            Peer[] peers = UtilsNAT.createNodes(2, rnd, 4000);
            master = peers[0]; // the relay peer
        	new PeerBuilderNAT(master).start(); // register relayRPC ioHandler
            slave = peers[1];

            // create channel creator
            FutureChannelCreator fcc = slave.connectionBean().reservation().create(1, PeerAddress.MAX_RELAYS);
            fcc.awaitUninterruptibly();

            final FuturePeerConnection fpc = slave.createPeerConnection(master.peerAddress());
            FutureDone<PeerConnection> rcf = new PeerBuilderNAT(slave).start().relayRPC().setupRelay(fcc.channelCreator(), fpc);
            rcf.awaitUninterruptibly();

            //Check if permanent peer connection was created
            Assert.assertTrue(rcf.isSuccess());
            Assert.assertEquals(master.peerAddress(), fpc.object().remotePeer());
            Assert.assertTrue(fpc.object().channelFuture().channel().isActive());
            Assert.assertTrue(fpc.object().channelFuture().channel().isOpen());

        } finally {
            master.shutdown().await();
            slave.shutdown().await();
        }
    }	public BaseFuture publishNeighbors() {
	    return null;
    }

    
    @Test
    public void testNoRelayDHT() throws Exception {
    	final Random rnd = new Random(42);
    	 PeerDHT master = null;
         PeerDHT slave = null;
         try {
             PeerDHT[] peers = UtilsNAT.createNodesPeer(10, rnd, 4000);
             master = peers[0]; // the relay peer
             UtilsNAT.perfectRouting(peers);
             for(PeerDHT peer:peers) {
            	 new PeerBuilderNAT(peer.peer()).start();
             }
             PeerMapConfiguration pmc = new PeerMapConfiguration(Number160.createHash(rnd.nextInt()));
             slave = new PeerBuilderDHT(new PeerBuilder(Number160.ONE).peerMap(new PeerMap(pmc)).ports(13337).start()).start();
             printMapStatus(slave, peers);
             FuturePut futurePut = peers[8].put(slave.peerID()).data(new Data("hello")).start().awaitUninterruptibly();
             futurePut.futureRequests().awaitUninterruptibly();
             Assert.assertTrue(futurePut.isSuccess());
             Assert.assertFalse(slave.storageLayer().contains(
            		 new Number640(slave.peerID(), Number160.ZERO, Number160.ZERO, Number160.ZERO)));
             System.err.println("DONE!");
             
         } finally {
             master.shutdown().await();
             slave.shutdown().await();
         }
    }

	private void printMapStatus(PeerDHT slave, PeerDHT[] peers) {
	    for(PeerDHT peer:peers) {
	    	 if(peer.peerBean().peerMap().allOverflow().contains(slave.peerAddress())) {
	    		 System.err.println("found relayed peer in overflow bag " + peer.peerAddress());
	    	 }
	     }
	     
	     for(PeerDHT peer:peers) {
	    	 if(peer.peerBean().peerMap().all().contains(slave.peerAddress())) {
	    		 System.err.println("found relayed peer in regular bag" + peer.peerAddress());
	    	 }
	     }
    }
	
	@Test
    public void testRelayDHTSimple() throws Exception {
        final Random rnd = new Random(42);
         PeerDHT master = null;
         PeerDHT unreachablePeer = null;
         try {
        	 PeerDHT[] peers = UtilsNAT.createNodesPeer(1, rnd, 4000);
             master = peers[0]; // the relay peer
             new PeerBuilderNAT(master.peer()).start();
             
             // Test setting up relay peers
 			unreachablePeer = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start()).start();
 			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer.peer()).start();
 			
 			FutureRelayNAT fbn = uNat.startRelay(master.peerAddress());
 			fbn.awaitUninterruptibly();
 			Assert.assertTrue(fbn.isSuccess());
             
            System.err.println("DONE!");
             
         } finally {
             master.shutdown().await();
             unreachablePeer.shutdown().await();
         }
    }
    
	@Test
    public void testRelayDHT() throws Exception {
        final Random rnd = new Random(42);
         PeerDHT master = null;
         PeerDHT unreachablePeer = null;
         try {
        	 PeerDHT[] peers = UtilsNAT.createNodesPeer(10, rnd, 4000);
             master = peers[0]; // the relay peer
             UtilsNAT.perfectRouting(peers);
             for(PeerDHT peer:peers) {
            	 new PeerBuilderNAT(peer.peer()).start();
             }
             
             // Test setting up relay peers
 			unreachablePeer = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start()).start();
 			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer.peer()).start();
 			
 			FutureRelayNAT fbn = uNat.startRelay(master.peerAddress());
 			fbn.awaitUninterruptibly();
 			Assert.assertTrue(fbn.isSuccess());
 			
 			//unreachablePeer.peer().bootstrap().peerAddress(master.peerAddress()).start();
             
            // PeerMapConfiguration pmc = new PeerMapConfiguration(Number160.createHash(rnd.nextInt()));
            
            // slave = new PeerMaker(Number160.ONE).peerMap(new PeerMap(pmc)).ports(13337).makeAndListen();
            // FutureRelay rf = new RelayConf(slave).bootstrapAddress(master.getPeerAddress()).start().awaitUninterruptibly();
            // Assert.assertTrue(rf.isSuccess());
            // RelayManager manager = rf.relayManager();
            // System.err.println("relays: "+manager.getRelayAddresses());
            // System.err.println("psa: "+ slave.getPeerAddress().getPeerSocketAddresses());
             //wait for maintenance to kick in
             Thread.sleep(4000);
             
             printMapStatus(unreachablePeer, peers);
             
             FuturePut futurePut = peers[8].put(unreachablePeer.peerID()).data(new Data("hello")).start().awaitUninterruptibly();
             //the relayed one is the slowest, so we need to wait for it!
             futurePut.futureRequests().awaitUninterruptibly();
             System.err.println(futurePut.failedReason());
             
             Assert.assertTrue(futurePut.isSuccess());
             //we cannot see the peer in futurePut.rawResult, as the relayed is the slowest and we finish earlier than that.
             Assert.assertTrue(unreachablePeer.storageLayer().contains(new Number640(unreachablePeer.peerID(), Number160.ZERO, Number160.ZERO, Number160.ZERO)));
             System.err.println("DONE!");
             
         } finally {
             master.shutdown().await();
             unreachablePeer.shutdown().await();
         }
    }
	
	@Test
    public void testRelayDHTPutGet() throws Exception {
        final Random rnd = new Random(42);
         PeerDHT master = null;
         PeerDHT unreachablePeer = null;
         try {
        	 PeerDHT[] peers = UtilsNAT.createNodesPeer(10, rnd, 4000);
             master = peers[0]; // the relay peer
             UtilsNAT.perfectRouting(peers);
             for(PeerDHT peer:peers) {
            	 new PeerBuilderNAT(peer.peer()).start();
             }
             
             // Test setting up relay peers
 			unreachablePeer = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start()).start();
 			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer.peer()).start();
 			
 			FutureRelayNAT fbn = uNat.startRelay(master.peerAddress());
 			fbn.awaitUninterruptibly();
 			Assert.assertTrue(fbn.isSuccess());
 			
 			//unreachablePeer.peer().bootstrap().peerAddress(master.peerAddress()).start();
             
            // PeerMapConfiguration pmc = new PeerMapConfiguration(Number160.createHash(rnd.nextInt()));
            
            // slave = new PeerMaker(Number160.ONE).peerMap(new PeerMap(pmc)).ports(13337).makeAndListen();
            // FutureRelay rf = new RelayConf(slave).bootstrapAddress(master.getPeerAddress()).start().awaitUninterruptibly();
            // Assert.assertTrue(rf.isSuccess());
            // RelayManager manager = rf.relayManager();
            // System.err.println("relays: "+manager.getRelayAddresses());
            // System.err.println("psa: "+ slave.getPeerAddress().getPeerSocketAddresses());
             //wait for maintenance to kick in
             Thread.sleep(5000);
             
             printMapStatus(unreachablePeer, peers);
             
             RoutingConfiguration r = new RoutingConfiguration(5, 1, 1);
             RequestP2PConfiguration rp = new RequestP2PConfiguration(1, 1, 0);
             
             System.err.println(unreachablePeer.peerID());
             
             FuturePut futurePut = peers[8].put(unreachablePeer.peerID()).data(new Data("hello")).routingConfiguration(r).requestP2PConfiguration(rp).start().awaitUninterruptibly();
             //the relayed one is the slowest, so we need to wait for it!
             futurePut.awaitUninterruptibly();
             futurePut.futureRequests().awaitUninterruptibly();
             System.err.println(futurePut.failedReason());
             System.err.println("peer8="+peers[8].peerAddress());
             
             Assert.assertTrue(futurePut.isSuccess());
             Assert.assertTrue(unreachablePeer.storageLayer().contains(new Number640(unreachablePeer.peerID(), Number160.ZERO, Number160.ZERO, Number160.ZERO)));
             //Thread.sleep(5000);
             FutureGet futureGet = peers[8].get(unreachablePeer.peerID()).routingConfiguration(r).requestP2PConfiguration(rp).start();
             futureGet.awaitUninterruptibly();
             Assert.assertTrue(futureGet.isSuccess());
             
             //we cannot see the peer in futurePut.rawResult, as the relayed is the slowest and we finish earlier than that.
             
             
             
         } finally {
             master.shutdown().await();
             unreachablePeer.shutdown().await();
         }
    }
	
	@Test
    public void testRelayDHTPutGet2() throws Exception {
        final Random rnd = new Random(42);
         PeerDHT master = null;
         PeerDHT unreachablePeer1 = null;
         PeerDHT unreachablePeer2 = null;
         try {
        	 PeerDHT[] peers = UtilsNAT.createNodesPeer(10, rnd, 4000);
             master = peers[0]; // the relay peer
            
             for(PeerDHT peer:peers) {
            	 new PeerBuilderNAT(peer.peer()).start();
             }
             
             // Test setting up relay peers
            unreachablePeer1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start()).start();
            unreachablePeer1.peer().peerBean().serverPeerAddress(unreachablePeer1.peer().peerAddress().changeFirewalledTCP(true).changeFirewalledUDP(true));
 			PeerNAT uNat1 = new PeerBuilderNAT(unreachablePeer1.peer()).start();
 			FutureRelayNAT fbn1 = uNat1.startRelay(master.peerAddress());
 			fbn1.awaitUninterruptibly();
 			Assert.assertTrue(fbn1.isSuccess());
 			//
 			unreachablePeer2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13338).start()).start();
 			unreachablePeer2.peer().peerBean().serverPeerAddress(unreachablePeer2.peer().peerAddress().changeFirewalledTCP(true).changeFirewalledUDP(true));
 			PeerNAT uNat2 = new PeerBuilderNAT(unreachablePeer2.peer()).start();
 			FutureRelayNAT fbn2 = uNat2.startRelay(master.peerAddress());
 			fbn2.awaitUninterruptibly();
 			Assert.assertTrue(fbn2.isSuccess());
 			
 			peers[8] = unreachablePeer1;
 			peers[9] = unreachablePeer2;
 			
 			 UtilsNAT.perfectRouting(peers);
 			
 			//unreachablePeer.peer().bootstrap().peerAddress(master.peerAddress()).start();
             
            // PeerMapConfiguration pmc = new PeerMapConfiguration(Number160.createHash(rnd.nextInt()));
            
            // slave = new PeerMaker(Number160.ONE).peerMap(new PeerMap(pmc)).ports(13337).makeAndListen();
            // FutureRelay rf = new RelayConf(slave).bootstrapAddress(master.getPeerAddress()).start().awaitUninterruptibly();
            // Assert.assertTrue(rf.isSuccess());
            // RelayManager manager = rf.relayManager();
            // System.err.println("relays: "+manager.getRelayAddresses());
            // System.err.println("psa: "+ slave.getPeerAddress().getPeerSocketAddresses());
             //wait for maintenance to kick in
             Thread.sleep(5000);
             
             printMapStatus(unreachablePeer1, peers);
             
             printMapStatus(unreachablePeer2, peers);
             
             RoutingConfiguration r = new RoutingConfiguration(5, 1, 1);
             RequestP2PConfiguration rp = new RequestP2PConfiguration(1, 1, 0);
             
             System.err.println(unreachablePeer1.peerID()); //f1
             System.err.println(unreachablePeer2.peerID()); //e7
             
             FuturePut futurePut = unreachablePeer1.put(unreachablePeer2.peerID()).data(new Data("hello")).routingConfiguration(r).requestP2PConfiguration(rp).start().awaitUninterruptibly();
             //the relayed one is the slowest, so we need to wait for it!
             futurePut.awaitUninterruptibly();
             futurePut.futureRequests().awaitUninterruptibly();
             System.err.println(futurePut.failedReason());
             
             Assert.assertTrue(futurePut.isSuccess());
             Assert.assertTrue(unreachablePeer2.storageLayer().contains(new Number640(unreachablePeer2.peerID(), Number160.ZERO, Number160.ZERO, Number160.ZERO)));
             
             FutureGet futureGet = unreachablePeer1.get(unreachablePeer2.peerID()).routingConfiguration(r).requestP2PConfiguration(rp).fastGet(false).start().awaitUninterruptibly();
             //TODO: try peers even if no data found with fastget
             System.err.println(futureGet.failedReason());
             Assert.assertTrue(futureGet.isSuccess());
             
             //we cannot see the peer in futurePut.rawResult, as the relayed is the slowest and we finish earlier than that.
             
             System.err.println("DONE!");
             
         } finally {
             master.shutdown().await();
             unreachablePeer1.shutdown().await();
             unreachablePeer2.shutdown().await();
         }
    }
	
	@Test
    public void testRelayDHTPutGetSigned() throws Exception {
        final Random rnd = new Random(42);
         PeerDHT master = null;
         PeerDHT unreachablePeer1 = null;
         PeerDHT unreachablePeer2 = null;
         try {
        	 PeerDHT[] peers = UtilsNAT.createNodesPeer(10, rnd, 4000);
             master = peers[0]; // the relay peer
            
             for(PeerDHT peer:peers) {
            	 new PeerBuilderNAT(peer.peer()).start();
             }
             
             KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
             KeyPair pair1 = gen.generateKeyPair();
             KeyPair pair2 = gen.generateKeyPair();
             
             // Test setting up relay peers
            unreachablePeer1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).keyPair(pair1).ports(13337).start()).start();
            unreachablePeer1.peer().peerBean().serverPeerAddress(unreachablePeer1.peer().peerAddress().changeFirewalledTCP(true).changeFirewalledUDP(true));
 			PeerNAT uNat1 = new PeerBuilderNAT(unreachablePeer1.peer()).start();
 			FutureRelayNAT fbn1 = uNat1.startRelay(master.peerAddress());
 			fbn1.awaitUninterruptibly();
 			Assert.assertTrue(fbn1.isSuccess());
 			//
 			unreachablePeer2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(rnd.nextInt())).keyPair(pair2).ports(13338).start()).start();
 			unreachablePeer2.peer().peerBean().serverPeerAddress(unreachablePeer2.peer().peerAddress().changeFirewalledTCP(true).changeFirewalledUDP(true));
 			PeerNAT uNat2 = new PeerBuilderNAT(unreachablePeer2.peer()).start();
 			FutureRelayNAT fbn2 = uNat2.startRelay(master.peerAddress());
 			fbn2.awaitUninterruptibly();
 			Assert.assertTrue(fbn2.isSuccess());
 			
 			peers[8] = unreachablePeer1;
 			peers[9] = unreachablePeer2;
 			
 			 UtilsNAT.perfectRouting(peers);
 			
 			//unreachablePeer.peer().bootstrap().peerAddress(master.peerAddress()).start();
             
            // PeerMapConfiguration pmc = new PeerMapConfiguration(Number160.createHash(rnd.nextInt()));
            
            // slave = new PeerMaker(Number160.ONE).peerMap(new PeerMap(pmc)).ports(13337).makeAndListen();
            // FutureRelay rf = new RelayConf(slave).bootstrapAddress(master.getPeerAddress()).start().awaitUninterruptibly();
            // Assert.assertTrue(rf.isSuccess());
            // RelayManager manager = rf.relayManager();
            // System.err.println("relays: "+manager.getRelayAddresses());
            // System.err.println("psa: "+ slave.getPeerAddress().getPeerSocketAddresses());
             //wait for maintenance to kick in
             Thread.sleep(5000);
             
             printMapStatus(unreachablePeer1, peers);
             
             printMapStatus(unreachablePeer2, peers);
             
             RoutingConfiguration r = new RoutingConfiguration(5, 1, 1);
             RequestP2PConfiguration rp = new RequestP2PConfiguration(1, 1, 0);
             
             System.err.println(unreachablePeer1.peerID()); //f1
             System.err.println(unreachablePeer2.peerID()); //e7
             
             FuturePut futurePut = unreachablePeer1.put(unreachablePeer2.peerID()).data(new Data("hello")).sign().routingConfiguration(r).requestP2PConfiguration(rp).start().awaitUninterruptibly();
             //the relayed one is the slowest, so we need to wait for it!
             futurePut.awaitUninterruptibly();
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
             master.shutdown().await();
             unreachablePeer1.shutdown().await();
             unreachablePeer2.shutdown().await();
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
				new PeerBuilderNAT(peer).start();
			}

			// Test setting up relay peers
			unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start();
			PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).start();
			FutureRelayNAT fbn = uNat.startRelay(master.peerAddress());
			fbn.awaitUninterruptibly();
			Assert.assertTrue(fbn.isSuccess());

		} finally {
			master.shutdown().await();
			unreachablePeer.shutdown().await();
		}
	}
    

    private Collection<PeerAddress> getNeighbors(Peer peer) {
    	Map<Number320, DispatchHandler> handlers = peer.connectionBean().dispatcher().searchHandler(5);
    	for(Map.Entry<Number320, DispatchHandler> entry:handlers.entrySet()) {
    		if(entry.getValue() instanceof RelayForwarderRPC) {
    			return ((RelayForwarderRPC)entry.getValue()).all();  
    		}
    	}
    	return null;
    }
    
}
