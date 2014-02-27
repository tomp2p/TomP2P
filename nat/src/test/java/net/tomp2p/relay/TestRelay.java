package net.tomp2p.relay;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import net.tomp2p.Utils2;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FuturePut;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
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
            Peer[] peers = Utils2.createNodes(nrOfNodes, rnd, 4001);
            master = peers[0];
            List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>(nrOfNodes);
            for (int i = 0; i < peers.length; i++) {
                new RelayRPC(peers[i]);
                if (peers[i] != master) {
                    FutureBootstrap res = peers[i].bootstrap().setPeerAddress(master.getPeerAddress()).start();
                    tmp.add(res);
                }
            }
            for (FutureBootstrap fm : tmp) {
                fm.awaitUninterruptibly();
                Assert.assertTrue("Bootrapping test peers failed", fm.isSuccess());
            }

            // Test setting up relay peers
            unreachablePeer = new PeerMaker(Number160.createHash(rnd.nextInt())).ports(5000).makeAndListen();
            RelayFuture rf = new RelayBuilder(unreachablePeer).bootstrapAddress(master.getPeerAddress()).start();
            rf.awaitUninterruptibly();
            RelayManager manager = rf.relayManager();
            Assert.assertTrue(rf.isSuccess());
            Assert.assertEquals(manager, rf.relayManager());
            Assert.assertTrue(manager.getRelayAddresses().size() > 0);
            Assert.assertTrue(manager.getRelayAddresses().size() <= PeerAddress.MAX_RELAYS);
            Assert.assertEquals(manager.getRelayAddresses().size(), unreachablePeer.getPeerAddress().getPeerSocketAddresses().length);

        } finally {
            if (master != null) {
                unreachablePeer.shutdown().await();
                master.shutdown().await();
            }
        }
    }

    @Test
    public void testRelay() throws Exception {
        final Random rnd = new Random(42);
        final int nrOfNodes = 200;
        Peer master = null;
        Peer slave = null;
        try {
            // setup test peers
            Peer[] peers = Utils2.createNodes(nrOfNodes, rnd, 4001);
            master = peers[0];
            List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>(nrOfNodes);
            for (int i = 0; i < peers.length; i++) {
                new RelayRPC(peers[i]);
                if (peers[i] != master) {
                    FutureBootstrap res = peers[i].bootstrap().setPeerAddress(master.getPeerAddress()).start();
                    tmp.add(res);
                }
            }
            for (FutureBootstrap fm : tmp) {
                fm.awaitUninterruptibly();
                Assert.assertTrue("Bootrapping test peers failed", fm.isSuccess());
            }

            // Test setting up relay peers
            slave = new PeerMaker(Number160.createHash(rnd.nextInt())).ports(13337).makeAndListen();

            RelayFuture rf = new RelayBuilder(slave).bootstrapAddress(master.getPeerAddress()).start();
            rf.awaitUninterruptibly();

            Assert.assertTrue(rf.isSuccess());

            //Check if flags are set correctly
            Assert.assertTrue(slave.getPeerAddress().isRelayed());
            Assert.assertFalse(slave.getPeerAddress().isFirewalledTCP());
            Assert.assertFalse(slave.getPeerAddress().isFirewalledUDP());

            // Ping the unreachable peer from any peer
            System.out.print("Send TCP ping to unreachable peer");
            BaseFuture f3 = peers[rnd.nextInt(nrOfNodes)].ping().setTcpPing().setPeerAddress(slave.getPeerAddress()).start();
            f3.awaitUninterruptibly();
            System.err.println(f3.getFailedReason());
            Assert.assertTrue(f3.isSuccess());
            System.out.println("...done");

            System.out.print("Send direct message to unreachable peer");
            final String request = "Hello ";
            final String response = "World!";
            slave.setObjectDataReply(new ObjectDataReply() {
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    Assert.assertEquals(request.toString(), request);
                    return response;
                }
            });
            FutureDirect fd = peers[rnd.nextInt(nrOfNodes)].sendDirect(slave.getPeerAddress()).setObject(request).start();

            fd.addListener(new BaseFutureListener<FutureDirect>() {
                public void operationComplete(FutureDirect future) throws Exception {
                    Assert.assertEquals(response, future.object());
                }

                public void exceptionCaught(Throwable t) throws Exception {
                    Assert.fail(t.getMessage());
                }
            });
            fd.awaitUninterruptibly();
            System.out.println("...done");
            Thread.sleep(100);

        } finally {
            if (slave != null) {
                slave.shutdown().await();
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
        Peer slave = null;
        try {
            // setup test peers
            Peer[] peers = Utils2.createNodes(nrOfNodes, rnd, 4001);
            master = peers[0];
            List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>(nrOfNodes);
            for (int i = 0; i < peers.length; i++) {
                new RelayRPC(peers[i]);
                if (peers[i] != master) {
                    FutureBootstrap res = peers[i].bootstrap().setPeerAddress(master.getPeerAddress()).start();
                    tmp.add(res);
                }
            }
            for (FutureBootstrap fm : tmp) {
                fm.awaitUninterruptibly();
                Assert.assertTrue("Bootrapping test peers failed", fm.isSuccess());
            }

            // Set up unreachable peer
            slave = new PeerMaker(Number160.createHash("urp")).ports(13337).makeAndListen();

            RelayFuture rf = new RelayBuilder(slave).bootstrapAddress(master.getPeerAddress()).start();
            rf.awaitUninterruptibly();
            Assert.assertTrue(rf.isSuccess());
            
            PeerAddress relayPeer = rf.relayManager().getRelayAddresses().iterator().next();
            Peer found = null;
            for(Peer p:peers) {
            	if(p.getPeerAddress().equals(relayPeer)) {
            		found = p;
            		break;
            	}
            }
            
            Thread.sleep(1000);

            int nrOfNeighbors = getNeighbors(found).size();
            //we have in total 9 peers, we should find 8 as neighbors
            Assert.assertEquals(8, nrOfNeighbors);
            
            System.err.println("neighbors: "+nrOfNeighbors);

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

        } finally {
            if (slave != null) {
                slave.shutdown().await();
            }
            if (master != null) {
                master.shutdown().await();
            }
        }
    }

    @Test
    public void testRelayFailed() throws Exception {
        final Random rnd = new Random(42);
        final int nrOfNodes = 20;
        Peer master = null;
        Peer slave = null;
        try {
            // setup test peers
            Peer[] peers = Utils2.createNodes(nrOfNodes, rnd, 4001);
            master = peers[0];
            List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>(nrOfNodes);
            for (int i = 0; i < peers.length; i++) {
                new RelayRPC(peers[i]);
                if (peers[i] != master) {
                    FutureBootstrap res = peers[i].bootstrap().setPeerAddress(master.getPeerAddress()).start();
                    tmp.add(res);
                }
            }
            for (FutureBootstrap fm : tmp) {
                fm.awaitUninterruptibly();
                Assert.assertTrue("Bootrapping test peers failed", fm.isSuccess());
            }

            // Set up relays
            slave = new PeerMaker(Number160.createHash(rnd.nextInt())).ports(13337).makeAndListen();

            RelayFuture rf = new RelayBuilder(slave).bootstrapAddress(master.getPeerAddress()).start();
            rf.awaitUninterruptibly();
            Assert.assertTrue(rf.isSuccess());
            RelayManager manager = rf.relayManager();

            Set<PeerAddress> relays = new HashSet<PeerAddress>(manager.getRelayAddresses());

            //Shut down a random relay peer
            Peer shutdownPeer = null;
            for (Peer peer : peers) {
                if (relays.contains(peer.getPeerAddress())) {
                    shutdownPeer = peer;
                    BaseFuture fd = peer.shutdown();
                    fd.awaitUninterruptibly();
                    break;
                }
            }

            //needed because failure of a node is detected with periodic heartbeat
            Thread.sleep(5000);

            Set<PeerAddress> newRelays = new HashSet<PeerAddress>(manager.getRelayAddresses());
            Assert.assertNotNull(shutdownPeer);
            Assert.assertEquals(manager.maxRelays(), newRelays.size());
            Assert.assertTrue(!newRelays.contains(shutdownPeer.getPeerAddress()));
            newRelays.removeAll(relays);
            Assert.assertEquals(1, newRelays.size());

        } finally {
            if (slave != null) {
                slave.shutdown().await();
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
            Peer[] peers = Utils2.createNodes(2, rnd, 4000);
            master = peers[0]; // the relay peer
            new RelayRPC(master); // register relayRPC ioHandler
            slave = peers[1];

            // create channel creator
            FutureChannelCreator fcc = slave.getConnectionBean().reservation().create(1, PeerAddress.MAX_RELAYS);
            fcc.awaitUninterruptibly();

            FutureDone<Void> futureDone = new RelayRPC(slave).setupRelay(master.getPeerAddress(), fcc.getChannelCreator());
            futureDone.awaitUninterruptibly();

            //Check if permanent peer connection was created
            Assert.assertTrue(futureDone.isSuccess());
            //TODO: 
//            FuturePeerConnection fpc = rcf.futurePeerConnection();
//            Assert.assertEquals(master.getPeerAddress(), fpc.getObject().remotePeer());
//            Assert.assertTrue(fpc.getObject().channelFuture().channel().isActive());
//            Assert.assertTrue(fpc.getObject().channelFuture().channel().isOpen());

        } finally {
            master.shutdown().await();
            slave.shutdown().await();
        }
    }
    
    @Test
    public void testRelayDHT() throws Exception {
    	final Random rnd = new Random(42);
    	 Peer master = null;
         Peer slave = null;
         try {
             Peer[] peers = Utils2.createNodes(100, rnd, 4000);
             master = peers[0]; // the relay peer
             for (int i = 0; i < peers.length; i++) {
                 new RelayRPC(peers[i]);
             }
             Utils2.perfectRouting(peers);
             PeerMapConfiguration pmc = new PeerMapConfiguration(Number160.createHash(rnd.nextInt()));
            
             slave = new PeerMaker(Number160.createHash(rnd.nextInt())).peerMap(new PeerMap(pmc)).ports(13337).makeAndListen();
             
             RelayFuture rf = new RelayBuilder(slave).bootstrapAddress(master.getPeerAddress()).start();
             rf.awaitUninterruptibly();
             Assert.assertTrue(rf.isSuccess());
             RelayManager manager = rf.relayManager();
             System.err.println("relays: "+manager.getRelayAddresses());
             System.err.println("psa: "+Arrays.toString(slave.getPeerAddress().getPeerSocketAddresses()));
             manager.publishNeighbors();
             Thread.sleep(3000);
             FuturePut futurePut = peers[33].put(slave.getPeerID()).setData(new Data("hello")).start().awaitUninterruptibly();
             Assert.assertTrue(futurePut.isSuccess());
             Assert.assertTrue(futurePut.getRawResult().containsKey(slave.getPeerAddress()));
             
         } finally {
             master.shutdown().await();
             slave.shutdown().await();
         }
    }

    private Collection<PeerAddress> getNeighbors(Peer peer) {
    	Map<Number160, DispatchHandler> handlers = peer.getConnectionBean().dispatcher().searchHandler(5);
    	handlers.remove(peer.getPeerID());
    	DispatchHandler dh = handlers.values().iterator().next();
    	return ((RelayNeighborRPC)dh).getAll(); 
    }

}
