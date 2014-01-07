package net.tomp2p.relay;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

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
			RelayManager manager = new RelayManager(unreachablePeer, master.getPeerAddress());
			RelayFuture rf = manager.setupRelays();
			rf.awaitUninterruptibly();
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
			Peer slave = new PeerMaker(Number160.createHash(rnd.nextInt())).ports(13337).makeAndListen();

			// Ping peer before setting up relays
			// System.out.println("Ping unreachable peer before setting up relays");
			// BaseFuture f1 =
			// peers[rnd.nextInt(nrOfNodes)].ping().setPeerAddress(slave.getPeerAddress()).start();
			// f1.awaitUninterruptibly();
			// Assert.assertFalse(f1.isSuccess());

			RelayManager manager = new RelayManager(slave, master.getPeerAddress());
			RelayFuture rf = manager.setupRelays();
			rf.awaitUninterruptibly();

			Assert.assertTrue(rf.isSuccess());
			
			//Check if flags are set correctly
			//Assert.assertTrue(slave.getPeerAddress().isRelay()); //TODO
			Assert.assertFalse(slave.getPeerAddress().isFirewalledTCP());
			Assert.assertFalse(slave.getPeerAddress().isFirewalledUDP());

			// Ping the unreachable peer from any peer
			System.out.print("Send UDP ping to unreachable peer");
			BaseFuture f2 = peers[rnd.nextInt(nrOfNodes)].ping().setPeerAddress(slave.getPeerAddress()).start();
			f2.awaitUninterruptibly();
			Assert.assertTrue(f2.isSuccess());
			System.out.println("...done");

			System.out.print("Send TCP ping to unreachable peer");
			BaseFuture f3 = peers[rnd.nextInt(nrOfNodes)].ping().setTcpPing().setPeerAddress(slave.getPeerAddress()).start();
			f3.awaitUninterruptibly();
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

			RelayConnectionFuture rcf = new RelayRPC(slave).setupRelay(master.getPeerAddress(), fcc.getChannelCreator());
			rcf.awaitUninterruptibly();
			
			//Check if permanent peer connection was created
			Assert.assertTrue(rcf.isSuccess());
			FuturePeerConnection fpc = rcf.futurePeerConnection();
			Assert.assertEquals(master.getPeerAddress(), fpc.getObject().remotePeer());
			Assert.assertTrue(fpc.getObject().channelFuture().channel().isActive());
			Assert.assertTrue(fpc.getObject().channelFuture().channel().isOpen());

		} finally {
			master.shutdown().await();
			slave.shutdown().await();
		}
	}

}
