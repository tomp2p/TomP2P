package net.tomp2p.rcon;
import java.io.IOException;
import java.net.ConnectException;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.nat.NATUtils;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.FutureRelay;
import net.tomp2p.relay.UtilsNAT;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRcon {

	private static final Logger LOG = LoggerFactory.getLogger(TestRcon.class);

	private Peer reachable = null;
	private Peer unreachable = null;
	private Peer relay = null;
	private Peer[] peers = new Peer[2];

	private static final int PORTS = 4001;
	private static final Number160 REACHABLE_ID = Number160.createHash("reachable");
	private static final Number160 UNREACHABLE_ID = Number160.createHash("unreachable");
	private static final Number160 RELAY_ID = Number160.createHash("relay");
	
	@Before
	public void testRconSetup() throws IOException {
		relay = new PeerBuilder(RELAY_ID).ports(PORTS).start();
		reachable = new PeerBuilder(REACHABLE_ID).behindFirewall(false).ports(PORTS).masterPeer(relay).start();
		unreachable = new PeerBuilder(UNREACHABLE_ID).behindFirewall(true).ports(PORTS).masterPeer(relay).start();

		peers[0] = relay;
		peers[1] = reachable;
		
		UtilsNAT.perfectRouting(peers);
		makePeerNat();
		bootstrapUnreachable();

		System.out.println("BOOTSTRAP SUCCESS!!!!");
		
//		Assert.assertEquals(true, unreachable.peerAddress().isFirewalledTCP());
//		Assert.assertEquals(true, unreachable.peerAddress().isFirewalledUDP());
	}

	private void makePeerNat() {
		
		peers = new Peer[3];
		peers[0] = relay;
		peers[1] = reachable;
		peers[2] = unreachable;
		
		for (Peer ele : peers) {
			new PeerNAT(ele);
		}
	}

	private void bootstrapUnreachable() throws ConnectException {
		// Set the isFirewalledUDP and isFirewalledTCP flags
		PeerAddress upa = unreachable.peerBean().serverPeerAddress();
		upa = upa.changeFirewalledTCP(true).changeFirewalledUDP(true);
		unreachable.peerBean().serverPeerAddress(upa);

		// find neighbors
		FutureBootstrap futureBootstrap = unreachable.bootstrap().peerAddress(relay.peerAddress()).start();
		futureBootstrap.awaitUninterruptibly();

		// setup relay
		PeerNAT uNat = new PeerNAT(unreachable);
		// set up 3 relays
		FutureRelay futureRelay = uNat.minRelays(1).startSetupRelay();
		futureRelay.awaitUninterruptibly();

		// find neighbors again
		FutureBootstrap fb2 = unreachable.bootstrap().peerAddress(relay.peerAddress()).start();
		fb2.awaitUninterruptibly();
		
		if (fb2.isSuccess()) {
			LOG.info("unreachable Bootstrap success!");
		} else {
			LOG.error("unreachable Bootstrap fail!");
			throw new ConnectException();
		}
	}
	
	@Test
	public void testReverseConnection() throws ClassNotFoundException, IOException, InterruptedException {
		
		final String requestString = "This is a test String";
		
		for (Peer ele : peers) {
			ele.objectDataReply(new ObjectDataReply() {
				
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					Assert.assertEquals(requestString, ((String) request));
					return "SUCCESS HIT";
				}
			});
		}
		
		PeerAddress address = unreachable.peerAddress().changeAddress(relay.peerAddress().inetAddress()).changeFirewalledTCP(true).changeFirewalledUDP(true);
		FutureDirect fd = reachable.sendDirect(address).object(requestString).connectionTimeoutTCPMillis(60000).start();
		fd.awaitUninterruptibly();
		
		if (!fd.isCompleted()) {
			Assert.fail("The sendDirect Message failed");
		} else {
			Assert.assertTrue(fd.isSuccess());
		}
		
		Thread.sleep(99999999);
	}
	
	@After
	public void shutdown() {
		for (Peer ele: peers) {
			ele.shutdown();
		}
	}
}
