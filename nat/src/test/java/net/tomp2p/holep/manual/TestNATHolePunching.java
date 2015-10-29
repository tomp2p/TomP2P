package net.tomp2p.holep.manual;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.holep.NATType;
import net.tomp2p.holep.NATTypeDetection;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

/**
 * Add the following lines to sudoers username ALL=(ALL) NOPASSWD:
 * <location>/nat-net.sh username ALL=(ALL) NOPASSWD: /usr/bin/ip
 * 
 * Make sure the network namespaces can resolve the hostname, otherwise huge
 * delays are to be expected.
 * 
 * This testcase runs on a single machine and tests the two widely used NAT
 * settings (port-preserving and symmetric). However, most likely this will
 * never run on travis-ci as this requires some extra setup on the machine
 * itself. Thus, this test-case is disabled by default and tests have to be
 * performed manully.
 * 
 * @author Thomas Bocek
 *
 */
//travis-ci cannot test this, the kernel does not support all the required features:
//Perhaps iptables or your kernel needs to be upgraded
//see also here: https://github.com/travis-ci/travis-ci/issues/1341
//@Ignore
public class TestNATHolePunching implements Serializable {
	
	private static final long serialVersionUID = 1L;
	final static private Random RND = new Random(42);
	
	//### CHANGE THIS TO YOUR INTERFACE###
	final static private String INF = "enp0s25";
	
	static private Number160 relayPeerId = new Number160(RND);

	@Before
	public void before() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("start", "0");
		LocalNATUtils.executeNatSetup("start", "1");
	}

	@After
	public void after() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("stop", "0");
		LocalNATUtils.executeNatSetup("stop", "1");
	}

	private static Serializable discover(final String address, Peer peer)
			throws UnknownHostException {
		PeerAddress relayP = new PeerAddress(relayPeerId, address, 5002, 5002);
		FutureDone<NATType> type = NATTypeDetection.checkNATType(peer, relayP)
				.awaitUninterruptibly();
		return type.isSuccess() ? type.object().name() : type.failedReason();
	}

	@SuppressWarnings("serial")
	@Test
	public void testDetection() throws Exception {
		Peer relayPeer = null;
		RemotePeer unr1 = null;
		RemotePeer unr2 = null;
		try {
			relayPeer = LocalNATUtils.createRealNode(relayPeerId, INF, 5002);
			InetAddress relayAddress = relayPeer.peerAddress().inetAddress();
			final String address = relayAddress.getHostAddress();
			CommandSync sync = new CommandSync(2);
			unr1 = LocalNATUtils.executePeer(0, sync, 
					new Command[] { new Command() {
						// startup
						@Override
						public Serializable execute() throws Exception {
							Peer peer = LocalNATUtils.init("10.0.0.2", 5000, 0);
							put("peer", peer);
							return "initialized " + peer.peerAddress();
						}
					}, new Command() {
						// detect the NAT type
						@Override
						public Serializable execute() throws Exception {
							Peer peer = (Peer) get("peer");
							return discover(address, peer);
						}
					}, new Command() {
						//shutdown
						@Override
						public Serializable execute() throws Exception {
							Peer peer = (Peer) get("peer");
							return LocalNATUtils.shutdown(peer);
						}
					} });

			unr2 = LocalNATUtils.executePeer(1, sync, 
					new Command[] { new Command() {
						// startup
						@Override
						public Serializable execute() throws Exception {
							Peer peer = LocalNATUtils.init("10.0.1.2", 5001, 1);
							put("peer", peer);
							return "initialized " + peer.peerAddress();
						}
					}, new Command() {
						// detect the NAT type
						@Override
						public Serializable execute() throws Exception {
							Peer peer = (Peer) get("peer");
							return discover(address, peer);
						}
					}, new Command() {
						//shutdown
						@Override
						public Serializable execute() throws Exception {
							Peer peer = (Peer) get("peer");
							return LocalNATUtils.shutdown(peer);
						}
					} });
			unr1.waitFor();
			unr2.waitFor();

			Assert.assertEquals(NATType.PORT_PRESERVING.toString(),
					unr1.getResult(1));
			Assert.assertEquals(NATType.NON_PRESERVING_OTHER.toString(),
					unr2.getResult(1));

		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(relayPeer);
			System.out.print(".");
			LocalNATUtils.shutdown(unr1, unr2);
			System.out.println(".");
		}
	}
}
