package net.tomp2p.holep.manual;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.holep.NATType;
import net.tomp2p.holep.NATTypeDetection;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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
@Ignore
public class TestNATTypeDetection implements Serializable {
	
	private static final long serialVersionUID = 1L;
	final static private Random RND = new Random(42);
	final static private String INF = "wlp3s0";
	static private Peer relayPeer = null;
	static private Number160 relayPeerId = new Number160(RND);

	@Before
	public void before() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("start", "0");
		LocalNATUtils.executeNatSetup("start", "1", "sym");
	}

	@After
	public void after() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("stop", "0");
		LocalNATUtils.executeNatSetup("stop", "1");
	}

	/**
	 * If you have a terrible lag in InetAddress.getLocalHost(), make sure the
	 * hostname resolves in the other network domain.
	 * 
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
		LocalNATUtils.handleMain(args);
	}

	private static Serializable init(Command command, String ip, int port)
			throws UnknownHostException, IOException {
		Bindings b = new Bindings();
		Random rnd = new Random(0);
		b.addAddress(InetAddress.getByName(ip));
		Peer peer = new PeerBuilder(new Number160(rnd)).ports(port).bindings(b)
				.start();
		command.put("peer", peer);
		return "initialized " + peer.peerAddress();
	}

	private static Serializable discover(final String address, Peer peer)
			throws UnknownHostException {
		PeerAddress relayP = new PeerAddress(relayPeerId, address, 5002, 5002);
		FutureDone<NATType> type = NATTypeDetection.checkNATType(peer, relayP)
				.awaitUninterruptibly();
		if (type.isSuccess()) {
			return type.object().name();
		} else {
			return type.failedReason();
		}
	}

	private static Serializable shutdown(Peer peer) {
		if (peer != null) {
			peer.shutdown().awaitUninterruptibly();
		}
		return "shutdown done";
	}

	@Test
	public void testDetection() throws Exception {
		relayPeer = null;
		RemotePeer unr1 = null;
		RemotePeer unr2 = null;
		try {
			relayPeer = LocalNATUtils.createRealNode(relayPeerId, INF);
			InetAddress relayAddress = relayPeer.peerAddress().inetAddress();
			final String address = relayAddress.getHostAddress();
			unr1 = LocalNATUtils.executePeer(TestNATTypeDetection.class, "0",
					new Command[] { new Command() {
						// startup
						@Override
						public Serializable execute() throws Exception {
							return init(this, "10.0.0.2", 5000);
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
							return shutdown(peer);
						}
					} });

			unr2 = LocalNATUtils.executePeer(TestNATTypeDetection.class, "1",
					new Command[] { new Command() {
						// startup
						@Override
						public Serializable execute() throws Exception {
							return init(this, "10.0.1.2", 5001);
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
							return shutdown(peer);
						}
					} });
			unr1.waitFor();
			unr2.waitFor();

			Assert.assertEquals(NATType.PORT_PRESERVING.toString(),
					unr1.getResult(1));
			Assert.assertEquals(NATType.NON_PRESERVING_OTHER.toString(),
					unr2.getResult(1));

		} finally {
			System.err.print("shutdown.");
			if (relayPeer != null) {
				relayPeer.shutdown().awaitUninterruptibly();
				relayPeer = null;
			}
			System.err.print(".");
			if (unr1 != null) {
				LocalNATUtils.killPeer(unr1.process());
			}
			System.err.println(".");
			if (unr2 != null) {
				LocalNATUtils.killPeer(unr2.process());
			}
		}
	}
}
