package net.tomp2p.holep.manual;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FuturePing;
import net.tomp2p.nat.FutureNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Add the following lines to sudoers
 * username ALL=(ALL) NOPASSWD: <location>/nat-net.sh
 * username ALL=(ALL) NOPASSWD: /usr/bin/ip
 * 
 * Make sure the network namespaces can resolve the hostname, otherwise huge delays are to be expected.
 * 
 * This testcase runs on a single machine and tests the two widely used NAT settings (port-preserving and symmetric). 
 * However, most likely this will never run on travis-ci as this requires some extra setup on the machine itself. 
 * Thus, this test-case is disabled by default and tests have to be performed manully.
 * 
 * @author Thomas Bocek
 *
 */
//travis-ci cannot test this, the kernel does not support all the required features:
//Perhaps iptables or your kernel needs to be upgraded
//see also here: https://github.com/travis-ci/travis-ci/issues/1341
@Ignore
public class TestNATLocal implements Serializable {
	
	private static final long serialVersionUID = 1L;

	//### CHANGE THIS TO YOUR INTERFACE###
	final static private String INF = "enp0s25";
	
	final static private Random RND = new Random(42);
	static private Number160 relayPeerId = new Number160(RND);
	
	@Before
	public void before() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("start", "0");
		LocalNATUtils.executeNatSetup("upnp", "0");
	}

	@After
	public void after() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("stop", "0");
	}

	
	@SuppressWarnings("serial")
	@Test
	public void testLocalSend() throws Exception {
		Peer relayPeer = null;
		RemotePeer unr1 = null;
		try {
			relayPeer = LocalNATUtils.createRealNode(relayPeerId, INF, 5002);
			final PeerSocket4Address relayAddress = relayPeer.peerAddress().ipv4Socket();
			
			CommandSync sync = new CommandSync(1);
			unr1 = LocalNATUtils.executePeer(0, sync, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.init("10.0.0.2", 5000, 0);
					Peer peer2 = LocalNATUtils.init("10.0.0.3", 5001, 1);
					put("p1", peer1);
					put("p2", peer2);
					
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					FutureDiscover fd2 = peer2.discover().peerSocketAddress(relayAddress).start().awaitUninterruptibly();
					PeerNAT pn1 = new PeerBuilderNAT(peer1).start();
					PeerNAT pn2 = new PeerBuilderNAT(peer2).start();
					FutureNAT fn1 = pn1.portForwarding(fd1).awaitUninterruptibly();
					FutureNAT fn2 = pn2.portForwarding(fd2).awaitUninterruptibly();
					
					System.err.println("fn: "+fn1.failedReason());
					Assert.assertTrue(fn1.isSuccess());
					
					StringBuilder sb = new StringBuilder();
					if(fn1.isSuccess() && fn2.isSuccess()) {
						// now peer1 and peer2 know each other locally.
						PeerAddress punr2 = peer2.peerAddress();
						InetAddress internal = Inet4Address.getByName("0.0.0.3");
						punr2 = punr2.withIpInternalSocket((PeerSocket4Address)PeerSocketAddress.create(internal, 5001, 5001, 5002));
						//TODO: mark reachable
						FuturePing fp1 = peer1.ping().peerAddress(punr2).start().awaitUninterruptibly();
						sb.append(fp1.isSuccess());
						System.out.println(fp1.failedReason() + " /" + fp1.remotePeer());
						FuturePing fp2 = peer1.ping().tcpPing().peerAddress(punr2).start().awaitUninterruptibly();
						sb.append(fp2.isSuccess());
						System.out.println(fp1.failedReason() + " /" + fp2.remotePeer());
						sb.append(fp2.isSuccess());
						System.out.println(peer1.peerBean().peerMap().getPeerStatistic(punr2).peerAddress());
						sb.append(peer1.peerBean().peerMap().getPeerStatistic(punr2).peerAddress());
					} else {
						System.err.println("failed: "+ fn1.failedReason() + fn2.failedReason());
						return fn1.failedReason() + fn2.failedReason();
					}
					
					return sb.toString();
				}
			}, new Command() {
				
				@Override
				public Serializable execute() throws Exception {
					return LocalNATUtils.shutdown((Peer)get("p1"), (Peer)get("p2"));
				}
			});
			unr1.waitFor();
			Assert.assertEquals("truetruetrue/172.20.0.1", unr1.getResult(0));
		} finally {
			System.out.print("LOCAL> shutdown.");
			LocalNATUtils.shutdown(relayPeer);
			System.out.print(".");
			LocalNATUtils.shutdown(unr1);
			System.out.println(".");
		}
	}
}
