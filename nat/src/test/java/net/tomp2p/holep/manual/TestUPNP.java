package net.tomp2p.holep.manual;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.nat.FutureNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.storage.Data;

public class TestUPNP implements Serializable {
	
	final static private Random RND = new Random(42);
	static private Number160 relayPeerId = new Number160(RND);
	//### CHANGE THIS TO YOUR INTERFACE###
	final static private String INF = "enp0s25";
	
	@Before
	public void before() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("start", "0", "sym");
		LocalNATUtils.executeNatSetup("start", "1", "sym");
		LocalNATUtils.executeNatSetup("upnp", "0");
		LocalNATUtils.executeNatSetup("upnp", "1");
	}

	@After
	public void after() throws IOException, InterruptedException {
		LocalNATUtils.executeNatSetup("stop", "0");
		LocalNATUtils.executeNatSetup("stop", "1");
	}
	
	@Test
	public void testUPNP() throws Exception {
		Peer relayPeer = null;
		PeerDHT pd = null;
		RemotePeer unr1 = null;
		RemotePeer unr2 = null;
		try {
			relayPeer = LocalNATUtils.createRealNode(relayPeerId, INF, 5002);
			pd = new PeerBuilderDHT(relayPeer).start();
			final PeerSocketAddress relayAddress = relayPeer.peerAddress().peerSocketAddress();
			final PeerAddress relay = relayPeer.peerAddress();
			System.out.println("relay peer at: "+relay);

			CommandSync sync = new CommandSync(2);
			
			unr1 = LocalNATUtils.executePeer(0, sync, new Command() {
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.0.2", 5000, 0, "peer1");
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start();
					PeerNAT pn = new PeerBuilderNAT(peer1).start();
					
					FutureNAT fn = pn.portForwarding(fd1).awaitUninterruptibly();
					System.err.println("fn: "+fn.failedReason());
					Assert.assertTrue(fn.isSuccess());
					
					BaseFuture fb1 = peer1.bootstrap().peerAddress(relay).start().awaitUninterruptibly();
					System.err.println(fb1.failedReason());
					Assert.assertTrue(fb1.isSuccess());
					
					PeerDHT pd = new PeerBuilderDHT(peer1).start();
					put("pd", pd);
					
					BaseFuture fb2 = peer1.bootstrap().peerAddress(relay).start().awaitUninterruptibly();
					System.err.println(fb2.failedReason());
					Assert.assertTrue(fb2.isSuccess());
					Assert.assertEquals(1, peer1.peerBean().peerMap().all().size());
					return ""+(peer1.peerBean().peerMap().all().size() == 1);
				}
				
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return "true";
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					PeerDHT pd = (PeerDHT) get("pd");
					FuturePut fp = pd.put(Number160.ONE).data(new Data("test1")).start().awaitUninterruptibly();
					Assert.assertTrue(fp.isSuccess());
					return "" + fp.isSuccess();
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					//Thread.sleep(2000);
					PeerDHT pd = (PeerDHT) get("pd");
					String retVal = "" + (1 == pd.storageLayer().get().size());
					Assert.assertEquals(1, pd.storageLayer().get().size());
					System.err.println("shutdown");
					LocalNATUtils.shutdown((Peer)get("p1"));
					return retVal;
				}
			});
			
			unr2 = LocalNATUtils.executePeer(1, sync, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return "true";
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					Peer peer1 = LocalNATUtils.createNattedPeer("10.0.1.2", 5000, 1, "peer2");
					put("p1", peer1);
					FutureDiscover fd1 = peer1.discover().peerSocketAddress(relayAddress).start();
					PeerNAT pn = new PeerBuilderNAT(peer1).start();
					FutureNAT fn = pn.portForwarding(fd1).awaitUninterruptibly();
					System.err.println("fn: "+fn.failedReason());
					Assert.assertTrue(fn.isSuccess());
					
					BaseFuture fb1 = peer1.bootstrap().peerAddress(relay).start().awaitUninterruptibly();
					System.err.println(fb1.failedReason());
					Assert.assertTrue(fb1.isSuccess());
					
					PeerDHT pd = new PeerBuilderDHT(peer1).start();
					put("pd", pd);
					
					//Thread.sleep(5000);
					BaseFuture fb2 = peer1.bootstrap().peerAddress(relay).start().awaitUninterruptibly();
					System.err.println(fb2.failedReason());
					Assert.assertTrue(fb2.isSuccess());
					Assert.assertEquals(2, peer1.peerBean().peerMap().all().size());
					
					return "" + (peer1.peerBean().peerMap().all().size() == 2);
				}
				
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					return "true";
				}
			}, new Command() {
				@Override
				public Serializable execute() throws Exception {
					PeerDHT pd = (PeerDHT) get("pd");
					String retVal = "" + (1 == pd.storageLayer().get().size());
					Assert.assertEquals(1, pd.storageLayer().get().size());
					System.err.println("shutdown");
					LocalNATUtils.shutdown((Peer)get("p1"));
					return retVal;
				}
			});
			
			unr1.waitFor();
			unr2.waitFor();
			Assert.assertEquals(1, pd.storageLayer().get().size());
			
			for(int i=0;i<unr1.resultSize();i++) {
				Assert.assertEquals("true", unr1.getResult(i));
			}
			for(int i=0;i<unr2.resultSize();i++) {
				Assert.assertEquals("true", unr1.getResult(i));
			}
			
			
			} finally {
				System.out.print("LOCAL> shutdown.");
				LocalNATUtils.shutdown(relayPeer);
				System.out.print(".");
				LocalNATUtils.shutdown(unr1);
				System.out.println(".");
			}
	}
}
