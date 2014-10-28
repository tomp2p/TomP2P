package net.tomp2p.peers;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.Utils2;

import org.junit.Assert;
import org.junit.Test;

public class TestFilter {
	@Test
	public void testIPv4Filter1() throws UnknownHostException {
		PeerIPFilter filter = new PeerIPFilter(32, 128);
		
		PeerAddress pa1 = Utils2.createAddressIP("192.168.1.1");
		PeerAddress pa2 = Utils2.createAddressIP("192.168.1.2");
		PeerAddress pa3 = Utils2.createAddressIP("2001:0db8:0:f101::1");
		PeerAddress pa4 = Utils2.createAddressIP("2001:0db8:0:f101::2");
		
		Collection<PeerAddress> all = new ArrayList<PeerAddress>();
		all.add(pa2);
		all.add(pa4);
		
		Assert.assertFalse(filter.reject(pa1, all, null));
		Assert.assertFalse(filter.reject(pa3, all, null));
		
		Assert.assertTrue(filter.reject(pa2, all, null));
		Assert.assertTrue(filter.reject(pa4, all, null));
		
	}
	
	@Test
	public void testIPv4Filter2() throws UnknownHostException {
		PeerIPFilter filter = new PeerIPFilter(24, 104);
		
		PeerAddress pa1 = Utils2.createAddressIP("192.168.1.1");
		PeerAddress pa2 = Utils2.createAddressIP("192.168.1.2");
		PeerAddress pa3 = Utils2.createAddressIP("10.168.1.1");
		
		PeerAddress pa4 = Utils2.createAddressIP("2001:0db8:0:f101::1");
		PeerAddress pa5 = Utils2.createAddressIP("2001:0db8:0:f101::2");
		PeerAddress pa6 = Utils2.createAddressIP("1001:0db8:0:f101::2");
		
		Collection<PeerAddress> all = new ArrayList<PeerAddress>();
		all.add(pa2);
		all.add(pa4);
		
		Assert.assertTrue(filter.reject(pa1, all, null));
		Assert.assertTrue(filter.reject(pa4, all, null));
		
		Assert.assertTrue(filter.reject(pa2, all, null));
		Assert.assertTrue(filter.reject(pa5, all, null));
		
		Assert.assertFalse(filter.reject(pa3, all, null));
		Assert.assertFalse(filter.reject(pa6, all, null));
	}
	
	/*@Test
	public void testPeerStatFilter() throws UnknownHostException {
		Random rnd = new Random(42);
		Number160 key = new Number160(rnd);
		PeerMapConfiguration conf = new PeerMapConfiguration(key);
		PeerMap peerMap = new PeerMap(conf);
		
		Statistics statistics = new Statistics(peerMap);
		
		PeerStatRoutingFilter psrf = new PeerStatRoutingFilter(statistics, 6);
		
		conf.addPeerFilter(pf);
		
		for(int i=0;i<10000;i++) {
			PeerAddress pa = Utils2.createAddress(new Number160(rnd));
			Assert.assertTrue(peerMap.peerFound(pa, null));
			System.err.println("i:"+i);
		}
		System.err.println("key: "+key);
		Number160 attack = new Number160("0xba419d350dfe8af7aee7bbe10c45c0284f083ce1");
		PeerAddress pa = Utils2.createAddress(attack);
		Assert.assertFalse(peerMap.peerFound(pa, null));
		
		
		
		Peer master = null;
		try {
			Peer[] peers = Utils2.createNodes(100, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			
			+
			
			final Data data1 = new Data(new byte[1]);
			data1.ttlSeconds(3);
			FuturePut futurePut = master.put(Number160.createHash("test")).setVersionKey(Number160.MAX_VALUE)
			        .setData(data1).start();
			futurePut.awaitUninterruptibly();
			Assert.assertEquals(true, futurePut.isSuccess());
			//
			Map<Number640, Data> map = peers[0].getPeerBean().storage().get();
			Assert.assertEquals(Number160.MAX_VALUE, map.entrySet().iterator().next().getKey().getVersionKey());
			LOG.error("done");
		} finally {
			if (master != null) {
				master.shutdown().awaitListenersUninterruptibly();
			}
		}
		 
		
		
	}*/
}
