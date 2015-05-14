package net.tomp2p.peers;


import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLocalMap {
	
	private PeerAddress[] addresses;
	
	@Before
	public void setup() {
		addresses = new PeerAddress[2];		
		addresses[0] = new PeerAddress(new Number160("0x5"));
		addresses[1] = new PeerAddress(new Number160("0x5"));
	}
	
	@Test
	public void testMap() {
		LocalMap map = new LocalMap(Number160.ZERO);
		boolean added = map.peerFound(addresses[0], addresses[0]);
		Assert.assertTrue(added);
		boolean offline = map.peerFailed(addresses[0], null);
		Assert.assertTrue(offline);
		PeerStatistic addr = map.translate(addresses[1]);
		Assert.assertTrue(addr == null);
	}
	
	@Test
	public void testMapMaintenance() throws InterruptedException {
		LocalMap map = new LocalMap(Number160.ZERO);
		boolean added = map.peerFound(addresses[0], addresses[0]);
		Assert.assertTrue(added);
		PeerStatistic ps = map.nextForMaintenance(Collections.<PeerAddress>emptyList());
		Assert.assertTrue(ps == null);
		Thread.sleep(2100);
		ps = map.nextForMaintenance(Collections.<PeerAddress>emptyList());
		Assert.assertTrue(ps != null);
		
	}
	
	@Test
	public void testMapTranslate() {
		LocalMap map = new LocalMap(Number160.ZERO);
		boolean added = map.peerFound(addresses[0], addresses[0]);
		Assert.assertTrue(added);
		PeerStatistic addr = map.translate(addresses[1]);
		Assert.assertTrue(addr != null);
		PeerAddress old = map.translateReverse(addresses[0]);
		Assert.assertTrue(old == addresses[1]);
	}
	
	
}
