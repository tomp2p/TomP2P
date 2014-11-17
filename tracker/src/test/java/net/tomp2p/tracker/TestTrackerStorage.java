package net.tomp2p.tracker;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;

import net.tomp2p.connection.PeerException;
import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestTrackerStorage {
	
	@Test
	public void testStoragePutGetVerified() throws IOException, ClassNotFoundException {
		Number160 self = Number160.ONE;
		PeerAddress selfAddress = new PeerAddress(self);
		PeerMapConfiguration pmc = new PeerMapConfiguration(self);
		PeerMap pm = new PeerMap(pmc);
		TrackerStorage trackerStorage = new TrackerStorage(10, new int[] { 10 }, 1, pm, selfAddress, false);

		Number320 n320 = new Number320(Number160.ZERO, Number160.ZERO);

		boolean retVal = trackerStorage.put(n320, selfAddress, null, new Data("test"));
		Assert.assertTrue(retVal);
		//
		TrackerData td = trackerStorage.trackerData(n320);
		Object o = td.peerAddresses().values().iterator().next().object();
		Assert.assertEquals(o, "test");
	}
	
	@Test
	public void testStoragePutGetUnverified() throws IOException, ClassNotFoundException {
		Number160 self = Number160.ONE;
		PeerAddress selfAddress = new PeerAddress(self);
		PeerMapConfiguration pmc = new PeerMapConfiguration(self);
		PeerMap pm = new PeerMap(pmc);
		TrackerStorage trackerStorage = new TrackerStorage(10, new int[] { 10 }, 1, pm, selfAddress, true);

		Number320 n320 = new Number320(Number160.ZERO, Number160.ZERO);

		boolean retVal = trackerStorage.put(n320, selfAddress, null, new Data("test"));
		Assert.assertTrue(retVal);
		//
		TrackerData td = trackerStorage.trackerData(n320);
		Assert.assertTrue(td.isEmpty());
	}
	
	@Test
	public void testStoragePutGetUnverifiedVerified() throws IOException, ClassNotFoundException {
		Number160 self = Number160.ONE;
		PeerAddress selfAddress = new PeerAddress(self);
		PeerMapConfiguration pmc = new PeerMapConfiguration(self);
		PeerMap pm = new PeerMap(pmc);
		TrackerStorage trackerStorage = new TrackerStorage(Integer.MAX_VALUE, new int[] { 10 }, 1, pm, selfAddress, true);

		Number320 n320 = new Number320(Number160.ZERO, Number160.ZERO);

		trackerStorage.put(n320, selfAddress, null, new Data("test"));
		trackerStorage.peerFound(selfAddress, null, null);
		
		TrackerData td = trackerStorage.trackerData(n320);
		Object o = td.peerAddresses().values().iterator().next().object();
		Assert.assertEquals(o, "test");
	}
	
	@Test
	public void testStoragePutGetUnverifiedVerifiedSecurity() throws IOException, ClassNotFoundException, NoSuchAlgorithmException {
		Number160 self = Number160.ONE;
		PeerAddress selfAddress = new PeerAddress(self);
		PeerMapConfiguration pmc = new PeerMapConfiguration(self);
		PeerMap pm = new PeerMap(pmc);
		TrackerStorage trackerStorage = new TrackerStorage(Integer.MAX_VALUE, new int[] { 10 }, 1, pm, selfAddress, true);

		Number320 n320 = new Number320(Number160.ZERO, Number160.ZERO);

		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		
		trackerStorage.put(n320, selfAddress, pair1.getPublic(), new Data("test"));
		trackerStorage.peerFound(selfAddress, null, null);
		trackerStorage.put(n320, selfAddress, pair1.getPublic(), new Data("test1"));
		
		TrackerData td = trackerStorage.trackerData(n320);
		Object o = td.peerAddresses().values().iterator().next().object();
		Assert.assertEquals(o, "test1");
	}
	
	@Test
	public void testTrackerRemove1() throws IOException {
		Number160 self = Number160.ONE;
		PeerAddress selfAddress = new PeerAddress(self);
		PeerMapConfiguration pmc = new PeerMapConfiguration(self);
		PeerMap pm = new PeerMap(pmc);
		TrackerStorage trackerStorage = new TrackerStorage(10, new int[] { 10 }, 1, pm, selfAddress, false);

		Number320 n320 = new Number320(Number160.ZERO, Number160.ZERO);

		trackerStorage.put(n320, selfAddress, null, new Data("test"));
		trackerStorage.peerFailed(selfAddress, new PeerException(PeerException.AbortCause.PEER_ABORT, ""));
		Assert.assertEquals(0, trackerStorage.size());
	}
	
	@Test
	public void testTrackerRemove2() throws IOException {
		Number160 self = Number160.ONE;
		PeerAddress selfAddress = new PeerAddress(self);
		PeerMapConfiguration pmc = new PeerMapConfiguration(self);
		PeerMap pm = new PeerMap(pmc);
		TrackerStorage trackerStorage = new TrackerStorage(10, new int[] { 10 }, 1, pm, selfAddress, true);

		Number320 n320 = new Number320(Number160.ZERO, Number160.ZERO);

		trackerStorage.put(n320, selfAddress, null, new Data("test"));
		trackerStorage.peerFailed(selfAddress, new PeerException(PeerException.AbortCause.PEER_ABORT, ""));
		Assert.assertEquals(0, trackerStorage.sizeUnverified());
	}
	
	@Test
	public void testTrackerMaintenance1() throws IOException {
		Number160 self = Number160.ONE;
		PeerAddress selfAddress = new PeerAddress(self);
		PeerMapConfiguration pmc = new PeerMapConfiguration(self);
		PeerMap pm = new PeerMap(pmc);
		TrackerStorage trackerStorage = new TrackerStorage(10, new int[] { 10 }, 1, pm, selfAddress, true);

		Number320 n320 = new Number320(Number160.ZERO, Number160.ZERO);

		trackerStorage.put(n320, selfAddress, null, new Data("test"));
		PeerStatistic ps = trackerStorage.nextForMaintenance(null);
		Assert.assertEquals(ps.peerAddress().peerId(), self);	
	}
	
	@Test
	public void testTrackerMaintenance2() throws IOException {
		Number160 self = Number160.ONE;
		PeerAddress selfAddress = new PeerAddress(self);
		PeerMapConfiguration pmc = new PeerMapConfiguration(self);
		PeerMap pm = new PeerMap(pmc);
		TrackerStorage trackerStorage = new TrackerStorage(10, new int[] { 10 }, 1, pm, selfAddress, true);

		Number320 n320 = new Number320(Number160.ZERO, Number160.ZERO);

		trackerStorage.put(n320, selfAddress, null, new Data("test"));
		PeerStatistic ps = trackerStorage.nextForMaintenance(null);
		trackerStorage.peerFound(selfAddress, null, null);
		ps = trackerStorage.nextForMaintenance(null);
		Assert.assertNull(ps);
	}
}
