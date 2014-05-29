package net.tomp2p.peers;

import net.tomp2p.storage.StorageMemoryReplicationNRoot;

import org.junit.Assert;
import org.junit.Test;

public class TestStorageMemoryReplicationNRoot {

	@Test
	public void testStorageMemoryReplication1() {
		StorageMemoryReplicationNRoot storageMemoryReplication = new StorageMemoryReplicationNRoot();
		Number160 testLoc = Number160.createHash("test1");
		Number160 testPer = Number160.createHash("test2");
		storageMemoryReplication.updateResponsibilities(testLoc, testPer);
		Assert.assertEquals(testPer, storageMemoryReplication.findPeerIDsForResponsibleContent(testLoc)
				.iterator().next());
	}

	@Test
	public void testStorageMemoryReplication2() {
		StorageMemoryReplicationNRoot storageMemoryReplication = new StorageMemoryReplicationNRoot();
		Number160 testLoc = Number160.createHash("loc1");
		Number160 testPer1 = Number160.createHash("peer1");
		Number160 testPer2 = Number160.createHash("peer2");
		storageMemoryReplication.updateResponsibilities(testLoc, testPer1);
		storageMemoryReplication.updateResponsibilities(testLoc, testPer2);
		Assert.assertFalse(storageMemoryReplication.updateResponsibilities(testLoc, testPer1));
		Assert.assertFalse(storageMemoryReplication.updateResponsibilities(testLoc, testPer2));
	}

	@Test
	public void testStorageMemoryReplication3() {
		StorageMemoryReplicationNRoot storageMemoryReplication = new StorageMemoryReplicationNRoot();
		Number160 testLoc = Number160.createHash("test1");
		Number160 testPer = Number160.createHash("test2");
		storageMemoryReplication.updateResponsibilities(testLoc, testPer);
		Assert.assertEquals(testLoc, storageMemoryReplication.findContentForResponsiblePeerID(testPer)
				.iterator().next());
	}

	@Test
	public void testStorageMemoryReplication4() {
		StorageMemoryReplicationNRoot storageMemoryReplication = new StorageMemoryReplicationNRoot();
		Number160 testLoc = Number160.createHash("loc1");
		Number160 testPer1 = Number160.createHash("peer1");
		Number160 testPer2 = Number160.createHash("peer2");
		storageMemoryReplication.updateResponsibilities(testLoc, testPer1);
		storageMemoryReplication.updateResponsibilities(testLoc, testPer2);
		Assert.assertEquals(testLoc, storageMemoryReplication.findContentForResponsiblePeerID(testPer1)
				.iterator().next());
		Assert.assertEquals(testLoc, storageMemoryReplication.findContentForResponsiblePeerID(testPer2)
				.iterator().next());
	}

	@Test
	public void testStorageMemoryReplication5() {
		StorageMemoryReplicationNRoot storageMemoryReplication = new StorageMemoryReplicationNRoot();
		Number160 testLoc = Number160.createHash("test1");
		Number160 testPer = Number160.createHash("test2");
		storageMemoryReplication.updateResponsibilities(testLoc, testPer);
		storageMemoryReplication.updateResponsibilities(testLoc, testPer);
		Assert.assertEquals(testPer, storageMemoryReplication.findPeerIDsForResponsibleContent(testLoc)
				.iterator().next());
		Assert.assertEquals(testLoc, storageMemoryReplication.findContentForResponsiblePeerID(testPer)
				.iterator().next());
	}

	@Test
	public void testStorageMemoryReplication6() {
		StorageMemoryReplicationNRoot storageMemoryReplication = new StorageMemoryReplicationNRoot();
		Number160 testLoc = Number160.createHash("test1");
		Number160 testPer = Number160.createHash("test2");
		storageMemoryReplication.updateResponsibilities(testLoc, testPer);
		storageMemoryReplication.updateResponsibilities(testLoc, testPer);
		storageMemoryReplication.removeResponsibility(testLoc);
		Assert.assertTrue(storageMemoryReplication.findPeerIDsForResponsibleContent(testLoc).isEmpty());
		Assert.assertEquals(0, storageMemoryReplication.findContentForResponsiblePeerID(testPer).size());
	}

	@Test
	public void testStorageMemoryReplication7() {
		StorageMemoryReplicationNRoot storageMemoryReplication = new StorageMemoryReplicationNRoot();
		Number160 testLoc = Number160.createHash("test1");
		Number160 testPer1 = Number160.createHash("test2");
		Number160 testPer2 = Number160.createHash("test3");
		storageMemoryReplication.updateResponsibilities(testLoc, testPer1);
		storageMemoryReplication.updateResponsibilities(testLoc, testPer2);
		storageMemoryReplication.removeResponsibility(testLoc);
		Assert.assertTrue(storageMemoryReplication.findPeerIDsForResponsibleContent(testLoc).isEmpty());
		Assert.assertEquals(0, storageMemoryReplication.findContentForResponsiblePeerID(testPer1).size());
		Assert.assertEquals(0, storageMemoryReplication.findContentForResponsiblePeerID(testPer2).size());
	}

}
