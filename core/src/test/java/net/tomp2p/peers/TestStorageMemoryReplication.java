package net.tomp2p.peers;

import net.tomp2p.storage.StorageMemory;

import org.junit.Assert;
import org.junit.Test;

public class TestStorageMemoryReplication {
    @Test
    public void testStorageMemoryReplication1() {
        StorageMemory storageMemoryReplication = new StorageMemory();
        Number160 testLoc = Number160.createHash("test1");
        Number160 testPer = Number160.createHash("test2");
        storageMemoryReplication.updateResponsibilities(testLoc, testPer);
        Assert.assertEquals(testPer, storageMemoryReplication.findPeerIDForResponsibleContent(testLoc));
    }

    @Test
    public void testStorageMemoryReplication2() {
    	StorageMemory storageMemoryReplication = new StorageMemory();
        Number160 testLoc = Number160.createHash("test1");
        Number160 testPer = Number160.createHash("test2");
        storageMemoryReplication.updateResponsibilities(testLoc, testPer);
        Assert.assertEquals(testLoc, storageMemoryReplication.findContentForResponsiblePeerID(testPer).iterator()
                .next());
    }

    @Test
    public void testStorageMemoryReplication3() {
    	StorageMemory storageMemoryReplication = new StorageMemory();
        Number160 testLoc = Number160.createHash("test1");
        Number160 testPer = Number160.createHash("test2");
        storageMemoryReplication.updateResponsibilities(testLoc, testPer);
        storageMemoryReplication.updateResponsibilities(testLoc, testPer);
        Assert.assertEquals(testPer, storageMemoryReplication.findPeerIDForResponsibleContent(testLoc));
        Assert.assertEquals(testLoc, storageMemoryReplication.findContentForResponsiblePeerID(testPer).iterator()
                .next());
    }

    @Test
    public void testStorageMemoryReplication4() {
    	StorageMemory storageMemoryReplication = new StorageMemory();
        Number160 testLoc = Number160.createHash("test1");
        Number160 testPer = Number160.createHash("test2");
        storageMemoryReplication.updateResponsibilities(testLoc, testPer);
        storageMemoryReplication.updateResponsibilities(testLoc, testPer);
        storageMemoryReplication.removeResponsibility(testLoc);
        Assert.assertEquals(null, storageMemoryReplication.findPeerIDForResponsibleContent(testLoc));
        Assert.assertEquals(0, storageMemoryReplication.findContentForResponsiblePeerID(testPer).size());
    }
}
