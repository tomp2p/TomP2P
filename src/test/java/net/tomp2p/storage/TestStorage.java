package net.tomp2p.storage;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Collection;
import java.util.SortedMap;

import junit.framework.Assert;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestStorage
{
	final private static Number160 locationKey = new Number160(10);
	final private static Number160 domainKey = new Number160(20);
	final private static Number160 content1 = new Number160(50);
	final private static Number160 content2 = new Number160(60);
	final private static Number160 content3 = new Number160(70);
	final private static Number160 content4 = new Number160(80);
	final private static Number480 key1 = new Number480(locationKey, domainKey, content1);
	final private static Number480 key2 = new Number480(locationKey, domainKey, content2);
	final private static Number480 key3 = new Number480(locationKey, domainKey, content3);
	final private static Number480 key4 = new Number480(locationKey, domainKey, content4);
	private static String DIR;

	@Before
	public void befor()
	{
		File tmpDir = Files.createTempDir();
		DIR = tmpDir.getPath();
	}

	@After
	public void after()
	{
		File f = new File(DIR);
		f.listFiles(new FileFilter()
		{
			@Override
			public boolean accept(File pathname)
			{
				if (pathname.isFile())
					pathname.delete();
				return false;
			}
		});
		f.delete();
	}

	@Test
	public void testPutInitial() throws Exception
	{
		Storage storageM = new StorageMemory();
		Storage storageD = new StorageDisk(DIR);
		store(storageM);
		store(storageD);
		storageM.close();
		storageD.close();
	}

	private void store(Storage storage) throws IOException
	{
		store(storage, null, false);
	}

	private void store(Storage storage, PublicKey publicKey, boolean protectDomain)
			throws IOException
	{
		boolean store = storage.put(key1, new Data("test1"), publicKey, false, protectDomain);
		Assert.assertEquals(true, store);
		store = storage.put(key2, new Data("test2"), publicKey, false, protectDomain);
		Assert.assertEquals(true, store);
	}

	@Test
	public void testGet() throws Exception
	{
		testGet(new StorageMemory());
		testGet(new StorageDisk(DIR));
	}

	private void testGet(Storage storage) throws IOException, ClassNotFoundException
	{
		store(storage);
		Data result1 = storage.get(key1);
		Assert.assertEquals("test1", result1.getObject());
		Data result2 = storage.get(key2);
		Assert.assertEquals("test2", result2.getObject());
		Data result3 = storage.get(key3);
		Assert.assertEquals(null, result3);
		storage.close();
	}

	@Test
	public void testPut() throws Exception
	{
		testPut(new StorageMemory());
		testPut(new StorageDisk(DIR));
	}

	private void testPut(Storage storage) throws IOException
	{
		store(storage);
		boolean store = storage.put(key1, new Data("test3"), null, false, false);
		Assert.assertEquals(true, store);
		storage.put(key3, new Data("test4"), null, false, false);
		SortedMap<Number480, Data> result = storage.get(key1, key4);
		Assert.assertEquals(3, result.size());
		storage.close();
	}

	@Test
	public void testPutIfAbsent() throws Exception
	{
		testPutIfAbsent(new StorageMemory());
		testPutIfAbsent(new StorageDisk(DIR));
	}

	private void testPutIfAbsent(Storage storage) throws IOException
	{
		store(storage);
		boolean store = storage.put(key1, new Data("test3"), null, true, false);
		Assert.assertEquals(false, store);
		storage.put(key3, new Data("test4"), null, true, false);
		SortedMap<Number480, Data> result1 = storage.get(key1, key4);
		Assert.assertEquals(3, result1.size());
		SortedMap<Number480, Data> result2 = storage.get(key1, key3);
		Assert.assertEquals(2, result2.size());
		storage.close();
	}

	@Test
	public void testRemove() throws Exception
	{
		testRemove(new StorageMemory());
		testRemove(new StorageDisk(DIR));
	}

	private void testRemove(Storage storage) throws IOException, ClassNotFoundException
	{
		store(storage);
		Data result1 = storage.remove(key1, null);
		Assert.assertEquals("test1", result1.getObject());
		SortedMap<Number480, Data> result2 = storage.get(key1, key4);
		Assert.assertEquals(1, result2.size());
		store(storage);
		SortedMap<Number480, Data> result3 = storage.remove(key1, key4, null);
		Assert.assertEquals(2, result3.size());
		SortedMap<Number480, Data> result4 = storage.get(key1, key4);
		Assert.assertEquals(0, result4.size());
		storage.close();
	}

	@Test
	public void testTTL1() throws Exception
	{
		testTTL1(new StorageDisk(DIR));
		testTTL1(new StorageMemory());
	}

	private void testTTL1(Storage storage) throws Exception
	{
		Data data = new Data("string");
		data.setTTLSeconds(0);
		storage.put(key1, data, null, false, false);
		Thread.sleep(2000);
		Data tmp = storage.get(key1);
		Assert.assertEquals(true, tmp != null);
		storage.close();
	}

	@Test
	public void testTTL2() throws Exception
	{
		testTTL2(new StorageDisk(DIR));
		testTTL2(new StorageMemory());
	}

	private void testTTL2(Storage storage) throws Exception
	{
		Data data = new Data("string");
		data.setTTLSeconds(1);
		storage.put(key1, data, null, false, false);
		Thread.sleep(2000);
		Data tmp = storage.get(key1);
		Assert.assertEquals(true, tmp == null);
		storage.close();
	}

	@Test
	public void testResponsibility() throws Exception
	{
		testResponsibility(new StorageDisk(DIR));
		testResponsibility(new StorageMemory());
	}

	private void testResponsibility(Storage storage) throws Exception
	{
		storage.updateResponsibilities(content1, locationKey);
		storage.updateResponsibilities(content2, locationKey);
		Assert.assertEquals(locationKey, storage.findPeerIDForResponsibleContent(content1));
		Assert.assertEquals(2, storage.findContentForResponsiblePeerID(locationKey).size());
		storage.updateResponsibilities(content1, domainKey);
		storage.updateResponsibilities(content2, locationKey);
		Assert.assertEquals(domainKey, storage.findPeerIDForResponsibleContent(content1));
		storage.close();
	}

	@Test
	public void testPublicKeyDomain() throws Exception
	{
		testPublicKeyDomain(new StorageDisk(DIR));
		testPublicKeyDomain(new StorageMemory());
	}

	private void testPublicKeyDomain(Storage storage) throws Exception
	{
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		KeyPair pair2 = gen.generateKeyPair();
		store(storage, pair1.getPublic(), true);
		boolean result1 = storage.put(key3, new Data("test4"), pair1.getPublic(), false, false);
		Assert.assertEquals(true, result1);
		boolean result3 = storage.put(key3, new Data("test6"), pair1.getPublic(), false, true);
		Assert.assertEquals(true, result3);
		// domain is protected by pair1
		boolean result2 = storage.put(key3, new Data("test5"), pair2.getPublic(), false, true);
		Assert.assertEquals(false, result2);
		storage.close();
	}

	@Test
	public void testDirectReplication() throws Exception
	{
		Assert.assertEquals(2, testDirectReplication(new StorageDisk(DIR)));
		Assert.assertEquals(0, testDirectReplication(new StorageMemory()));
	}

	private int testDirectReplication(Storage storage) throws Exception
	{
		Data data1 = new Data("test");
		data1.setDirectReplication(true);
		storage.put(key1, data1, null, false, false);
		Data data2 = new Data("test");
		data2.setDirectReplication(true);
		storage.put(key2, data2, null, false, false);
		Data data3 = new Data("test");
		storage.put(key3, data3, null, false, false);
		Collection<Number480> tmp = storage.storedDirectReplication();
		int size = tmp.size();
		storage.close();
		return size;
	}
}