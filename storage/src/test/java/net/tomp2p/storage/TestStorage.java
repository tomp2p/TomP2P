package net.tomp2p.storage;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.StorageLayer.PutStatus;

import org.junit.Assert;
import org.junit.Test;

/**
 * This is an exact copy of TestStorage in the core package. I was not able to
 * reference this with maven, m2e and being able to make a release.
 * 
 * @author Thomas Bocek
 * 
 */
public class TestStorage {
	final private static Number160 locationKey = new Number160(10);

	final private static Number160 domainKey = new Number160(20);

	final private static Number160 content1 = new Number160(50);

	final private static Number160 content2 = new Number160(60);

	final private static Number160 content3 = new Number160(70);

	final private static Number160 content4 = new Number160(80);

	final private Number640 key1 = new Number640(locationKey, domainKey, content1, Number160.ZERO);
	final private Number640 key2 = new Number640(locationKey, domainKey, content2, Number160.ZERO);
	final private Number640 key3 = new Number640(locationKey, domainKey, content3, Number160.ZERO);
	final private Number640 key4 = new Number640(locationKey, domainKey, content4, Number160.ZERO);

	public Storage createStorage() throws IOException {
		return new StorageMemory();
	}

	@Test
	public void testPutInitial() throws Exception {
		Storage storageM = createStorage();
		store(new StorageLayer(storageM));
		storageM.close();
	}

	private void store(StorageLayer storage) throws IOException {
		store(storage, null, false);
	}

	private void store(StorageLayer storage, int nr) throws IOException {
		Enum<?> store = storage.put(key1, new Data("test1"), null, false, false);
		Assert.assertEquals(PutStatus.OK, store);
		store = storage.put(key2, new Data("test2"), null, false, false);
		Assert.assertEquals(PutStatus.OK, store);
	}

	private void store(StorageLayer storage, PublicKey publicKey, boolean protectDomain) throws IOException {
		Enum<?> store = storage.put(key1, new Data("test1"), publicKey, false, protectDomain);
		Assert.assertEquals(PutStatus.OK, store);
		store = storage.put(key2, new Data("test2"), publicKey, false, protectDomain);
		Assert.assertEquals(PutStatus.OK, store);
	}

	@Test
	public void testGet() throws Exception {
		Storage storageM = createStorage();
		testGet(new StorageLayer(storageM));
		storageM.close();
	}

	private void testGet(StorageLayer storage) throws IOException, ClassNotFoundException {
		store(storage);
		Data result1 = storage.get(key1);
		Assert.assertEquals("test1", result1.object());
		Data result2 = storage.get(key2);
		Assert.assertEquals("test2", result2.object());
		Data result3 = storage.get(key3);
		Assert.assertEquals(null, result3);
	}

	@Test
	public void testPut() throws Exception {
		Storage storageM = createStorage();
		testPut(new StorageLayer(storageM));
		storageM.close();
	}

	private void testPut(StorageLayer storage) throws IOException {
		store(storage);
		Enum<?> store = storage.put(key1, new Data("test3"), null, false, false);
		Assert.assertEquals(PutStatus.OK, store);
		storage.put(key3, new Data("test4"), null, false, false);
		SortedMap<Number640, Data> result = storage.get(key1, key4, -1, true);
		Assert.assertEquals(3, result.size());
	}

	@Test
	public void testPutIfAbsent() throws Exception {
		Storage storageM = createStorage();
		testPutIfAbsent(new StorageLayer(storageM));
		storageM.close();
	}

	private void testPutIfAbsent(StorageLayer storage) throws IOException {
		store(storage);
		Enum<?> store = storage.put(key1, new Data("test3"), null, true, false);
		Assert.assertEquals(PutStatus.FAILED_NOT_ABSENT, store);
		storage.put(key3, new Data("test4"), null, true, false);
		SortedMap<Number640, Data> result1 = storage.get(key1, key3, -1, true);
		Assert.assertEquals(3, result1.size());
		SortedMap<Number640, Data> result2 = storage.get(key1, key2, -1, true);
		Assert.assertEquals(2, result2.size());
	}

	@Test
	public void testRemove() throws Exception {
		Storage storageM = createStorage();
		testRemove(new StorageLayer(storageM));
		storageM.close();
	}

	private void testRemove(StorageLayer storage) throws IOException, ClassNotFoundException {
		store(storage);
		Data result1 = storage.remove(key1, null, true).element0();
		Assert.assertEquals("test1", result1.object());
		SortedMap<Number640, Data> result2 = storage.get(key1, key4, -1, true);
		Assert.assertEquals(1, result2.size());
		store(storage);
		SortedMap<Number640, Data> result3 = storage.removeReturnData(key1, key4, null);
		Assert.assertEquals(2, result3.size());
		SortedMap<Number640, Data> result4 = storage.get(key1, key4, -1, true);
		Assert.assertEquals(0, result4.size());
	}

	@Test
	public void testTTL1() throws Exception {
		Storage storageM = createStorage();
		testTTL1(new StorageLayer(storageM));
		storageM.close();
	}

	private void testTTL1(StorageLayer storage) throws Exception {
		Data data = new Data("string");
		data.ttlSeconds(0);
		storage.put(key1, data, null, false, false);
		Thread.sleep(2000);
		Data tmp = storage.get(key1);
		Assert.assertEquals(true, tmp != null);
	}

	@Test
	public void testTTL2() throws Exception {
		Storage storageM = createStorage();
		testTTL2(new StorageLayer(storageM));
		storageM.close();
	}

	private void testTTL2(StorageLayer storage) throws Exception {
		Data data = new Data("string");
		data.ttlSeconds(1);
		storage.put(key1, data, null, false, false);
		Thread.sleep(2000);
		storage.checkTimeout();
		Data tmp = storage.get(key1);
		Assert.assertEquals(true, tmp == null);
	}

	@Test
	public void testTTLLeak() throws Exception {
		Storage storageM = createStorage();
		testTTLLeak(new StorageLayer(storageM));
		Assert.assertEquals(0, storageM.subMapTimeout(Long.MAX_VALUE).size());
		storageM.close();
	}

	private void testTTLLeak(StorageLayer storage) throws Exception {
		Data data = new Data("string");
		data.ttlSeconds(1);
		storage.put(key1, data, null, false, false);
		Thread.sleep(2000);
		storage.checkTimeout();
		Data tmp = storage.get(key1);
		Assert.assertEquals(true, tmp == null);
	}

	@Test
	public void testResponsibility() throws Exception {
		Storage storageM = createStorage();
		testResponsibility(storageM);
		storageM.close();
	}

	private void testResponsibility(Storage storage) throws Exception {
		storage.updateResponsibilities(content1, locationKey);
		storage.updateResponsibilities(content2, locationKey);
		Assert.assertEquals(locationKey, storage.findPeerIDsForResponsibleContent(content1).iterator().next());
		Assert.assertEquals(2, storage.findContentForResponsiblePeerID(locationKey).size());
		storage.updateResponsibilities(content1, domainKey);
		storage.updateResponsibilities(content2, locationKey);
		Assert.assertEquals(domainKey, storage.findPeerIDsForResponsibleContent(content1).iterator().next());
	}

	@Test
	public void testPublicKeyDomain() throws Exception {
		Storage storageM = createStorage();
		testPublicKeyDomain(new StorageLayer(storageM));
		storageM.close();
	}

	private void testPublicKeyDomain(StorageLayer storage) throws Exception {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		KeyPair pair2 = gen.generateKeyPair();
		store(storage, pair1.getPublic(), true);
		Enum<?> result1 = storage.put(key3, new Data("test4"), pair1.getPublic(), false, false);
		Assert.assertEquals(PutStatus.OK, result1);
		Enum<?> result3 = storage.put(key3, new Data("test6"), pair1.getPublic(), false, true);
		Assert.assertEquals(PutStatus.OK, result3);
		// domain is protected by pair1
		Enum<?> result2 = storage.put(key3, new Data("test5"), pair2.getPublic(), false, true);
		Assert.assertEquals(PutStatus.FAILED_SECURITY, result2);
	}

	@Test
	public void testLock1() {
		KeyLock<Number160> lock = new KeyLock<Number160>();
		KeyLock<Number160>.RefCounterLock tmp = lock.lock(Number160.createHash("test"));
		Assert.assertEquals(1, lock.cacheSize());
		lock.unlock(tmp);
		Assert.assertEquals(0, lock.cacheSize());
		lock.unlock(tmp);
	}

	@Test
	public void testLock2() {
		KeyLock<Number160> lock = new KeyLock<Number160>();
		KeyLock<Number160>.RefCounterLock tmp1 = lock.lock(Number160.createHash("test1"));
		KeyLock<Number160>.RefCounterLock tmp2 = lock.lock(Number160.createHash("test2"));
		Assert.assertEquals(2, lock.cacheSize());
		lock.unlock(tmp1);
		lock.unlock(tmp2);
		Assert.assertEquals(0, lock.cacheSize());
	}

	@Test
	public void testLockConcurrent() throws InterruptedException {
		final KeyLock<Number160> lock = new KeyLock<Number160>();
		for (int i = 0; i < 100; i++) {
			final int ii = i;
			new Thread(new Runnable() {
				@Override
				public void run() {
					KeyLock<Number160>.RefCounterLock tmp1 = lock.lock(Number160.createHash("test1"));
					KeyLock<Number160>.RefCounterLock tmp2 = lock.lock(Number160.createHash("test2"));
					lock.unlock(tmp1);
					lock.unlock(tmp2);
					System.err.print("a" + ii + " ");
				}
			}).start();
		}
		for (int i = 0; i < 100; i++) {
			final int ii = i;
			new Thread(new Runnable() {
				@Override
				public void run() {
					KeyLock<Number160>.RefCounterLock tmp1 = lock.lock(Number160.createHash("test3"));
					KeyLock<Number160>.RefCounterLock tmp2 = lock.lock(Number160.createHash("test4"));
					lock.unlock(tmp1);
					lock.unlock(tmp2);
					System.err.print("b" + ii + " ");
				}
			}).start();
		}
		Thread.sleep(500);
		Assert.assertEquals(0, lock.cacheSize());
		KeyLock<Number160>.RefCounterLock tmp1 = lock.lock(Number160.createHash("test1"));
		KeyLock<Number160>.RefCounterLock tmp2 = lock.lock(Number160.createHash("test2"));
		Assert.assertEquals(2, lock.cacheSize());
		lock.unlock(tmp1);
		Assert.assertEquals(1, lock.cacheSize());
		lock.unlock(tmp2);
		Assert.assertEquals(0, lock.cacheSize());
	}

	@Test
	public void testConcurrency() throws InterruptedException, IOException {
		final Storage sM = createStorage();
		final StorageLayer storageGeneric = new StorageLayer(sM);
		store(storageGeneric);
		final AtomicInteger counter = new AtomicInteger();
		final Data result1 = storageGeneric.get(key1);
		for (int i = 0; i < 10; i++) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						Assert.assertEquals("test1", result1.object());
					} catch (Throwable t) {
						t.printStackTrace();
						counter.incrementAndGet();
					}
				}
			}).start();
		}
		Thread.sleep(500);
		Assert.assertEquals(0, counter.get());
		sM.close();
	}

	@Test
	public void testConcurrency2() throws InterruptedException, IOException {
		final Storage sM = createStorage();
		final StorageLayer storageGeneric = new StorageLayer(sM);
		store(storageGeneric);
		final AtomicInteger counter = new AtomicInteger();
		for (int i = 0; i < 100; i++) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					Data result1 = null;
					Data result2 = null;
					Data result3 = null;
					try {
						result1 = storageGeneric.get(key1);
						Assert.assertEquals("test1", result1.object());
						result3 = storageGeneric.get(key3);
						Assert.assertEquals(null, result3);
						store(storageGeneric, 1);
					} catch (Throwable t) {
						t.printStackTrace();
						counter.incrementAndGet();
					}
					try {
						result2 = storageGeneric.get(key2);
						Assert.assertEquals("test2", result2.object());
						result3 = storageGeneric.get(key3);
						Assert.assertEquals(null, result3);
						store(storageGeneric, 1);
					} catch (Throwable t) {
						t.printStackTrace();
						counter.incrementAndGet();
					}
				}
			}).start();
		}
		Thread.sleep(500);
		Assert.assertEquals(0, counter.get());
		sM.close();
	}
}
