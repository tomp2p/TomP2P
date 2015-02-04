package net.tomp2p.storage;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

public class TestRangeLock {
	@Test
	public void testRangeLockOverlapping() throws InterruptedException {
		final CountDownLatch cd = new CountDownLatch(1);
		final RangeLock<Integer> r = new RangeLock<Integer>();

		RangeLock<Integer>.Range lock = r.lock(1, 2);
		Assert.assertEquals(2, r.size());
		new Thread(new Runnable() {
			@Override
			public void run() {
				RangeLock<Integer>.Range rr = r.lock(2, 3);
				Assert.assertEquals(2, r.size());
				r.unlock(rr);
				Assert.assertEquals(0, r.size());
				cd.countDown();
			}
		}).start();
		Thread.sleep(500);
		r.unlock(lock);

		cd.await();
	}

	@Test
	public void testRangeLockNonOverlapping() throws InterruptedException {
		final CountDownLatch cd = new CountDownLatch(1);
		final RangeLock<Integer> r = new RangeLock<Integer>();

		RangeLock<Integer>.Range lock = r.lock(1, 2);
		Assert.assertEquals(2, r.size());
		new Thread(new Runnable() {
			@Override
			public void run() {
				RangeLock<Integer>.Range rr = r.lock(3, 4);
				Assert.assertEquals(4, r.size());
				r.unlock(rr);
				cd.countDown();
			}
		}).start();
		cd.await();
		r.unlock(lock);
		Assert.assertEquals(0, r.size());
	}
	
	@Test
	public void testTreeMap() {
		final NavigableMap<Integer, Integer> cache = new TreeMap<Integer, Integer>();
		
		cache.put(10, 10);
		cache.put(20, 20);
		cache.put(30, 30);
		cache.put(40, 40);
		
		NavigableMap<Integer, Integer> tmp = cache.subMap(20, true, 30, true);
		Assert.assertEquals(2, tmp.size());
		tmp.put(25, 25);
		Assert.assertEquals(3, tmp.size());
		
	}
}
