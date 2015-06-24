package net.tomp2p.dht;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestRangeLock {
	
	@Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };
    
    @Test
	public void testRangeLockOverlapping() throws InterruptedException {
    	testRangeLockOverlapping(1, 2, 2, 3);
    	testRangeLockOverlapping(1, 4, 2, 4);
    	testRangeLockOverlapping(1, 5, 2, 6);
    	testRangeLockOverlapping(1, 5, 1, 5);
    	testRangeLockOverlapping(1, 5, 2, 4);
    	testRangeLockOverlapping(2, 4, 1, 5);
    }
    
    private void testRangeLockOverlapping(int a1, int a2, int b1, int b2) throws InterruptedException {
    	testRangeLockOverlapping1(a1, a2, b1, b2);
    	testRangeLockOverlapping2(a1, a2, b1, b2);
	}

	private void testRangeLockOverlapping1(final int a1, final int a2, final int b1, final int b2) throws InterruptedException {
		//System.err.println("test1 "+a1 +","+a2+"/"+b1+","+b2);
		final CountDownLatch cd = new CountDownLatch(1);
		final RangeLock<Integer> r = new RangeLock<Integer>();

		RangeLock<Integer>.Range lock = r.lock(a1, a2);
		Assert.assertEquals(2, r.size());
		final AtomicLong diff = new AtomicLong();
		final long start = System.currentTimeMillis();
		new Thread(new Runnable() {
			@Override
			public void run() {
				Assert.assertEquals(2, r.size());
				RangeLock<Integer>.Range rr = r.lock(b1, b2);
				diff.set(System.currentTimeMillis() - start);
				Assert.assertEquals(2, r.size());
				r.unlock(rr);
				Assert.assertEquals(0, r.size());
				cd.countDown();
			}
		}).start();
		Thread.sleep(50);
		r.unlock(lock);
		cd.await();
		Assert.assertTrue(diff.get() >= 50);
	}
    
    private void testRangeLockOverlapping2(final int a1, final int a2, final int b1, final int b2) throws InterruptedException {
    	//System.err.println("test2 "+a1 +","+a2+"/"+b1+","+b2);
		final CountDownLatch cd = new CountDownLatch(1);
		final RangeLock<Integer> r = new RangeLock<Integer>();

		RangeLock<Integer>.Range lock = r.lock(a1, a2);
		Assert.assertEquals(2, r.size());
		new Thread(new Runnable() {
			@Override
			public void run() {
				Assert.assertEquals(2, r.size());
				RangeLock<Integer>.Range lock2 = r.tryLock(b1, b2);
				Assert.assertNull(lock2);
				Assert.assertEquals(2, r.size());
				cd.countDown();
			}
		}).start();
		cd.await();
		r.unlock(lock);
		Assert.assertEquals(0, r.size());
	}
    
    @Test
   	public void testRangeLockNonOverlapping() throws InterruptedException {
    	testRangeLockNonOverlapping(1, 2, 3, 4);
    	testRangeLockNonOverlapping(3, 4, 1, 2);
       }

	private void testRangeLockNonOverlapping(final int a1, final int a2, final int b1, final int b2) throws InterruptedException {
		final CountDownLatch cd = new CountDownLatch(1);
		final RangeLock<Integer> r = new RangeLock<Integer>();

		RangeLock<Integer>.Range lock = r.lock(a1, a2);
		Assert.assertEquals(2, r.size());
		new Thread(new Runnable() {
			@Override
			public void run() {
				RangeLock<Integer>.Range rr = r.lock(b1, b2);
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
	public void testNumber640() throws InterruptedException {
		final CountDownLatch cd = new CountDownLatch(1);
		final Number640 gMin = new Number640(new Number160("0x6469ac84a89748bb67b923c833ed0c778a17aea3"), new Number160("0x0"), new Number160("0x0"), new Number160("0x0"));
		final Number640 gMax = new Number640(new Number160("0x6469ac84a89748bb67b923c833ed0c778a17aea3"), new Number160("0xffffffffffffffffffffffffffffffffffffffff"), new Number160("0xffffffffffffffffffffffffffffffffffffffff"), new Number160("0xffffffffffffffffffffffffffffffffffffffff"));
		
		final Number640 pMin = new Number640(new Number160("0x6469ac84a89748bb67b923c833ed0c778a17aea3"), new Number160("0x9120580e94f134cb7c9f27cd1e43dbc82980e152"), new Number160("0x40f06fd774092478d450774f5ba30c5da78acc8"), new Number160("0x2579476ab7f8486700000000"));
		final Number640 pMax = new Number640(new Number160("0x6469ac84a89748bb67b923c833ed0c778a17aea3"), new Number160("0x9120580e94f134cb7c9f27cd1e43dbc82980e152"), new Number160("0x40f06fd774092478d450774f5ba30c5da78acc8"), new Number160("0xdb630bdf5d05a8bde00000000"));
		
		final RangeLock<Number640> r = new RangeLock<Number640>();
		
		RangeLock<Number640>.Range lock1 = r.lock(gMin, gMax);
		Assert.assertEquals(2, r.size());
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				RangeLock<Number640>.Range lock2 = r.tryLock(pMin, pMax);
				Assert.assertNull(lock2);
				Assert.assertEquals(2, r.size());
				cd.countDown();
			}
		}).start();
		cd.await();
		r.unlock(lock1);
		Assert.assertEquals(0, r.size());
		
	}
}
