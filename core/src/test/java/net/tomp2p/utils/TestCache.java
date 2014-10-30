package net.tomp2p.utils;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

public class TestCache {
    @Test
    public void testCache() throws InterruptedException {
        ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1, 1024);
        test.put("hallo1", "test1");
        Thread.sleep(500);
        test.put("hallo2", "test2");
        test.put("hallo3", "test3");
        Thread.sleep(600);
        Assert.assertEquals(2, test.size());
    }

    @Test
    public void testCache2() throws InterruptedException {
        ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1, 1024);
        test.put("hallo0", "test0");
        Thread.sleep(500);
        for (int i = 1; i < 800; i++) {
            test.put("hallo" + i, "test" + i);
        }
        Thread.sleep(500);
        Assert.assertEquals(800 - 1, test.size());
    }

    @Test
    public void testCache3multi() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            testCache3();
        }
    }

    @Test
    public void testCache3() throws InterruptedException {
        final ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(3, 1024);
        test.put("hallo0", "test0");
        Thread.sleep(1000);
        final long start = System.currentTimeMillis();
        final AtomicBoolean failed = new AtomicBoolean(false);
        final AtomicInteger integer1 = new AtomicInteger(1);
        final AtomicInteger integer2 = new AtomicInteger(1);
        final AtomicLong long1 = new AtomicLong();
        for (int i = 1; i < 800; i++) {
            final int ii = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName("test-cache1 " + ii);
                    integer1.incrementAndGet();
                    test.put("hallo" + ii, "test" + ii);
                    
					long seen = System.currentTimeMillis();
					if (seen > long1.get())
						long1.set(seen);
					
                    integer2.incrementAndGet();
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            Thread.currentThread().setName("test-cache2 " + ii);
                            String val = test.get("hallo" + ii);
                            if (!("test" + ii).equals(val)) {
                                failed.set(true);
                            }
                        }
                    }).start();
                }
            }).start();
        }
        long waitfor = 2900 - (System.currentTimeMillis() - start);
        System.out.println("waitfor: " + waitfor);
        Thread.sleep((int) waitfor);
        System.out.println("TestCache: expected: " + (800 - 1) + ", got: " + test.size() + ", failed: " + failed.get()
                + " - expired " + test.expiredCounter() + ", inserts: " + integer1 + "/" + integer2 + ", threads: "
                + Thread.activeCount());
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            System.out.println(t.getName());
        }
        Assert.assertEquals(800 - 1, test.size());
        Assert.assertEquals(false, failed.get());
    }

    @Test
    public void testCache4() throws InterruptedException {
        String key = "hallo0";
        ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1, 1024);
        test.put(key, "test0");
        Thread.sleep(1100);
        test.put(key, "test1");
        Thread.sleep(200);
        String val = test.get(key);
        Assert.assertEquals("test1", val);
    }

    @Test
    public void testCache5() throws InterruptedException {
        String key = "hallo0";
        ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1, 1024);
        test.put(key, "test0");
        Thread.sleep(1100);
        test.put(key, "test1");
        Thread.sleep(1100);
        String val = test.get(key);
        Assert.assertEquals(null, val);
    }

    @Test
    public void testCache6() throws InterruptedException {
        String key = "hallo0";
        ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1, 1024, false);
        test.put(key, "test0");
        Thread.sleep(500);
        test.putIfAbsent(key, "test1");
        Thread.sleep(800);
        String val = test.get(key);
        Assert.assertEquals(null, val);
    }

    @Test
    public void testCache7() throws InterruptedException {
        String key = "hallo0";
        ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1, 1024, true);
        test.put(key, "test0");
        Thread.sleep(500);
        test.putIfAbsent(key, "test1");
        Thread.sleep(800);
        String val = test.get(key);
        // putIfAbsent will refresh test0
        Assert.assertEquals("test0", val);
    }
    
    @Test
    public void testIteratorKey() {
    	String key = "hallo0";
    	ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1, 1024, true);
        test.put(key, "test0");
        Iterator<Entry<String, String>> iterator = test.entrySet().iterator();
        while(iterator.hasNext()) {
        	iterator.next();
        	iterator.remove();
        }
        Assert.assertEquals(0, test.size());
        
    }
    
	@Test
	public void testIteratorValue() {
		String key = "hallo0";
		ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1, 1024, true);
		test.put(key, "test0");
		Iterator<String> iterator = test.values().iterator();
		while (iterator.hasNext()) {
			iterator.next();
			try {
				iterator.remove();
				Assert.fail();
			} catch (UnsupportedOperationException u) {

			}
		}
		Assert.assertEquals(1, test.size());

	}
}