package net.tomp2p.utils;

import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.junit.Test;

public class TestCache
{
	@Test
	public void testCache()
	{
		ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1);
		test.put("hallo1", "test1");
		Timings.sleepUninterruptibly(500);
		test.put("hallo2", "test2");
		test.put("hallo3", "test3");
		Timings.sleepUninterruptibly(600);
		Assert.assertEquals(2, test.size());
	}
	
	@Test
	public void testCache2()
	{
		ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1);
		test.put("hallo0", "test0");
		Timings.sleepUninterruptibly(500);
		for(int i=1;i<800;i++)
		{
			test.put("hallo"+i, "test"+i);
		}
		Timings.sleepUninterruptibly(500);
		Assert.assertEquals(800-1, test.size());
	}
	
	@Test
	public void testCache3()
	{
		final ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(6);
		test.put("hallo0", "test0");
		Timings.sleepUninterruptibly(3000);
		final AtomicBoolean failed = new AtomicBoolean(false);
		for(int i=1;i<800;i++)
		{
			final int ii=i;
			new Thread(new Runnable()
			{
				@Override
				public void run()
				{
					test.put("hallo"+ii, "test"+ii);
					new Thread(new Runnable()
					{
						@Override
						public void run()
						{
							String val = test.get("hallo"+ii);
							if(!("test"+ii).equals(val))
							{
								failed.set(true);
							}
						}
					}).start();
				}
			}).start();
		}
		Timings.sleepUninterruptibly(3000);
		Assert.assertEquals(800-1, test.size());
		Assert.assertEquals(false, failed.get());
	}
	
	@Test
	public void testCache4()
	{
		String key = "hallo0";
		ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1);
		test.put(key, "test0");
		Timings.sleepUninterruptibly(1100);
		test.put(key, "test1");
		Timings.sleepUninterruptibly(200);
		String val = test.get(key);
		Assert.assertEquals("test1", val);
	}
	
	@Test
	public void testCache5()
	{
		String key = "hallo0";
		ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1);
		test.put(key, "test0");
		Timings.sleepUninterruptibly(1100);
		test.put(key, "test1");
		Timings.sleepUninterruptibly(1100);
		String val = test.get(key);
		Assert.assertEquals(null, val);
	}
	
	@Test
	public void testCache6()
	{
		String key = "hallo0";
		ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1, 1024, false);
		test.put(key, "test0");
		Timings.sleepUninterruptibly(500);
		test.putIfAbsent(key, "test1");
		Timings.sleepUninterruptibly(800);
		String val = test.get(key);
		Assert.assertEquals(null, val);
	}
	
	@Test
	public void testCache7()
	{
		String key = "hallo0";
		ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1, 1024, true);
		test.put(key, "test0");
		Timings.sleepUninterruptibly(500);
		test.putIfAbsent(key, "test1");
		Timings.sleepUninterruptibly(800);
		String val = test.get(key);
		//putIfAbsent will refresh test0
		Assert.assertEquals("test0", val);
	}
}