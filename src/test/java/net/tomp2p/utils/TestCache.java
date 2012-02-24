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
		Timings.sleepUninterruptibly(500);
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
		final ConcurrentCacheMap<String, String> test = new ConcurrentCacheMap<String, String>(1);
		test.put("hallo0", "test0");
		Timings.sleepUninterruptibly(500);
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
		Timings.sleepUninterruptibly(500);
		Assert.assertEquals(800-1, test.size());
		Assert.assertEquals(false, failed.get());
	}
}
