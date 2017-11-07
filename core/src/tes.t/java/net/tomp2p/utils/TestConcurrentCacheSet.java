package net.tomp2p.utils;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestConcurrentCacheSet {
	
	@Test
	public void testCache1() throws InterruptedException {
		ConcurrentCacheSet<String> ccs = new ConcurrentCacheSet<String>(1);
		ccs.add("test");
		Assert.assertTrue(ccs.contains("test"));
		Thread.sleep(1001);
		Assert.assertFalse(ccs.contains("test"));
	}
	
	@Test
	public void testCache2() throws InterruptedException {
		ConcurrentCacheSet<String> ccs = new ConcurrentCacheSet<String>(1);
		ccs.add("test1");
		ccs.add("test2");
		
		List<String> tmp = new ArrayList<String>();
		tmp.add("test1");
		
		ccs.removeAll(tmp);
		Assert.assertTrue(ccs.contains("test2"));
		Thread.sleep(1001);
		Assert.assertEquals(0, ccs.size());
	}
	
	@Test
	public void testCache3() throws InterruptedException {
		ConcurrentCacheSet<String> ccs = new ConcurrentCacheSet<String>(1);
		ccs.add("test1");
		ccs.add("test2");
		ccs.clear();
		Assert.assertEquals(0, ccs.size());
	}
}
