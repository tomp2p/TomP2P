package net.tomp2p.rpc;

import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Test;

public class TestBloomFilter
{
	@Test
	public void testBloomfilter()
	{
		SimpleBloomFilter<Number160> bloomFilter=new SimpleBloomFilter<Number160>(4096,1024);
		bloomFilter.add(Number160.MAX_VALUE);
		byte[] me=bloomFilter.toByteArray();
		SimpleBloomFilter<Number160> bloomFilter2=new SimpleBloomFilter<Number160>(me);
		Assert.assertEquals(true, bloomFilter2.contains(Number160.MAX_VALUE));
		Assert.assertEquals(false, bloomFilter2.contains(Number160.ONE));
		Assert.assertEquals(false, bloomFilter2.contains(Number160.ZERO));
	}
}
