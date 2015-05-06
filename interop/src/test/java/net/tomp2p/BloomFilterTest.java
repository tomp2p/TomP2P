package net.tomp2p;

import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.SimpleBloomFilter;

import org.junit.Test;

public class BloomFilterTest {

	static byte[] sampleBytes1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 };
	static byte[] sampleBytes2 = new byte[] { 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };

	static Number160 sample160_1 = Number160.ZERO;
	static Number160 sample160_2 = Number160.ONE;
	static Number160 sample160_3 = Number160.MAX_VALUE;
	static Number160 sample160_4 = new Number160(sampleBytes1);
	static Number160 sample160_5 = new Number160(sampleBytes2);

	@Test
	public void testBloomfilter() {
		
		SimpleBloomFilter<Number160> sampleBf1 = new SimpleBloomFilter<Number160>(1, 1);
		sampleBf1.add(sample160_1);
		
		SimpleBloomFilter<Number160> sampleBf2 = new SimpleBloomFilter<Number160>(1, 2);
		sampleBf2.add(sample160_1);
		sampleBf2.add(sample160_2);
	}
	
	@Test
	public void testBloomfilter2() {
		
		// create sample bloom filters
		SimpleBloomFilter<Number160> sampleBf1 = new SimpleBloomFilter<Number160>(1, 1);
		sampleBf1.add(sample160_1);

		SimpleBloomFilter<Number160> sampleBf2 = new SimpleBloomFilter<Number160>(1, 2);
		sampleBf2.add(sample160_1);
		sampleBf2.add(sample160_2);

		SimpleBloomFilter<Number160> sampleBf3 = new SimpleBloomFilter<Number160>(1, 3);
		sampleBf3.add(sample160_1);
		sampleBf3.add(sample160_2);
		sampleBf3.add(sample160_3);

		SimpleBloomFilter<Number160> sampleBf4 = new SimpleBloomFilter<Number160>(1, 4);
		sampleBf4.add(sample160_1);
		sampleBf4.add(sample160_2);
		sampleBf4.add(sample160_3);
		sampleBf4.add(sample160_4);

		SimpleBloomFilter<Number160> sampleBf5 = new SimpleBloomFilter<Number160>(1, 5);
		sampleBf5.add(sample160_1);
		sampleBf5.add(sample160_2);
		sampleBf5.add(sample160_3);
		sampleBf5.add(sample160_4);
		sampleBf5.add(sample160_5);
	}
}
