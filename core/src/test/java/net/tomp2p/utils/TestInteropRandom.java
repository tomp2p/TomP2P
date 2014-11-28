package net.tomp2p.utils;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class TestInteropRandom {

	@Test
	public void testSeed() {
		
		// create a random seed
		Random r = new Random();
		long seed = r.nextLong();
		
		int tests = 20;
		InteropRandom[] randoms = new InteropRandom[tests];
		int[] results = new int[tests];
		
		for (int i = 0; i < tests; i++)
		{
			randoms[i] = new InteropRandom(seed);
			results[i] = randoms[i].nextInt(1000);
			
			if (i > 0) {
				Assert.assertEquals(results[i], results[i-1]);
			}
		}
	}
}
