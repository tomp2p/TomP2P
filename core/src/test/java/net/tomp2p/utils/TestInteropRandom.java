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
	
	@Test
	public void testInteropSeed()
    {
        // use the same seed as in .NET
        final int seed = 1234567890;
        InteropRandom random = new InteropRandom(seed);
        int result1 = random.nextInt(1000);
        int result2 = random.nextInt(500);
        int result3 = random.nextInt(10);
      
        // requires same results as in .NET
        // result1 is 677
        // result2 is 242
        // result3 is 1
        Assert.assertEquals(result1, 677);
        Assert.assertEquals(result2, 242);
        Assert.assertEquals(result3, 1);
    }
}
