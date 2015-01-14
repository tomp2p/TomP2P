package net.tomp2p.rpc;

import java.util.BitSet;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class TestRPCUtils {
	@Test
	public void convert() {
		
		Random rnd = new Random(42);
		for(int i=0;i<100;i++) {
			byte[] b = new byte[rnd.nextInt(10000)];
			rnd.nextBytes(b);
			BitSet bs = RPCUtils.fromByteArray(b);
			byte[] r= RPCUtils.toByteArray(bs);
			Assert.assertArrayEquals(b, r);
		}
	}
}
