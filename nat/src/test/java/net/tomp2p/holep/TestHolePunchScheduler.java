package net.tomp2p.holep;

import org.junit.Assert;
import org.junit.Test;

public class TestHolePunchScheduler {

	@Test
	public void testHolePunchSchedulerFail() {
		HolePScheduler holePunchScheduler = null;
		try {
			holePunchScheduler = new HolePScheduler(-1, null);
		} catch (Exception e) {
			//do nothing
		}
		
		try {
			holePunchScheduler = new HolePScheduler(301, null);
		} catch (Exception e) {
			//do nothing
		}
		
		try {
			holePunchScheduler = new HolePScheduler(10, null);
		} catch (Exception e) {
			//do nothing
		}
		Assert.assertNull(holePunchScheduler);
	}
}
