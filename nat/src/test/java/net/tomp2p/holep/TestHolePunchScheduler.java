package net.tomp2p.holep;

import org.junit.Assert;
import org.junit.Test;

public class TestHolePunchScheduler {

	@Test
	public void testHolePunchSchedulerFail() {
		HolePunchScheduler holePunchScheduler = null;
		try {
			holePunchScheduler = new HolePunchScheduler(-1, null);
		} catch (Exception e) {
			//do nothing
		}
		
		try {
			holePunchScheduler = new HolePunchScheduler(301, null);
		} catch (Exception e) {
			//do nothing
		}
		
		try {
			holePunchScheduler = new HolePunchScheduler(10, null);
		} catch (Exception e) {
			//do nothing
		}
		Assert.assertNull(holePunchScheduler);
	}
}
