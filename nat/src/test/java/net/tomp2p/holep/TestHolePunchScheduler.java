package net.tomp2p.holep;

import org.junit.Assert;
import org.junit.Test;

public class TestHolePunchScheduler {

	@Test
	public void testHolePunchSchedulerFail() {
		HolePSchedulerLegacy holePunchScheduler = null;
		try {
			holePunchScheduler = new HolePSchedulerLegacy(-1, null);
		} catch (Exception e) {
			//do nothing
		}
		
		try {
			holePunchScheduler = new HolePSchedulerLegacy(301, null);
		} catch (Exception e) {
			//do nothing
		}
		
		try {
			holePunchScheduler = new HolePSchedulerLegacy(10, null);
		} catch (Exception e) {
			//do nothing
		}
		Assert.assertNull(holePunchScheduler);
	}
}
