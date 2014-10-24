package net.tomp2p.examples.relay;

import java.util.ArrayList;
import java.util.List;

/**
 * Gathers some statistics about times and success rate
 * @author Nico Rutishauser
 *
 */
public class DHTQueryStatistics {

	// timing
	private final List<Long> times;
	private long startTime;
	
	// success rate
	private final List<Boolean> successes;
	
	public DHTQueryStatistics() {
		this.times = new ArrayList<Long>();
		this.successes = new ArrayList<Boolean>();
	}
	
	public void start() {
		startTime = System.currentTimeMillis();
	}
	
	public void finished(boolean success) {
		times.add(System.currentTimeMillis() - startTime);
		successes.add(success);
	}
	
	public double getSuccessRate() {
		double success = 0;
		for (Boolean result : successes) {
			if(result) {
				success++;
			}
		}
		
		return success / (double) successes.size();
	}
	
	public long getAverageTime() {
		long total = 0;
		for (Long time : times) {
			total += time;
		}
		
		return total / times.size();
	}
}
