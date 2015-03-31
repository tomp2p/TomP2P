package net.tomp2p.holep;

import net.tomp2p.holep.strategy.HolePStrategy;

/**
 * This class is used as a {@link Thread} specifically for the
 * {@link HolePStrategy}. It calls the tryConnect() method on the
 * {@link HolePStrategy} every second until it reached the given numberOfPunches.
 * 
 * @author Jonas Wagner
 * 
 */
public class HolePScheduler implements Runnable {

	private static final int FIVE_MINUTES = 300;
	private static final int ONE_SECOND_MILLIS = 1000;
	private int numberOfPunches;
	private final HolePStrategy holePuncher;

	public HolePScheduler(final int numberOfTrials, final HolePStrategy holePuncher) {
		// 300 -> 5min
		if (numberOfTrials > FIVE_MINUTES) {
			throw new IllegalArgumentException("numberOfTrials can't be higher than 300 (5min)!");
		} else if (numberOfTrials < 1) {
			throw new IllegalArgumentException("numberOfTrials must be at least 1!");
		} else if (holePuncher == null) {
			throw new IllegalArgumentException("HolePuncher can't be null!");
		} else {
			this.numberOfPunches = numberOfTrials;
			this.holePuncher = holePuncher;
		}
	}

	@Override
	public void run() {
		while (numberOfPunches != 0) {
			try {
				holePuncher.tryConnect();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			numberOfPunches--;
			try {
				Thread.sleep(ONE_SECOND_MILLIS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
