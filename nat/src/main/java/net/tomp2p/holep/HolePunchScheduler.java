package net.tomp2p.holep;

import java.util.HashMap;
import java.util.Map;

public class HolePunchScheduler implements Runnable {

	private static final int ONE_SECOND_MILLIS = 1000;
	private final Map<HolePuncher, Integer> holePunchers = new HashMap<HolePuncher, Integer>();
	private int numberOfTrials;
	private HolePuncher holePuncher;

	public HolePunchScheduler(int numberOfTrials, HolePuncher holePuncher) {
		// 300 -> 5min
		if (numberOfTrials > 300) {
			throw new IllegalArgumentException("numberOfTrials can't be higher than 300!");
		} else if (numberOfTrials < 1) {
			throw new IllegalArgumentException("numberOfTrials must be at least 1!");
		} else if (holePuncher == null) {
			throw new IllegalArgumentException("HolePuncher can't be null!");
		} else {
			this.numberOfTrials = numberOfTrials;
			this.holePuncher = holePuncher;
		}
	}

	@Override
	public void run() {
		while (numberOfTrials != 0) {
			try {
				holePuncher.tryConnect();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			numberOfTrials--;
			try {
				Thread.sleep(ONE_SECOND_MILLIS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
