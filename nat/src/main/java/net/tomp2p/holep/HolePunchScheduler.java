package net.tomp2p.holep;

import java.util.HashMap;
import java.util.Map;

public class HolePunchScheduler implements Runnable {

	private static boolean alive = false;
	private static final int ONE_SECOND_MILLIS = 1000;
	private final Map<HolePuncher, Integer> holePunchers = new HashMap<HolePuncher, Integer>();
	private static final HolePunchScheduler instance = new HolePunchScheduler();

	public static HolePunchScheduler instance() {
		return instance;
	}

	public void addHolePuncher(int numberOfTrials, HolePuncher holePuncher) throws Exception {
		// 300 -> 5min
		if (numberOfTrials > 300) {
			throw new IllegalArgumentException("numberOfTrials can't be higher than 300!");
		} else if (numberOfTrials < 1) {
			throw new IllegalArgumentException("numberOfTrials must be at least 1!");
		} else if (holePuncher == null) {
			throw new IllegalArgumentException("HolePuncher can't be null!");
		} else {
			holePunchers.put(holePuncher, new Integer(numberOfTrials));
			if (!alive) {
				alive = true;
				this.run();
			}
		}
	}

	@Override
	public void run() {
		while (alive) {
			if (!holePunchers.isEmpty()) {
				for (Map.Entry<HolePuncher, Integer> map : holePunchers.entrySet()) {
					if (map.getValue().intValue() == 0) {
						holePunchers.remove(map.getKey());
					} else {
						try {
							map.getKey().tryConnect();
						} catch (Exception e) {
							e.printStackTrace();
						}
						holePunchers.put(map.getKey(), map.getValue() - 1);
					}
				}
			} else {
				alive = false;
			}
			try {
				Thread.sleep(ONE_SECOND_MILLIS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void shutdownScheduler() {
		alive = false;
	}
}
