package net.tomp2p.holep;

public class HolePunchScheduler implements Runnable {

	private int numberOfTrials;
	private final HolePuncher holePuncher;
	private static final int ONE_SECOND = 1000;
	
	public HolePunchScheduler(int numberOfTrials, HolePuncher holePuncher) {
		// 300 -> 5min
		if (numberOfTrials > 300) {
			throw new IllegalArgumentException("numberOfTrials can't be higher than 300!");
		} else if (numberOfTrials < 1) {
			throw new IllegalArgumentException("numberOfTrials must be at least 1!");
		} else {
			this.numberOfTrials = numberOfTrials;
		}
		this.holePuncher = holePuncher;
	}
	
	@Override
	public void run() {
		while (numberOfTrials != 0) {
			holePuncher.tryConnect();
			try {
				Thread.sleep(ONE_SECOND);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			numberOfTrials--;
		}
	}
	
	public void finish() {
		this.numberOfTrials = 0;
	}

}
