package net.tomp2p.utils;

import java.util.Date;

public interface Ticker {

	/**
	 * This method is called every second by a {@link ScheduledExecutorService}.
	 * It should only be called by the {@link Scheduler}.
	 * 
	 * @param date
	 */
	public void receiveTicksignal(Date date);
}
