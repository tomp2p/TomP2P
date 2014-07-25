package net.tomp2p.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for the ticksignal which is sent to all registered Services every second.
 * 
 * @author jonaswagner
 *
 */
public class Scheduler implements Runnable, Ticker {

	private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
	private static Scheduler schedulerSingleton = new Scheduler();
	private List<Ticker> registeredServices = new ArrayList<Ticker>(1);

	private Scheduler() {
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(this, 0, 1, TimeUnit.SECONDS);
	}

	/**
	 * Returns the only instance of this class.
	 * 
	 * @return schedulerSingleton
	 */
	public static Scheduler getInstance() {
		return schedulerSingleton;
	}

	@Override
	public void receiveTicksignal(Date date) {
		for (Ticker ticker : registeredServices) {
			ticker.receiveTicksignal(date);
		}
	}

	@Override
	public void run() {
		if (!registeredServices.isEmpty()) {
			receiveTicksignal(new Date());
		}
	}

	/**
	 * This method registers an object to the registeredServices List. In this
	 * list, every object will be called every second via the
	 * receiveTicksignal() method until the object is unregistered.
	 * 
	 * @param ticker
	 */
	public void registerService(Ticker ticker) {
		registeredServices.add(ticker);
		LOG.debug("new service registered in Scheduler. The total number of registered services is now at "
				+ registeredServices.size());
	}

	/**
	 * Unregisters an object from the registeredServices List.
	 * @param ticker
	 */
	public void unregisterService(Ticker ticker) {
		registeredServices.remove(ticker);
		LOG.debug("service" + ticker
				+ " was removed from the Scheduler. The total number of registered services is now at "
				+ registeredServices.size());
	}
}
