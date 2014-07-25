package net.tomp2p.rcon.prototype;

import java.util.Date;

import net.tomp2p.utils.Scheduler;
import net.tomp2p.utils.Ticker;

public class SchedulerDriver implements Ticker{

	public static void main(String[] args) throws InterruptedException {
		SchedulerDriver driver = new SchedulerDriver();
		driver.registerThis();
		System.exit(0);
	}

	@Override
	public void receiveTicksignal(Date date) {
		System.out.println("Fired at time: " + date.getSeconds());
	}
	
	public void registerThis() {
		Scheduler.getInstance().registerService(this);
	}
	
	public void unregisterThis() {
		Scheduler.getInstance().unregisterService(this);
	}
}
