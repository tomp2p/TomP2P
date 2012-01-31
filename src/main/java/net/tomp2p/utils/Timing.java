package net.tomp2p.utils;
public interface Timing
{
	public abstract long currentTimeMillis();

	public abstract void sleep(int millis) throws InterruptedException;

	public abstract void sleepUninterruptibly(int millis);
}