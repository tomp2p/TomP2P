package net.tomp2p.utils;

public class Timings
{
	private static Timing impl = new TimingImpl();
	public static long currentTimeMillis()
	{
		return impl.currentTimeMillis();
	}
	public static void setImpl(Timing impl2)
	{
		impl = impl2;
	}
	public static void sleep(int millis) throws InterruptedException
	{
		impl.sleep(millis);
	}
	public static void sleepUninterruptibly(int millis)
	{
		impl.sleepUninterruptibly(millis);
	}
}
