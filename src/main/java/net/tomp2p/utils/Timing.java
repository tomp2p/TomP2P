package net.tomp2p.utils;

public class Timing
{
	private static TimingImpl impl = new TimingImpl();
	public static long currentTimeMillis()
	{
		return impl.currentTimeMillis();
	}
	public static void setImpl(TimingImpl impl2)
	{
		impl = impl2;
	}
}
