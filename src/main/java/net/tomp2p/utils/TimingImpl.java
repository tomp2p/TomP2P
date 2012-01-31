package net.tomp2p.utils;

public class TimingImpl implements Timing
{
	/* (non-Javadoc)
	 * @see net.tomp2p.utils.Timing#currentTimeMillis()
	 */
	@Override
	public long currentTimeMillis()
	{
		return System.currentTimeMillis();
	}

	/* (non-Javadoc)
	 * @see net.tomp2p.utils.Timing#sleep(int)
	 */
	@Override
	public void sleep(int millis) throws InterruptedException
	{
		Thread.sleep(millis);
	}

	@Override
	public void sleepUninterruptibly(int millis)
	{
		//TODO: make a proper sleepUninterruptibly, for now its good enough
		try
		{
			Thread.sleep(millis);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}	
	}
}
