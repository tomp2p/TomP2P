package net.tomp2p.futures;

import org.jboss.netty.channel.Channel;

public class FutureChannelCreation  extends BaseFutureImpl
{
	private Channel channel;
	private boolean semaphoreAcquired;
	public void setChannel(Channel channel)
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
			{
				return;
			}
			this.channel = channel;
			this.type = FutureType.OK;
		}
		notifyListerenrs();
	}
	
	public Channel getChannel()
	{
		synchronized (lock)
		{
			return channel;
		}
	}

	public void setAcquired(boolean semaphoreAcquired)
	{
		synchronized (lock)
		{
			this.semaphoreAcquired = semaphoreAcquired;
		}
	}
	
	public boolean isAcquired()
	{
		synchronized (lock)
		{
			return semaphoreAcquired;
		}
	}
}
