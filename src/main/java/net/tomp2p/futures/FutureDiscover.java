package net.tomp2p.futures;
import java.util.concurrent.TimeUnit;

import net.tomp2p.peers.PeerAddress;

import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

public class FutureDiscover extends BaseFutureImpl
{
	final private Timeout timeout;
	//
	private boolean goodUDP;
	private boolean goodTCP;
	private PeerAddress peerAddress;

	public FutureDiscover(Timer timer, int delaySec)
	{
		timeout = timer.newTimeout(new DiscoverTimeoutTask(), delaySec, TimeUnit.SECONDS);
	}

	@Override
	public void cancel()
	{
		timeout.cancel();
		super.cancel();
	}

	public void setGoodUDP(boolean goodUDP)
	{
		synchronized (lock)
		{
			this.goodUDP = goodUDP;
		}
	}

	public boolean getGoodUPD()
	{
		synchronized (lock)
		{
			return goodUDP;
		}
	}
	
	public void setGoodTCP(boolean goodTCP)
	{
		synchronized (lock)
		{
			this.goodTCP=goodTCP;
		}
	}

	public boolean getGoodTCP()
	{
		synchronized (lock)
		{
			return goodTCP;
		}
	}

	public void done(PeerAddress peerAddress)
	{
		timeout.cancel();
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
				return;
			this.peerAddress = peerAddress;
		}
		notifyListerenrs();
	}

	public PeerAddress getPeerAddress()
	{
		synchronized (lock)
		{
			return peerAddress;
		}
	}
	private final class DiscoverTimeoutTask implements TimerTask
	{
		@Override
		public void run(Timeout timeout) throws Exception
		{
			setFailed("Timeout in Discover");
		}
	}
}
