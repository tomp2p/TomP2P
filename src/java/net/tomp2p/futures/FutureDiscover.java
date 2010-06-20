package net.tomp2p.futures;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

import net.tomp2p.peers.PeerAddress;

public class FutureDiscover extends BaseFutureImpl
{
	final private Timer timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS, 10);
	final private Timeout timeout;
	private boolean onGoingUDP;
	private boolean onGoingTCP;
	//
	private boolean privateUDP;
	private boolean privateTCP;
	private PeerAddress peerAddress;

	public FutureDiscover(int delaySec)
	{
		timeout = timer.newTimeout(new DiscoverTimeoutTask(), delaySec, TimeUnit.SECONDS);
	}

	@Override
	public void cancel()
	{
		timeout.cancel();
		timer.stop();
		super.cancel();
	}

	public void setPrivateUPD(boolean privateUDP)
	{
		synchronized (lock)
		{
			this.privateUDP = privateUDP;
		}
	}

	public boolean getPrivateUPD()
	{
		synchronized (lock)
		{
			return privateUDP;
		}
	}

	public boolean getPrivateTCP()
	{
		synchronized (lock)
		{
			return privateTCP;
		}
	}

	public void onGoing(boolean onGoingUDP, boolean onGoingTCP)
	{
		synchronized (lock)
		{
			this.onGoingUDP = onGoingUDP;
			this.onGoingTCP = onGoingTCP;
		}
	}

	public void done(PeerAddress peerAddress)
	{
		timer.stop();
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
				return;
			this.peerAddress = peerAddress;
		}
		notifyListerenrs();
	}

	public boolean isUDPOngoing()
	{
		synchronized (lock)
		{
			return onGoingUDP;
		}
	}

	public boolean isTCPOngoing()
	{
		synchronized (lock)
		{
			return onGoingTCP;
		}
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
			timer.stop();
			setFailed("Timeout");
		}
	}
}
