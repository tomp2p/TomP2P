package net.tomp2p.message;

import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

public class DummyChannelFuture implements ChannelFuture
{

	@Override
	public void addListener(ChannelFutureListener listener)
	{
		// TODO Auto-generated method stub
	}

	@Override
	public ChannelFuture await() throws InterruptedException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean await(long timeoutMillis) throws InterruptedException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ChannelFuture awaitUninterruptibly()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean awaitUninterruptibly(long timeoutMillis)
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean awaitUninterruptibly(long timeout, TimeUnit unit)
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean cancel()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Throwable getCause()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Channel getChannel()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isCancelled()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDone()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isSuccess()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void removeListener(ChannelFutureListener listener)
	{
		// TODO Auto-generated method stub
	}

	@Override
	public boolean setFailure(Throwable cause)
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean setSuccess()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean setProgress(long arg0, long arg1, long arg2) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ChannelFuture rethrowIfFailed() throws Exception
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture sync() throws InterruptedException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture syncUninterruptibly()
	{
		// TODO Auto-generated method stub
		return null;
	}
}
