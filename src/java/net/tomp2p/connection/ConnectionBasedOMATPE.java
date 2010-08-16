package net.tomp2p.connection;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import javax.management.RuntimeErrorException;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

public class ConnectionBasedOMATPE extends OrderedMemoryAwareThreadPoolExecutor
{
	public ConnectionBasedOMATPE(int corePoolSize, int maxChannelMemorySize,
			int maxTotalMemorySize)
	{
		super(corePoolSize, maxChannelMemorySize, maxTotalMemorySize);
	}

	@Override
	protected ConcurrentMap<Object, Executor> newChildExecutorMap()
	{
		// The default implementation returns a special ConcurrentMap that
		// uses identity comparison only (see {@link IdentityHashMap}).
		// Because SocketAddress does not work with identity comparison,
		// we need to employ more generic implementation.
		return new ConcurrentHashMap<Object, Executor>();
	}

	@Override
	protected Object getChildExecutorKey(ChannelEvent e)
	{
		//System.err.println(e.getChannel().get);
		return new Identifier(e.getChannel().getRemoteAddress(), e.getChannel().getLocalAddress());
	}

	// Make public so that you can call from anywhere.
	@Override
	public boolean removeChildExecutor(Object key)
	{
		return super.removeChildExecutor(key);
	}
	private static class Identifier
	{
		final private SocketAddress remote;
		final private SocketAddress local;

		public Identifier(SocketAddress remote, SocketAddress local)
		{
			if(remote==null)
			{
				throw new RuntimeException("remote null");
			}
			if(local==null)
				throw new RuntimeException("local null");
		
			
			this.remote=remote;
			this.local=local;
		}
		
		@Override
		public int hashCode()
		{
			return remote.hashCode() ^ local.hashCode();
		}

		@Override
		public boolean equals(Object obj)
		{
			if (!(obj instanceof Identifier))
				return false;
			Identifier i = (Identifier) obj;
			return (i.remote.equals(remote) && i.local.equals(local))
					|| (i.remote.equals(local) && i.remote.equals(local));
		}
	}
}