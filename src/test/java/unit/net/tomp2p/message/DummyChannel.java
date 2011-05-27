package net.tomp2p.message;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;

public class DummyChannel implements Channel
{
	private final SocketAddress remoteAddress;
	List<Object> objects=new ArrayList<Object>();

	public DummyChannel(SocketAddress remoteAddress)
	{
		this.remoteAddress = remoteAddress;
	}
	@Override
	public ChannelFuture bind(SocketAddress localAddress)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture close()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture disconnect()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture getCloseFuture()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelConfig getConfig()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFactory getFactory()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getId()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getInterestOps()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public SocketAddress getLocalAddress()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Channel getParent()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelPipeline getPipeline()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SocketAddress getRemoteAddress()
	{
		return remoteAddress;
	}

	@Override
	public boolean isBound()
	{
		return true;
	}

	@Override
	public boolean isConnected()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isOpen()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isReadable()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isWritable()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ChannelFuture setInterestOps(int interestOps)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture setReadable(boolean readable)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture unbind()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture write(Object message)
	{
		objects.add(message);
		return null;
	}

	@Override
	public ChannelFuture write(Object message, SocketAddress remoteAddress)
	{
		objects.add(message);
		return null;
	}

	@Override
	public int compareTo(Channel o)
	{
		// TODO Auto-generated method stub
		return 0;
	}
	public List<Object> getObjects()
	{
		return objects;
	}
}
