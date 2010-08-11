package net.tomp2p.rpc;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.Utils2;
import net.tomp2p.connection.TCPChannelCache;
import net.tomp2p.connection.ConnectionCollector;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.message.Message;
import net.tomp2p.message.TomP2PDecoderTCP;
import net.tomp2p.message.TomP2PDecoderUDP;
import net.tomp2p.message.TomP2PEncoderStage1;
import net.tomp2p.utils.Utils;

import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.junit.Assert;
import org.junit.Test;


public class TestConnectionPool
{
	final private static AtomicBoolean fail = new AtomicBoolean(false);
	final private ChannelFactory udpChannelFactory = new NioDatagramChannelFactory(Executors
			.newCachedThreadPool());
	final private ChannelFactory tcpClientChannelFactory = new NioClientSocketChannelFactory(
			Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
	ExecutionHandler executionHandlerSender= new ExecutionHandler(Executors.newCachedThreadPool());

	/**
	 * Sends a lot of dummy messages over channels acquired by ConnectionPool to
	 * test channel acquisition and release. Prints a dot ('.') for every
	 * Message received and decoded.
	 */
	@Test
	public void testConnectionPoolTCP()
	{
		try
		{
			TCPChannelCache cache=new TCPChannelCache();
			setupServerNettyTCP();
			ConnectionCollector cp = new ConnectionCollector(tcpClientChannelFactory,
					udpChannelFactory,
					new ConnectionConfiguration(), executionHandlerSender);
			for (int j = 0; j < 10; j++)
			{
				for (int i = 0; i < 10; i++)
				{
					final ChannelFuture cf = cp.channelTCP(new SimpleChannelHandler(),
							new SimpleChannelHandler(), new InetSocketAddress("127.0.0.1", 8080),
							500, cache);
					cf.awaitUninterruptibly();
					Message m = Utils2.createDummyMessage();
					m.setMessageId(m.getMessageId() + i);
					cf.getChannel().write(m).addListener(new ChannelFutureListener()
					{
						@Override
						public void operationComplete(ChannelFuture arg0) throws Exception
						{
							new Thread(new Runnable()
							{
								@Override
								public void run()
								{
									Utils.sleep(1000);
									cf.getChannel().disconnect();
								}
							}).start();
						}
					});
				}
			}
			cp.shutdown();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testConnectionPoolUDP()
	{
		try
		{
			setupServerNettyUDP();
			ConnectionCollector cp = new ConnectionCollector(tcpClientChannelFactory,
					udpChannelFactory, new ConnectionConfiguration(), executionHandlerSender);
			for (int j = 0; j < 10; j++)
			{
				for (int i = 0; i < 10; ++i)
				{
					final Channel ch = cp.channelUDP(new SimpleChannelHandler(),
							new SimpleChannelHandler(), false);
					ch.connect(new InetSocketAddress("127.0.0.1", 8080)).awaitUninterruptibly();
					Message m = Utils2.createDummyMessage();
					m.setMessageId(m.getMessageId() + i);
					ChannelFuture cf = ch.write(m);
					cf.addListener(new ChannelFutureListener()
					{
						@Override
						public void operationComplete(ChannelFuture arg0) throws Exception
						{
							new Thread(new Runnable()
							{
								@Override
								public void run()
								{
									Utils.sleep(1000);
									ch.close();
								}
							}).start();
						}
					});
					if (fail.get())
						Assert.fail();
				}
			}
			cp.shutdown();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.fail();
		}
	}

	public boolean setupServerNettyUDP() throws Exception
	{
		ChannelFactory factory = new OioDatagramChannelFactory(Executors.newCachedThreadPool());
		ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(factory);
		setupBootstrap(bootstrap, true);
		Channel channel = bootstrap.bind(new InetSocketAddress(8080));
		return channel.isBound();
	}

	/**
	 * Creates TCP channels and listens on them
	 * 
	 * @param listenAddressesTCP the addresses which we will listen on
	 * @throws Exception
	 */
	public boolean setupServerNettyTCP() throws Exception
	{
		ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		ServerBootstrap bootstrap = new ServerBootstrap(factory);
		setupBootstrap(bootstrap, false);
		Channel channel = bootstrap.bind(new InetSocketAddress(8080));
		return channel.isBound();
	}

	private void setupBootstrap(Bootstrap bootstrap, boolean udp)
	{
		ChannelPipeline p = bootstrap.getPipeline();
		p.addLast("encoder", new TomP2PEncoderStage1());
		p.addLast("decoder", udp ? new TomP2PDecoderUDP() : new TomP2PDecoderTCP());
		p.addLast("handler", new ServerHandlerNetty());
		bootstrap.setOption("broadcast", "false");
	}
	@Sharable
	private class ServerHandlerNetty extends SimpleChannelHandler
	{
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
		{
			System.out.print("." + e.getRemoteAddress());
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
		{
			e.getCause().printStackTrace();
		}
	}
}
