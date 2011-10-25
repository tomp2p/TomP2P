package net.tomp2p.connection;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.TomP2PDecoderTCP;
import net.tomp2p.message.TomP2PDecoderUDP;
import net.tomp2p.message.TomP2PEncoderTCP;
import net.tomp2p.message.TomP2PEncoderUDP;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.rpc.RequestHandlerUDP;
import net.tomp2p.utils.CacheMap;

import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictor;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;

public class ChannelCreator 
{
	final private Semaphore semaphore;
	final private int permits;
	final private ChannelGroup channelsTCP;
	final private ChannelGroup channelsUDP;
	final private MessageLogger messageLoggerFilter;
	final private ChannelFactory tcpClientChannelFactory;
	final private ChannelFactory udpChannelFactory;
	final private AtomicBoolean shutdown;
	final private ConnectionReservation connectionReservation;
	final private boolean keepAliveAndReuse;
	final private Map<InetSocketAddress, ChannelFuture> cacheMap = Collections.synchronizedMap(new CacheMap<InetSocketAddress, ChannelFuture>(100));
	private static AtomicLong statConnectionsCreatedTCP=new AtomicLong();
	private static AtomicLong statConnectionsCreatedUDP=new AtomicLong();
	
	public ChannelCreator(ChannelGroup channelsTCP, ChannelGroup channelsUDP, int permits, 
			MessageLogger messageLoggerFilter, ChannelFactory tcpClientChannelFactory, 
			ChannelFactory udpClientChannelFactory, AtomicBoolean shutdown, ConnectionReservation connectionReservation, boolean keepAliveAndReuse)
	{
		this.permits = permits;
		this.channelsTCP = channelsTCP;
		this.channelsUDP = channelsUDP;
		this.semaphore=new Semaphore(permits);
		this.messageLoggerFilter = messageLoggerFilter;
		this.tcpClientChannelFactory = tcpClientChannelFactory;
		this.udpChannelFactory = udpClientChannelFactory;
		this.shutdown = shutdown;
		this.connectionReservation = connectionReservation;
		this.keepAliveAndReuse = keepAliveAndReuse;
	}
	
	public Channel createUDPChannel(ReplyTimeoutHandler timeoutHandler,
			RequestHandlerUDP requestHandler, final FutureResponse futureResponse, boolean broadcast) {
		if(shutdown.get())
		{
			throw new RuntimeException("Cannot create channel if already shutdown");
		}
		// If we are out of semaphores, we cannot create any channels. Since we know how many channels max. in parallel are created, we can reserve it. 
		if(!semaphore.tryAcquire())
		{
			throw new RuntimeException("you ran out of permits. You had "+permits+" available, but now its down to 0");
		}
		// now, we don't exceeded the limits, so create channels
		Channel channel = createChannelUDP(timeoutHandler, requestHandler, broadcast);
		futureResponse.setExitFast(false);
		channelsUDP.add(channel);
		channel.getCloseFuture().addListener(new ChannelFutureListener()
		{
			@Override
			public void operationComplete(ChannelFuture future) throws Exception
			{
				channelsUDP.remove(future.getChannel());
				semaphore.release();
				//we set fast exit to false, exit if connection has been closed.
				futureResponse.fireResponse();
			}
		});
		return channel;
	}
	
	public ChannelFuture createTCPChannel(ReplyTimeoutHandler timeoutHandler,
			final FutureResponse futureResponse, int connectTimeoutMillis,
			int idleTCPMillis, Message message, RequestHandlerTCP requestHandler) {
		if(shutdown.get())
		{
			throw new RuntimeException("Cannot create channel if already shutdown");
		}
		// If we are out of semaphores, we cannot create any channels. Since we know how many channels max. in parallel are created, we can reserve it. 
		if(!semaphore.tryAcquire())
		{
			throw new RuntimeException("you ran out of permits. You had "+permits+" available, but now its down to 0");
		}
		// now, we don't exceeded the limits, so create channels
		ChannelFuture channelFuture;
		final InetSocketAddress recipient = message.getRecipient().createSocketTCP();
		if(keepAliveAndReuse)
		{
			channelFuture = cacheMap.get(recipient);
			if(channelFuture == null)
			{
				channelFuture = createChannelTCP(timeoutHandler, requestHandler,
						recipient, new InetSocketAddress(0), connectTimeoutMillis);
				cacheMap.put(recipient, channelFuture);
			}
		}
		else
		{
			channelFuture = createChannelTCP(timeoutHandler, requestHandler,
					recipient, new InetSocketAddress(0), connectTimeoutMillis);
			
		}
		Channel channel = channelFuture.getChannel();
		futureResponse.setExitFast(false);
		channelsTCP.add(channel);
		channel.getCloseFuture().addListener(new ChannelFutureListener()
		{
			@Override
			public void operationComplete(ChannelFuture future) throws Exception
			{
				channelsTCP.remove(future.getChannel());
				semaphore.release();
				if(keepAliveAndReuse)
				{
					cacheMap.remove(recipient);
				}
				//we set fast exit to false, exit if connection has been closed.
				futureResponse.fireResponse();
			}
		});
		return channelFuture;
	}
	
	public void release()
	{
		connectionReservation.release(permits);
	}
	
	public void release(int nr)
	{
		connectionReservation.release(nr);
	}
	
	private ChannelFuture createChannelTCP(ChannelHandler timeoutHandler,
			ChannelHandler dispatcherReply, SocketAddress remoteAddress,
			SocketAddress localAddress, int connectionTimoutMillis)
	{
		statConnectionsCreatedTCP.incrementAndGet();
		ClientBootstrap bootstrap = new ClientBootstrap(tcpClientChannelFactory);
		bootstrap.setOption("connectTimeoutMillis", connectionTimoutMillis);
		//bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("soLinger", 0);
		bootstrap.setOption("reuseAddress", true);
		bootstrap.setOption("keepAlive", true);
		setupBootstrapTCP(bootstrap, timeoutHandler, dispatcherReply, new TomP2PDecoderTCP(), new TomP2PEncoderTCP(), new ChunkedWriteHandler(), messageLoggerFilter);
		return bootstrap.connect(remoteAddress);
	}
	
	private Channel createChannelUDP(ChannelHandler timeoutHandler, ChannelHandler replyHandler,
			boolean allowBroadcast)
	{
		statConnectionsCreatedUDP.incrementAndGet();
		ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(udpChannelFactory);
		setupBootstrapUDP(bootstrap, timeoutHandler, replyHandler, new TomP2PDecoderUDP(), new TomP2PEncoderUDP(), null);
		// enable per default, as we support a broadcast ping to find other peers.
		bootstrap.setOption("broadcast", allowBroadcast ? true : false);
		bootstrap.setOption("receiveBufferSizePredictor", new FixedReceiveBufferSizePredictor(
				ConnectionHandler.UDP_LIMIT));
		Channel c = bootstrap.bind(new InetSocketAddress(0));
		return c;
	}
	
	static void setupBootstrapTCP(Bootstrap bootstrap, ChannelHandler timeoutHandler,
			ChannelHandler dispatcherReply, ChannelUpstreamHandler decoder, ChannelDownstreamHandler encoder, ChunkedWriteHandler streamer, ChannelHandler messageLoggerFilter)
	{
		ChannelPipeline pipe = bootstrap.getPipeline();
		if (timeoutHandler != null) {
			pipe.addLast("timeout", timeoutHandler);
		}
		pipe.addLast("streamer", streamer);
		pipe.addLast("encoder", encoder);
		pipe.addLast("decoder", decoder);
		if (messageLoggerFilter != null) {
			pipe.addLast("loggerUpstream", messageLoggerFilter);
		}
		if (dispatcherReply != null) {
			pipe.addLast("reply", dispatcherReply);
		}
	}
	
	static void setupBootstrapUDP(Bootstrap bootstrap, ChannelHandler timeoutHandler,
			ChannelHandler dispatcherReply, ChannelUpstreamHandler decoder, ChannelDownstreamHandler encoder, ChannelHandler messageLoggerFilter)
	{
		ChannelPipeline pipe = bootstrap.getPipeline();
		if (timeoutHandler != null)
			pipe.addLast("timeout", timeoutHandler);
		pipe.addLast("encoder", encoder);
		pipe.addLast("decoder", decoder);
		if (messageLoggerFilter != null)
			pipe.addLast("loggerUpstream", messageLoggerFilter);
		if (dispatcherReply != null)
		{
			pipe.addLast("reply", dispatcherReply);
		}
	}

	public void tryClose(PeerAddress destination) 
	{
		ChannelFuture channelFuture = cacheMap.get(destination);
		if(channelFuture!=null)
		{
			channelFuture.getChannel().close();
		}
	}
	
	public static long getStatConnectionsCreatedTCP()
	{
		return statConnectionsCreatedTCP.get();
	}
	
	public static long getStatConnectionsCreatedUDP()
	{
		return statConnectionsCreatedUDP.get();
	}
}