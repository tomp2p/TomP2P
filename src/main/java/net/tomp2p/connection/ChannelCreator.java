package net.tomp2p.connection;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.TomP2PDecoderTCP;
import net.tomp2p.message.TomP2PDecoderUDP;
import net.tomp2p.message.TomP2PEncoderTCP;
import net.tomp2p.message.TomP2PEncoderUDP;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.rpc.RequestHandlerUDP;

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
	
	public ChannelCreator(ChannelGroup channelsTCP, ChannelGroup channelsUDP, int permits, 
			MessageLogger messageLoggerFilter, ChannelFactory tcpClientChannelFactory, 
			ChannelFactory udpClientChannelFactory, AtomicBoolean shutdown, ConnectionReservation connectionReservation)
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
	}
	
	public Channel createUDPChannel(ReplyTimeoutHandler timeoutHandler,
			RequestHandlerUDP requestHandler, final FutureResponse futureResponse, boolean broadcast) {
		if(shutdown.get())
			return null;
		// If we are out of semaphores, we cannot create any channels. Since we know how many channels max. in parallel are created, we can reserve it. 
		if(!semaphore.tryAcquire())
		{
			throw new RuntimeException("you ran out of permits. You had "+permits+" available, but now its down to 0");
		}
		// now, we don't exceeded the limits, so create channels
		Channel channel = createChannelUDP(timeoutHandler, requestHandler, broadcast);
		channelsUDP.add(channel);
		futureResponse.setExitFast(false);
		channel.getCloseFuture().addListener(new ChannelFutureListener()
		{
			@Override
			public void operationComplete(ChannelFuture future) throws Exception
			{
				channelsUDP.remove(future.getChannel());
				semaphore.release();
				//channel is closed, so we can fire the result to any listener
				futureResponse.fireResponse();
			}
			});
		return channel;
	}
	
	public ChannelFuture createTCPChannel(IdleStateHandler timeoutHandler,
			final FutureResponse futureResponse, int connectTimeoutMillis,
			int idleTCPMillis, Message message, RequestHandlerTCP requestHandler) {
		if(shutdown.get())
			return null;
		// If we are out of semaphores, we cannot create any channels. Since we know how many channels max. in parallel are created, we can reserve it. 
		if(!semaphore.tryAcquire())
		{
			throw new RuntimeException("you ran out of permits. You had "+permits+" available, but now its down to 0");
		}
		// now, we don't exceeded the limits, so create channels
		ChannelFuture channelFuture = createChannelTCP(timeoutHandler, requestHandler,
				message.getRecipient().createSocketTCP(), new InetSocketAddress(0), connectTimeoutMillis);
		Channel channel = channelFuture.getChannel();
		channelsTCP.add(channel);
		futureResponse.setExitFast(false);
		channel.getCloseFuture().addListener(new ChannelFutureListener()
		{
			@Override
			public void operationComplete(ChannelFuture future) throws Exception
			{
				channelsTCP.remove(future.getChannel());
				semaphore.release();
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

	
}