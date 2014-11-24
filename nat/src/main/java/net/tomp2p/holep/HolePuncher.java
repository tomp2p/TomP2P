package net.tomp2p.holep;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.Ports;
import net.tomp2p.connection.Sender;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.Pair;

// TODO jwa implement Hole Puncher
public class HolePuncher implements IPunchHole {
	
	private final Message message;
	private final ChannelCreator channelCreator;
	private final Peer peer;
	private int outgoingPort = 0;
	private int incomingPort = 0;

	public HolePuncher(final Message message, final ChannelCreator channelCreator, Peer peer, int outgoingPort, int incomingPort) {
		this.message = message;
		this.channelCreator = channelCreator;
		this.peer = peer;
		this.outgoingPort = outgoingPort;
		this.incomingPort = incomingPort;
	}
	
	public static Ports punchHoleUDP() {
		
		return null;
	}
	
	public ChannelFuture createAndSendUDP() {
		ChannelFuture future = createUDP();
		return sendUDP(future);
	}

	public ChannelFuture sendUDP(ChannelFuture future) {
		return future.channel().writeAndFlush(message);
	}

	private ChannelFuture createUDP() {
		final FutureResponse futureResponse = new FutureResponse(message);
		Sender sender = peer.connectionBean().sender();
		
		// we must predefine a socket in order to make sure that the outgoing port is known to us
		final InetAddress localInetAddress = peer.peerBean().serverPeerAddress().createSocketUDP().getAddress();
		InetSocketAddress localAddress = new InetSocketAddress(localInetAddress, outgoingPort);
		
		final InetAddress remoteInetAddress = message.recipient().inetAddress();
		InetSocketAddress remoteAddress = new InetSocketAddress(remoteInetAddress, incomingPort);
		
		// we must create a special handler to handle the connection
		SimpleChannelInboundHandler<Message> holePunchHandler = new SimpleChannelInboundHandler<Message>() {
			
			@Override
			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
				if (msg.isOk()) {
					System.err.println("SUCCESS!!!!!");
				} else {
					System.err.println("FAIL IN INBOUNDHANDLER OF HOLEPUNCHER!");
				}
			}
		};
		
		Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers = sender.configureHandlers(holePunchHandler, futureResponse, 30, false);
		
		ChannelFuture channelFuture = channelCreator.createUDP(false, handlers, futureResponse, localAddress);
		channelFuture = channelFuture.channel().connect(remoteAddress, localAddress);
		channelFuture.addListener(new GenericFutureListener<Future<? super Void>>() {

			@Override
			public void operationComplete(Future<? super Void> future) throws Exception {
				System.out.println("TURN DOWN FOR WHAT?");
			}
		});
		return channelFuture;
	}

	public ChannelCreator channelCreator() {
		return channelCreator;
	}

	public int outgoingPort() {
		return outgoingPort;
	}

	public int incomingPort() {
		return incomingPort;
	}
	
	@Override
	public void tryConnect() {
		createAndSendUDP();
	}
	
}
