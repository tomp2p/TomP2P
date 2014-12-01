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
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Pair;

// TODO jwa implement Hole Puncher
public class HolePuncher implements IPunchHole {
	
	private static final int IDLE_UDP_SECONDS = 30;
	private static final int DEFAULT_TRIALS = 10;
	private final Message message;
	private final ChannelCreator channelCreator;
	private final Peer peer;
	private final PeerAddress recipient;
	private int localPort = 0;
	private int remotePort = 0;

	public HolePuncher(final Message message, final ChannelCreator channelCreator, Peer peer, int localPort, int remotePort, PeerAddress recipient) {
		this.message = message;
		this.channelCreator = channelCreator;
		this.peer = peer;
		this.localPort = localPort;
		this.remotePort = remotePort;
		this.recipient = recipient;
	}
	
	
	public ChannelFuture createAndSendUDP() {

		final FutureResponse futureResponse = new FutureResponse(message);
		Sender sender = peer.connectionBean().sender();
		
		// we must predefine a socket in order to make sure that the outgoing port is known to us
		final InetAddress localInetAddress = peer.peerBean().serverPeerAddress().createSocketUDP().getAddress();
		InetSocketAddress localAddress = new InetSocketAddress(localInetAddress, localPort);
		
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
		
		Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers = sender.configureHandlers(peer.connectionBean().dispatcher(), futureResponse, IDLE_UDP_SECONDS, false);
		
		ChannelFuture channelFuture = channelCreator.createUDP(false, handlers, futureResponse, localAddress);
		
		System.err.println("##### HolePuncher #####");
		System.err.println("localAddress = " + localAddress.toString());
		System.err.println("remoteAddress = "+ message.recipient().createSocketUDP());
		System.err.println("##### HolePuncher #####");
		
		sender.afterConnect(futureResponse, message, channelFuture, false);
		
//		Thread holePuncher = new Thread(new HolePunchScheduler(DEFAULT_TRIALS, this));
//		holePuncher.run();
		
		peer.peerBean().peerMap().peerFound(recipient, recipient, null);
		
		return channelFuture;
	}

	public ChannelCreator channelCreator() {
		return channelCreator;
	}

	public int localPort() {
		return localPort;
	}

	public int remotePort() {
		return remotePort;
	}
	
	@Override
	public void tryConnect() {
		createAndSendUDP();
	}
	
}
