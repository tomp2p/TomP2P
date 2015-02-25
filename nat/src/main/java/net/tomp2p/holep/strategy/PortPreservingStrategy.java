package net.tomp2p.holep.strategy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.holep.NATType;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

public class PortPreservingStrategy extends AbstractHolePuncherStrategy {

	private static final NATType NAT_TYPE = NATType.PORT_PRESERVING;

	public PortPreservingStrategy(final Peer peer, final int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}

	@Override
	protected void doPortGuessingInitiatingPeer(final Message holePMessage, final FutureDone<Message> initMessageFutureDone,
			final List<ChannelFuture> channelFutures) throws Exception {
		// TODO jwa --> create something like a configClass or file where the
		// number of holes in the firewall can be specified.
		List<Integer> portList = new ArrayList<Integer>(numberOfHoles);
		for (int i = 0; i < channelFutures.size(); i++) {
			InetSocketAddress inetSocketAddress = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
			holePMessage.intValue(inetSocketAddress.getPort());
			portList.add(inetSocketAddress.getPort());
		}

		byte[] bytes = Utils.encodeJavaObject(portList);
		Buffer byteBuf = new Buffer(Unpooled.wrappedBuffer(bytes));
		holePMessage.buffer(byteBuf);

		// signal the other peer what type of NAT we are using
		holePMessage.longValue(NAT_TYPE.ordinal());
		initMessageFutureDone.done(holePMessage);
	}

	@Override
	protected void doPortGuessingTargetPeer(final Message replyMessage, final FutureDone<Message> replyMessageFuture2) throws Exception {
		for (int i = 0; i < channelFutures.size(); i++) {
			InetSocketAddress socket = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
			portMappings.add(new Pair<Integer, Integer>(originalMessage.intList().get(i), socket.getPort()));
			replyMessage.intValue((int) originalMessage.intList().get(i));
			replyMessage.intValue(socket.getPort());
		}
		replyMessageFuture2.done(replyMessage);
	}

	/**
	 * This method is just for testing causes.
	 */
	@Override
	protected FutureDone<List<ChannelFuture>> createChannelFutures(final FutureResponse originalFutureResponse,
			final List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> handlersList, final FutureDone<Message> mainFutureDone,
			final int numberOfHoles) {
		return super.createChannelFutures(originalFutureResponse, handlersList, mainFutureDone, numberOfHoles);
	}

	/**
	 * This method is just for testing causes.
	 */
	@Override
	protected List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> prepareHandlers(FutureResponse originalFutureResponse,
			final boolean initiator, final FutureDone<Message> futureDone) {
		return super.prepareHandlers(originalFutureResponse, initiator, futureDone);
	}

}
