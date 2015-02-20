package net.tomp2p.holep.strategy;

import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.List;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.holep.NATType;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Content;
import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.Pair;

public class PortPreservingStrategy extends AbstractHolePuncherStrategy {

	private static final NATType NAT_TYPE = NATType.PORT_PRESERVING;

	public PortPreservingStrategy(final Peer peer, final int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}

	@Override
	protected void prepareInitiatingPeerPorts(final Message holePMessage, final FutureDone<Message> initMessageFutureDone, final List<ChannelFuture> channelFutures) {
		// TODO jwa --> create something like a configClass or file where the
		// number of holes in the firewall can be specified.
		for (int i = 1; i < channelFutures.size(); i++) {
			InetSocketAddress inetSocketAddress = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
			holePMessage.intValue(inetSocketAddress.getPort());
		}

		// signal the other peer what type of NAT we are using
		holePMessage.longValue(NAT_TYPE.ordinal());
		initMessageFutureDone.done(holePMessage);
	}

	@Override
	protected void prepareTargetPeerPorts(final Message replyMessage, final FutureDone<Message> replyMessageFuture2) {
		replyMessage.presetContentTypes(true);
		replyMessage.contentType(Content.INTEGER);
		for (int i = 0; i < channelFutures.size(); i++) {
			InetSocketAddress socket = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
			portMappings.add(new Pair<Integer, Integer>(originalMessage.intList().get(i), socket.getPort()));
			replyMessage.intValue((int) originalMessage.intList().get(i));
			replyMessage.intValue(socket.getPort());
		}
		replyMessageFuture2.done(replyMessage);
	}

}
