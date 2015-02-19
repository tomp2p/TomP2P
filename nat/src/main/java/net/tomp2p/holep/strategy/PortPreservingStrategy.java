package net.tomp2p.holep.strategy;

import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.List;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.holep.NATType;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PortPreservingStrategy extends AbstractHolePuncherStrategy {

	private static final NATType NAT_TYPE = NATType.PORT_PRESERVING;
	private static final Logger LOG = LoggerFactory.getLogger(PortPreservingStrategy.class);

	public PortPreservingStrategy(final Peer peer, final int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}

	/*
	 * ================ methods on initiating nat peer ================
	 */

	protected boolean checkReplyValues(Message msg, FutureDone<Message> futureDone) {
		boolean ok = false;
		if (msg.command() == Commands.HOLEP.getNr() && msg.type() == Type.OK) {
			// the list with the ports should never be null or Empty
			if (!(msg.intList() == null || msg.intList().isEmpty())) {
				final int rawNumberOfHoles = msg.intList().size();
				// the number of the pairs of port must be even!
				if ((rawNumberOfHoles % 2) == 0) {
					ok = true;
				} else {
					handleFail("The number of ports in IntList was odd! This should never happen", futureDone);
				}
			} else {
				handleFail("IntList in replyMessage was null or Empty! No ports available!!!!", futureDone);
			}
		} else {
			handleFail("Could not acquire a connection via hole punching, got: " + msg, futureDone);
		}
		LOG.debug("ReplyValues of answerMessage from rendez-vous peer are: " + ok, futureDone);

		return ok;
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

	/*
	 * ================ methods on target nat peer ================
	 */

	protected void prepareTargetPeerPorts(final Message replyMessage, final FutureDone<Message> replyMessageFuture2) {
		for (int i = 0; i < channelFutures.size(); i++) {
			InetSocketAddress socket = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
			portMappings.add(new Pair<Integer, Integer>(originalMessage.intList().get(i), socket.getPort()));
			replyMessage.intValue(originalMessage.intList().get(i));
			replyMessage.intValue(socket.getPort());
		}
		replyMessageFuture2.done(replyMessage);
	}

}
