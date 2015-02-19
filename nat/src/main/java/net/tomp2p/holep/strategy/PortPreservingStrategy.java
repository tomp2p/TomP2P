package net.tomp2p.holep.strategy;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.holep.DuplicatesHandler;
import net.tomp2p.holep.HolePunchScheduler;
import net.tomp2p.holep.NATType;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

public class PortPreservingStrategy extends AbstractHolePuncher {

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
	protected void prepareInitiatingPeerPorts(final Message holePMessage, final FutureDone<Message> initMessageFutureDone) {
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

	public FutureDone<Message> replyHolePunch() {
		originalSender = (PeerAddress) originalMessage.neighborsSetList().get(0).neighbors().toArray()[0];
		final FutureDone<Message> replyMessageFuture = new FutureDone<Message>();
		FutureResponse frResponse = new FutureResponse(originalMessage);
		final PortPreservingStrategy thisInstance = this;

		FutureDone<List<ChannelFuture>> rmfChannelFutures = createChannelFutures(frResponse,
				prepareHandlers(frResponse, false, replyMessageFuture), replyMessageFuture);
		rmfChannelFutures.addListener(new BaseFutureAdapter<FutureDone<List<ChannelFuture>>>() {

			@Override
			public void operationComplete(FutureDone<List<ChannelFuture>> future) throws Exception {
				if (future.isSuccess()) {
					channelFutures = future.object();
					doPortMappings();
					Message replyMessage = createReplyMessage();
					// TODO jwa create some config class to specify the number
					// of trials
					Thread holePunchScheduler = new Thread(new HolePunchScheduler(10, thisInstance));
					holePunchScheduler.start();
					replyMessageFuture.done(replyMessage);
				} else {
					replyMessageFuture.failed("No ChannelFuture could be created!");
				}
			}
		});
		return replyMessageFuture;
	}

	protected void doPortMappings() {
		for (int i = 0; i < channelFutures.size(); i++) {
			InetSocketAddress socket = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
			portMappings.add(new Pair<Integer, Integer>(originalMessage.intList().get(i), socket.getPort()));
		}
	}

}
