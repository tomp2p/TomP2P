package net.tomp2p.holep;

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
	
	public PortPreservingStrategy(Peer peer, int numberOfHoles, int idleUDPSeconds, Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}
	
	/*
	 * ===================== shared methods =====================
	 */

	/**
	 * This method does two things. If the initiating peer calls it, he gets
	 * back a {@link List} of new {@link SimpleInboundHandler} to deal with the
	 * replies of the replying peer. If a replying peer is calling this method
	 * it will return a {@link List} of default {@link SimpleInboundHandler}s
	 * from the {@link Dispatcher}.
	 * 
	 * @param originalFutureResponse
	 * @return handlerList
	 */
	private List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> prepareHandlers(
			FutureResponse originalFutureResponse, final boolean initiator, final FutureDone<Message> futureDone) {
		List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> handlerList = new ArrayList<Map<String, Pair<EventExecutorGroup, ChannelHandler>>>(
				numberOfHoles);
		SimpleChannelInboundHandler<Message> inboundHandler;
		Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;

		if (initiator) {
			for (int i = 0; i < numberOfHoles; i++) {
				inboundHandler = createAfterHolePHandler(futureDone);
				handlers = peer.connectionBean().sender()
						.configureHandlers(inboundHandler, originalFutureResponse, idleUDPSeconds, false);
				handlerList.add(handlers);
			}
		} else {
			inboundHandler = new DuplicatesHandler(peer.connectionBean().dispatcher());
			for (int i = 0; i < numberOfHoles; i++) {
				handlers = peer.connectionBean().sender()
						.configureHandlers(inboundHandler, originalFutureResponse, idleUDPSeconds, false);
				handlerList.add(handlers);
			}
		}

		return handlerList;
	}

	@Override
	protected SimpleChannelInboundHandler<Message> createAfterHolePHandler(final FutureDone<Message> futureDone) {
		final SimpleChannelInboundHandler<Message> inboundHandler = new SimpleChannelInboundHandler<Message>() {

			@Override
			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
				if (Message.Type.OK == msg.type() && originalMessage.command() == msg.command()) {
					LOG.debug("Successfully transmitted the original message to peer:[" + msg.sender().toString()
							+ "]. Now here's the reply:[" + msg.toString() + "]");
					futureDone.done(msg);
					// TODO jwa does this work?
					ctx.close();
				} else if (Message.Type.REQUEST_3 == msg.type() && Commands.HOLEP.getNr() == msg.command()) {
					LOG.debug("Holes successfully punched with ports = {localPort = " + msg.recipient().udpPort()
							+ " , remotePort = " + msg.sender().udpPort() + "}!");
				} else {
					LOG.debug("Holes punche not punched with ports = {localPort = " + msg.recipient().udpPort()
							+ " , remotePort = " + msg.sender().udpPort() + "} yet!");
				}
			}
		};
		return inboundHandler;
	}
	
	/*
	 * ================ methods on initiating nat peer ================
	 */

	public FutureDone<Message> initiateHolePunch(final FutureDone<Message> mainFutureDone,
			final ChannelCreator originalChannelCreator, final FutureResponse originalFutureResponse,
			final NATType natType) {

		FutureDone<List<ChannelFuture>> fDoneChannelFutures = createChannelFutures(originalFutureResponse,
				prepareHandlers(originalFutureResponse, true, mainFutureDone), mainFutureDone);
		fDoneChannelFutures.addListener(new BaseFutureAdapter<FutureDone<List<ChannelFuture>>>() {

			@Override
			public void operationComplete(FutureDone<List<ChannelFuture>> future) throws Exception {
				if (future.isSuccess()) {
					final List<ChannelFuture> futures = future.object();
					peer.connectionBean()
							.sender()
							.sendUDP(createHolePunchInboundHandler(futures, originalFutureResponse, mainFutureDone),
									originalFutureResponse, createHolePunchInitMessage(futures, -1),
									originalChannelCreator, idleUDPSeconds, BROADCAST_VALUE);
					LOG.debug("ChannelFutures successfully created. Initialization of hole punching started.");

				} else {
					mainFutureDone.failed("No ChannelFuture could be created!");
				}
			}
		});
		return mainFutureDone;
	}
	
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
	
	/**
	 * This method creates the initial {@link Message} with {@link Commands}
	 * .HOLEP and {@link Type}.REQUEST_1. This {@link Message} will be forwarded
	 * to the rendez-vous server (a relay of the remote peer) and initiate the
	 * hole punching procedure on the other peer.
	 * 
	 * @param message
	 * @param channelCreator
	 * @return holePMessage
	 */
	protected Message createHolePunchInitMessage(final List<ChannelFuture> channelFutures, final long startingPort) {
		// we need to make a copy of the original Message
		Message holePMessage = super.createHolePunchInitMessage(channelFutures, startingPort);

		// signal the other peer what type of NAT we are using
		holePMessage.longValue(NAT_TYPE.ordinal());
		
		// TODO jwa --> create something like a configClass or file where the
		// number of holes in the firewall can be specified.
		for (int i = 1; i < channelFutures.size(); i++) {
			InetSocketAddress inetSocketAddress = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
			holePMessage.intValue(inetSocketAddress.getPort());
		}
		
		return holePMessage;
	}
	
	/*
	 * ================ methods on target nat peer ================
	 */
	
	public FutureDone<Message> replyHolePunch() {
		this.originalSender = (PeerAddress) originalMessage.neighborsSetList().get(0).neighbors().toArray()[0];
		final FutureDone<Message> replyMessageFuture = new FutureDone<Message>();
		frResponse = new FutureResponse(originalMessage);
		final HolePuncher thisInstance = this;

		FutureDone<List<ChannelFuture>> rmfChannelFutures = createChannelFutures(frResponse,
				prepareHandlers(frResponse));
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
}
