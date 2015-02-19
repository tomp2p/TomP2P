package net.tomp2p.holep;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.HolePunchInitiator;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a generic class which is supposed to handle any hole punching
 * procedure possible on a {@link Peer}.
 * 
 * @author Jonas Wagner
 * 
 */
public class HolePuncher {

	private static final Logger LOG = LoggerFactory.getLogger(HolePuncher.class);

	// these fields are needed from both procedures: initiation and reply
	private static final boolean BROADCAST_VALUE = HolePunchInitiator.BROADCAST;
	private static final boolean FIRE_AND_FORGET_VALUE = false;
	private final Peer peer;
	private final int numberOfHoles;
	private final int idleUDPSeconds;
	private boolean initiator = false;
	private final Message originalMessage;
	private List<ChannelFuture> channelFutures = new ArrayList<ChannelFuture>();

	// these fields are needed for the reply procedure
	private FutureResponse frResponse;
	private PeerAddress originalSender;
	private List<Pair<Integer, Integer>> portMappings = new ArrayList<Pair<Integer, Integer>>();

	// these fields are needed for the initiation procedure
	private FutureDone<Message> mainFutureDone;

	public HolePuncher(final Peer peer, final int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
		this.peer = peer;
		this.numberOfHoles = numberOfHoles;
		this.idleUDPSeconds = idleUDPSeconds;
		this.originalMessage = originalMessage;

		LOG.trace("new HolePuncher created, originalMessage {}", originalMessage.toString());
	}
//
//	@SuppressWarnings("unused")
//	private HolePuncher() {
//		peer = null;
//		originalMessage = null;
//		numberOfHoles = -1;
//		idleUDPSeconds = -1;
//	};
//
//	/*
//	 * ===================== shared methods =====================
//	 */
//
//	/**
//	 * This is a generic method which creates a number of {@link ChannelFuture}s
//	 * and calls the associated {@link FutureDone} as soon as they're done.
//	 * 
//	 * @param originalFutureResponse
//	 * @param handlersList
//	 * @return fDoneChannelFutures
//	 */
//	private final FutureDone<List<ChannelFuture>> createChannelFutures(final FutureResponse originalFutureResponse,
//			final List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> handlersList) {
//
//		final FutureDone<List<ChannelFuture>> fDoneChannelFutures = new FutureDone<List<ChannelFuture>>();
//		final AtomicInteger countDown = new AtomicInteger(numberOfHoles);
//		final List<ChannelFuture> channelFutures = new ArrayList<ChannelFuture>();
//
//		for (int i = 0; i < numberOfHoles; i++) {
//			final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers = handlersList.get(i);
//
//			FutureChannelCreator fcc = peer.connectionBean().reservation().create(1, 0);
//			fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
//				@Override
//				public void operationComplete(FutureChannelCreator future) throws Exception {
//					if (future.isSuccess()) {
//						ChannelFuture cF = future.channelCreator().createUDP(BROADCAST_VALUE, handlers,
//								originalFutureResponse);
//						cF.addListener(new GenericFutureListener<ChannelFuture>() {
//							@Override
//							public void operationComplete(ChannelFuture future) throws Exception {
//								if (future.isSuccess()) {
//									channelFutures.add(future);
//								} else {
//									handleFail("Error while creating the ChannelFutures!");
//								}
//								countDown.decrementAndGet();
//								if (countDown.get() == 0) {
//									fDoneChannelFutures.done(channelFutures);
//								}
//							}
//						});
//					} else {
//						countDown.decrementAndGet();
//						handleFail("Error while creating the ChannelFutures!");
//					}
//				}
//			});
//		}
//		return fDoneChannelFutures;
//	}
//
//	/**
//	 * This method does two things. If the initiating peer calls it, he gets
//	 * back a {@link List} of new {@link SimpleInboundHandler} to deal with the
//	 * replies of the replying peer. If a replying peer is calling this method
//	 * it will return a {@link List} of default {@link SimpleInboundHandler}s
//	 * from the {@link Dispatcher}.
//	 * 
//	 * @param originalFutureResponse
//	 * @return handlerList
//	 */
//	private List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> prepareHandlers(
//			final FutureResponse originalFutureResponse) {
//		List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> handlerList = new ArrayList<Map<String, Pair<EventExecutorGroup, ChannelHandler>>>(
//				numberOfHoles);
//		SimpleChannelInboundHandler<Message> inboundHandler;
//		Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;
//
//		if (initiator) {
//			for (int i = 0; i < numberOfHoles; i++) {
//				inboundHandler = createAfterHolePHandler();
//				handlers = peer.connectionBean().sender()
//						.configureHandlers(inboundHandler, originalFutureResponse, idleUDPSeconds, false);
//				handlerList.add(handlers);
//			}
//		} else {
//			inboundHandler = new DuplicatesHandler(peer.connectionBean().dispatcher());
//			for (int i = 0; i < numberOfHoles; i++) {
//				handlers = peer.connectionBean().sender()
//						.configureHandlers(inboundHandler, originalFutureResponse, idleUDPSeconds, false);
//				handlerList.add(handlers);
//			}
//		}
//
//		return handlerList;
//	}
//
//	private void handleFail(String failMessage) {
//		mainFutureDone.failed(failMessage);
//	}
//
//	/*
//	 * ================ methods on initiating nat peer ================
//	 */
//
//	public FutureDone<Message> initiateHolePunch(final FutureDone<Message> mainFutureDone,
//			final ChannelCreator originalChannelCreator, final FutureResponse originalFutureResponse,
//			final NATType natType) {
//		this.initiator = true;
//		this.mainFutureDone = mainFutureDone;
//
//		FutureDone<List<ChannelFuture>> fDoneChannelFutures = createChannelFutures(originalFutureResponse,
//				prepareHandlers(originalFutureResponse));
//		fDoneChannelFutures.addListener(new BaseFutureAdapter<FutureDone<List<ChannelFuture>>>() {
//
//			@Override
//			public void operationComplete(FutureDone<List<ChannelFuture>> future) throws Exception {
//				if (future.isSuccess()) {
//					final List<ChannelFuture> futures = future.object();
//					peer.connectionBean()
//							.sender()
//							.sendUDP(createHolePunchInboundHandler(futures, originalFutureResponse),
//									originalFutureResponse, createHolePunchInitMessage(futures, -1),
//									originalChannelCreator, idleUDPSeconds, BROADCAST_VALUE);
//					LOG.debug("ChannelFutures successfully created. Initialization of hole punching started.");
//
//				} else {
//					mainFutureDone.failed("No ChannelFuture could be created!");
//				}
//			}
//		});
//		return mainFutureDone;
//	}
//
//	/**
//	 * This method creates a {@link SimpleChannelInboundHandler} which sends the
//	 * original{@link Message} to the nat peer that needs to be contacted.
//	 * 
//	 * @param futures
//	 * @param originalFutureResponse
//	 * @return holePhandler
//	 */
//	private SimpleChannelInboundHandler<Message> createHolePunchInboundHandler(final List<ChannelFuture> futures,
//			final FutureResponse originalFutureResponse) {
//		SimpleChannelInboundHandler<Message> holePunchInboundHandler = new SimpleChannelInboundHandler<Message>() {
//
//			@Override
//			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
//				if (checkReplyValues(msg)) {
//					for (int i = 0; i < msg.intList().size(); i++) {
//						ChannelFuture channelFuture = extractChannelFuture(futures, msg.intList().get(i));
//						if (channelFuture == null) {
//							handleFail("Something went wrong with the portmappings!");
//						}
//						i++;
//						Message sendMessage = createSendOriginalMessage(msg.intList().get(i - 1), msg.intList().get(i));
//						peer.connectionBean().sender()
//								.afterConnect(originalFutureResponse, sendMessage, channelFuture, false);
//						LOG.warn("originalMessage has been sent to the other peer! {}", sendMessage);
//					}
//				}
//			}
//		};
//
//		LOG.debug("new HolePunchHandler created, waiting now for answer from rendez-vous peer.");
//		return holePunchInboundHandler;
//	}
//
//	/**
//	 * This method looks up a {@Link ChannelFuture} from the
//	 * channelFutures {@link List}. If the {@Link ChannelFuture} can't be
//	 * found it returns null instead.
//	 * 
//	 * @param futures
//	 * @param localPort
//	 * @return
//	 */
//	private ChannelFuture extractChannelFuture(List<ChannelFuture> futures, int localPort) {
//		for (ChannelFuture future : futures) {
//			if (future.channel().localAddress() != null) {
//				InetSocketAddress inetSocketAddress = (InetSocketAddress) future.channel().localAddress();
//				if (inetSocketAddress.getPort() == localPort) {
//					return future;
//				}
//			}
//		}
//		return null;
//	}
//
//	/**
//	 * This method creates the inboundHandler for the replyMessage of the peer
//	 * that we want to send a message to.
//	 * 
//	 * @return inboundHandler
//	 */
//	private SimpleChannelInboundHandler<Message> createAfterHolePHandler() {
//		final SimpleChannelInboundHandler<Message> inboundHandler = new SimpleChannelInboundHandler<Message>() {
//
//			@Override
//			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
//				if (Message.Type.OK == msg.type() && originalMessage.command() == msg.command()) {
//					LOG.debug("Successfully transmitted the original message to peer:[" + msg.sender().toString()
//							+ "]. Now here's the reply:[" + msg.toString() + "]");
//					mainFutureDone.done(msg);
//					// TODO jwa does this work?
//					ctx.close();
//				} else if (Message.Type.REQUEST_3 == msg.type() && Commands.HOLEP.getNr() == msg.command()) {
//					LOG.debug("Holes successfully punched with ports = {localPort = " + msg.recipient().udpPort()
//							+ " , remotePort = " + msg.sender().udpPort() + "}!");
//				} else {
//					LOG.debug("Holes punche not punched with ports = {localPort = " + msg.recipient().udpPort()
//							+ " , remotePort = " + msg.sender().udpPort() + "} yet!");
//				}
//			}
//		};
//		return inboundHandler;
//	}
//
//	/**
//	 * this method checks if the returned values from the replying nat peer are
//	 * valid.
//	 * 
//	 * @param msg
//	 * @return ok
//	 */
//	private boolean checkReplyValues(Message msg) {
//		boolean ok = false;
//		if (msg.command() == Commands.HOLEP.getNr() && msg.type() == Type.OK) {
//			// the list with the ports should never be null or Empty
//			if (!(msg.intList() == null || msg.intList().isEmpty())) {
//				final int rawNumberOfHoles = msg.intList().size();
//				// the number of the pairs of port must be even!
//				if ((rawNumberOfHoles % 2) == 0) {
//					ok = true;
//				} else {
//					handleFail("The number of ports in IntList was odd! This should never happen");
//				}
//			} else {
//				handleFail("IntList in replyMessage was null or Empty! No ports available!!!!");
//			}
//		} else {
//			handleFail("Could not acquire a connection via hole punching, got: " + msg);
//		}
//		LOG.debug("ReplyValues of answerMessage from rendez-vous peer are: " + ok);
//		return ok;
//	}
//
//	/**
//	 * This method duplicates the original {@link Message} multiple times. This
//	 * is needed, because the {@link Buffer} can only be read once.
//	 * 
//	 * @param originalMessage
//	 * @param localPort
//	 * @param remotePort
//	 * @return
//	 */
//	private Message createSendOriginalMessage(final int localPort, final int remotePort) {
//		Message sendMessage = new Message();
//		PeerAddress sender = originalMessage.sender().changePorts(-1, localPort).changeFirewalledTCP(false)
//				.changeFirewalledUDP(false).changeRelayed(false);
//		PeerAddress recipient = originalMessage.recipient().changePorts(-1, remotePort).changeFirewalledTCP(false)
//				.changeFirewalledUDP(false).changeRelayed(false);
//		sendMessage.recipient(recipient);
//		sendMessage.sender(sender);
//		sendMessage.version(originalMessage.version());
//		sendMessage.command(originalMessage.command());
//		sendMessage.type(originalMessage.type());
//		sendMessage.intValue(originalMessage.messageId());
//		sendMessage.udp(true);
//		for (Buffer buf : originalMessage.bufferList()) {
//			sendMessage.buffer(new Buffer(buf.buffer().duplicate()));
//		}
//		return sendMessage;
//	}
//
//	/**
//	 * This method creates the initial {@link Message} with {@link Commands}
//	 * .HOLEP and {@link Type}.REQUEST_1. This {@link Message} will be forwarded
//	 * to the rendez-vous server (a relay of the remote peer) and initiate the
//	 * hole punching procedure on the other peer.
//	 * 
//	 * @param message
//	 * @param channelCreator
//	 * @return holePMessage
//	 */
//	private Message createHolePunchInitMessage(final List<ChannelFuture> channelFutures, final long startingPort) {
//		PeerSocketAddress socketAddress = Utils.extractRandomRelay(originalMessage);
//
//		// we need to make a copy of the original Message
//		Message holePMessage = new Message();
//
//		// socketInfoMessage.messageId(message.messageId());
//		PeerAddress recipient = originalMessage.recipient().changeAddress(socketAddress.inetAddress())
//				.changePorts(socketAddress.tcpPort(), socketAddress.udpPort()).changeRelayed(false);
//		holePMessage.recipient(recipient);
//		holePMessage.sender(originalMessage.sender());
//		holePMessage.version(originalMessage.version());
//		holePMessage.udp(true);
//		holePMessage.command(RPC.Commands.HOLEP.getNr());
//		holePMessage.type(Message.Type.REQUEST_1);
//
//		// TODO jwa --> create something like a configClass or file where the
//		// number of holes in the firewall can be specified.
//		for (int i = 1; i < channelFutures.size(); i++) {
//			InetSocketAddress inetSocketAddress = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
//			holePMessage.intValue(inetSocketAddress.getPort());
//		}
//		
//		
//		
//		LOG.debug("Hole punch initMessage created {}", holePMessage.toString());
//		return holePMessage;
//	}
//
//	/*
//	 * ================ methods on target nat peer ================
//	 */
//
//	public FutureDone<Message> replyHolePunch() {
//		this.originalSender = (PeerAddress) originalMessage.neighborsSetList().get(0).neighbors().toArray()[0];
//		final FutureDone<Message> replyMessageFuture = new FutureDone<Message>();
//		frResponse = new FutureResponse(originalMessage);
//		final HolePuncher thisInstance = this;
//
//		FutureDone<List<ChannelFuture>> rmfChannelFutures = createChannelFutures(frResponse,
//				prepareHandlers(frResponse));
//		rmfChannelFutures.addListener(new BaseFutureAdapter<FutureDone<List<ChannelFuture>>>() {
//
//			@Override
//			public void operationComplete(FutureDone<List<ChannelFuture>> future) throws Exception {
//				if (future.isSuccess()) {
//					channelFutures = future.object();
//					doPortMappings();
//					Message replyMessage = createReplyMessage();
//					// TODO jwa create some config class to specify the number
//					// of trials
//					Thread holePunchScheduler = new Thread(new HolePunchScheduler(10, thisInstance));
//					holePunchScheduler.start();
//					replyMessageFuture.done(replyMessage);
//				} else {
//					replyMessageFuture.failed("No ChannelFuture could be created!");
//				}
//			}
//		});
//		return replyMessageFuture;
//	}
//
//	private void doPortMappings() {
//		for (int i = 0; i < channelFutures.size(); i++) {
//			InetSocketAddress socket = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
//			portMappings.add(new Pair<Integer, Integer>(originalMessage.intList().get(i), socket.getPort()));
//		}
//	}
//
//	private Message createDummyMessage(int i) {
//		Message dummyMessage = new Message();
//		final int remotePort = portMappings.get(i).element0();
//		final int localPort = portMappings.get(i).element1();
//		PeerAddress recipient = originalSender.changeFirewalledUDP(false).changeRelayed(false)
//				.changePorts(-1, remotePort);
//		PeerAddress sender = peer.peerBean().serverPeerAddress().changePorts(-1, localPort);
//		dummyMessage.recipient(recipient);
//		dummyMessage.command(Commands.HOLEP.getNr());
//		dummyMessage.type(Type.REQUEST_3);
//		dummyMessage.sender(sender);
//		dummyMessage.udp(true);
//		return dummyMessage;
//	}
//
//	private Message createReplyMessage() {
//		Message replyMessage = new Message();
//		replyMessage.messageId(originalMessage.messageId());
//		replyMessage.recipient(originalMessage.sender());
//		replyMessage.sender(peer.peerBean().serverPeerAddress());
//		replyMessage.command(Commands.HOLEP.getNr());
//		replyMessage.type(Message.Type.OK);
//		replyMessage.messageId(originalMessage.messageId());
//		for (Pair<Integer, Integer> pair : portMappings) {
//			if (!(pair == null || pair.isEmpty() || pair.element0() == null || pair.element1() == null)) {
//				replyMessage.intValue(pair.element0());
//				replyMessage.intValue(pair.element1());
//			}
//		}
//		return replyMessage;
//	}
//
//	/**
//	 * This methods is only called by a {@link HolePunchScheduler}. It simply
//	 * creates a dummyMessage and sends it from a given localPort (
//	 * {@link ChannelFuture}) to a given remotePort. This procedure then punches
//	 * the holes needed by the initiating {@link Peer}.
//	 * 
//	 * @throws Exception
//	 */
//	public void tryConnect() throws Exception {
//		if (channelFutures.size() != portMappings.size()) {
//			throw new Exception("the number of channels does not match the number of ports!");
//		}
//
//		for (int i = 0; i < channelFutures.size(); i++) {
//			Message dummyMessage = createDummyMessage(i);
//			FutureResponse futureResponse = new FutureResponse(dummyMessage);
//			LOG.trace("FIRE! remotePort: " + dummyMessage.recipient().udpPort() + ", localPort: "
//					+ dummyMessage.sender().udpPort());
//			peer.connectionBean().sender()
//					.afterConnect(futureResponse, dummyMessage, channelFutures.get(i), FIRE_AND_FORGET_VALUE);
//			// this is a workaround to avoid adding a nat peer to the offline
//			// list of a peer!
//			peer.peerBean().peerMap().peerFound(originalSender, originalSender, null);
//		}
//	}
}
