package net.tomp2p.holep.strategy;

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
import net.tomp2p.holep.DuplicatesHandler;
import net.tomp2p.holep.HolePunchScheduler;
import net.tomp2p.holep.HolePuncherStrategy;
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
 * DO NOT INSTANCIATE THIS CLASS!
 * 
 * @author Jonas Wagner
 * 
 */
public abstract class AbstractHolePuncherStrategy implements HolePuncherStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractHolePuncherStrategy.class);
	protected static final int NUMBER_OF_TRIALS = HolePunchInitiator.NUMBER_OF_TRIALS;
	protected static final boolean BROADCAST_VALUE = HolePunchInitiator.BROADCAST;
	protected static final boolean FIRE_AND_FORGET_VALUE = false;
	protected final Peer peer;
	protected final int numberOfHoles;
	protected final int idleUDPSeconds;
	protected final Message originalMessage;
	protected PeerAddress originalSender;
	protected List<ChannelFuture> channelFutures = new ArrayList<ChannelFuture>();
	protected List<Pair<Integer, Integer>> portMappings = new ArrayList<Pair<Integer, Integer>>();

	public AbstractHolePuncherStrategy(final Peer peer, final int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
		this.peer = peer;
		this.numberOfHoles = numberOfHoles;
		this.idleUDPSeconds = idleUDPSeconds;
		this.originalMessage = originalMessage;
		LOG.trace("new HolePuncher created, originalMessage {}", originalMessage.toString());
	}
	
	/**
	 * This method cares about which socket contacts which socket on the NATs
	 * which are needed to be traversed.
	 * 
	 * @param replyMessageFuture2
	 * @param replyMessage
	 */
	protected abstract void prepareTargetPeerPorts(final Message replyMessage, final FutureDone<Message> replyMessageFuture2);

	/**
	 * This method needs to be overwritten by each strategy in order to let the
	 * other peer know which ports it need to contact.
	 * 
	 * @param holePMessage
	 * @param initMessageFutureDone
	 * @param channelFutures2
	 */
	protected abstract void prepareInitiatingPeerPorts(final Message holePMessage, final FutureDone<Message> initMessageFutureDone,
			final List<ChannelFuture> channelFutures2);

	/**
	 * This is a generic method which creates a number of {@link ChannelFuture}s
	 * and calls the associated {@link FutureDone} as soon as they're done.
	 * 
	 * @param originalFutureResponse
	 * @param handlersList
	 * @return fDoneChannelFutures
	 */
	private final FutureDone<List<ChannelFuture>> createChannelFutures(final FutureResponse originalFutureResponse,
			final List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> handlersList, final FutureDone<Message> mainFutureDone, final int numberOfHoles) {

		final FutureDone<List<ChannelFuture>> fDoneChannelFutures = new FutureDone<List<ChannelFuture>>();
		final AtomicInteger countDown = new AtomicInteger(numberOfHoles);
		final List<ChannelFuture> channelFutures = new ArrayList<ChannelFuture>();

		for (int i = 0; i < numberOfHoles; i++) {
			final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers = handlersList.get(i);
			FutureChannelCreator fcc = peer.connectionBean().reservation().create(1, 0);
			fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception {
					if (future.isSuccess()) {
						ChannelFuture cF = future.channelCreator().createUDP(BROADCAST_VALUE, handlers, originalFutureResponse);
						cF.addListener(new GenericFutureListener<ChannelFuture>() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								if (future.isSuccess()) {
									channelFutures.add(future);
								} else {
									mainFutureDone.failed("Error while creating the ChannelFutures!");
								}
								countDown.decrementAndGet();
								if (countDown.get() == 0) {
									fDoneChannelFutures.done(channelFutures);
								}
							}
						});
					} else {
						countDown.decrementAndGet();
						mainFutureDone.failed("Error while creating the ChannelFutures!");
					}
				}
			});
		}
		return fDoneChannelFutures;
	}

	/**
	 * This method creates a {@link SimpleChannelInboundHandler} which sends the
	 * original{@link Message} to the nat peer that needs to be contacted.
	 * 
	 * @param futures
	 * @param originalFutureResponse
	 * @return holePhandler
	 */
	private SimpleChannelInboundHandler<Message> createHolePunchInboundHandler(final List<ChannelFuture> futures,
			final FutureResponse originalFutureResponse, final FutureDone<Message> futureDone) {
		SimpleChannelInboundHandler<Message> holePunchInboundHandler = new SimpleChannelInboundHandler<Message>() {
			@Override
			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
				if (checkReplyValues(msg, futureDone)) {
					for (int i = 0; i < msg.intList().size(); i++) {
						ChannelFuture channelFuture = extractChannelFuture(futures, msg.intList().get(i));
						if (channelFuture == null) {
							futureDone.failed("Something went wrong with the portmappings!");
						}
						i++;
						Message sendMessage = createSendOriginalMessage(msg.intList().get(i - 1), msg.intList().get(i));
						peer.connectionBean().sender().afterConnect(originalFutureResponse, sendMessage, channelFuture, false);
						LOG.warn("originalMessage has been sent to the other peer! {}", sendMessage);
					}
				}
			}
		};

		LOG.debug("new HolePunchHandler created, waiting now for answer from rendez-vous peer.");
		return holePunchInboundHandler;
	}

	/**
	 * This method looks up a {@Link ChannelFuture} from the
	 * channelFutures {@link List}. If the {@Link ChannelFuture} can't be
	 * found it returns null instead.
	 * 
	 * @param futures
	 * @param localPort
	 * @return
	 */
	private ChannelFuture extractChannelFuture(List<ChannelFuture> futures, int localPort) {
		for (ChannelFuture future : futures) {
			if (future.channel().localAddress() != null) {
				InetSocketAddress inetSocketAddress = (InetSocketAddress) future.channel().localAddress();
				if (inetSocketAddress.getPort() == localPort) {
					return future;
				}
			}
		}
		return null;
	}

	/**
	 * This method duplicates the original {@link Message} multiple times. This
	 * is needed, because the {@link Buffer} can only be read once.
	 * 
	 * @param originalMessage
	 * @param localPort
	 * @param remotePort
	 * @return
	 */
	private Message createSendOriginalMessage(final int localPort, final int remotePort) {
		PeerAddress sender = originalMessage.sender().changePorts(-1, localPort).changeFirewalledTCP(false).changeFirewalledUDP(false)
				.changeRelayed(false);
		PeerAddress recipient = originalMessage.recipient().changePorts(-1, remotePort).changeFirewalledTCP(false)
				.changeFirewalledUDP(false).changeRelayed(false);
		Message sendMessage = createHolePMessage(recipient, sender, originalMessage.command(), originalMessage.type());
		sendMessage.version(originalMessage.version());
		sendMessage.intValue(originalMessage.messageId());
		sendMessage.udp(true);
		sendMessage.expectDuplicate(true);
		for (Buffer buf : originalMessage.bufferList()) {
			sendMessage.buffer(new Buffer(buf.buffer().duplicate()));
		}
		return sendMessage;
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
	private FutureDone<Message> createInitMessage(final List<ChannelFuture> channelFutures) {
		FutureDone<Message> initMessageFutureDone = new FutureDone<Message>();
		PeerSocketAddress socketAddress = Utils.extractRandomRelay(originalMessage);
		// we need to make a copy of the original Message
		PeerAddress recipient = originalMessage.recipient().changeAddress(socketAddress.inetAddress())
				.changePorts(socketAddress.tcpPort(), socketAddress.udpPort()).changeRelayed(false);
		Message initMessage = createHolePMessage(recipient, originalMessage.sender(), RPC.Commands.HOLEP.getNr(), Message.Type.REQUEST_1);
		initMessage.version(originalMessage.version());
		initMessage.udp(true);
		prepareInitiatingPeerPorts(initMessage, initMessageFutureDone, channelFutures);
		LOG.debug("Hole punch initMessage created {}", initMessage.toString());
		return initMessageFutureDone;
	}

	private Message createDummyMessage(int i) {
		final int remotePort = portMappings.get(i).element0();
		final int localPort = portMappings.get(i).element1();
		PeerAddress recipient = originalSender.changeFirewalledUDP(false).changeRelayed(false).changePorts(-1, remotePort);
		PeerAddress sender = peer.peerBean().serverPeerAddress().changePorts(-1, localPort);
		Message dummyMessage = createHolePMessage(recipient, sender, RPC.Commands.HOLEP.getNr(), Message.Type.REQUEST_3);
		dummyMessage.udp(true);
		return dummyMessage;
	}

	private FutureDone<Message> createReplyMessage() {
		FutureDone<Message> replyMessageFuture2 = new FutureDone<Message>();
		Message replyMessage = createHolePMessage(originalMessage.sender(), peer.peerBean().serverPeerAddress(), Commands.HOLEP.getNr(),
				Type.OK);
		replyMessage.messageId(originalMessage.messageId());
		prepareTargetPeerPorts(replyMessage, replyMessageFuture2);
		return replyMessageFuture2;
	}

	private Message createHolePMessage(final PeerAddress recipient, final PeerAddress sender, final byte command, final Message.Type type) {
		Message message = new Message();
		message.recipient(recipient);
		message.sender(sender);
		message.command(command);
		message.type(type);
		return message;
	}

	/**
	 * This method creates the inboundHandler for the replyMessage of the peer
	 * that we want to send a message to.
	 * 
	 * @return inboundHandler
	 */
	private SimpleChannelInboundHandler<Message> createAfterHolePHandler(final FutureDone<Message> futureDone) {
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
					LOG.debug("Holes successfully punched with ports = {localPort = " + msg.recipient().udpPort() + " , remotePort = "
							+ msg.sender().udpPort() + "}!");
				} else {
					LOG.debug("Holes punche not punched with ports = {localPort = " + msg.recipient().udpPort() + " , remotePort = "
							+ msg.sender().udpPort() + "} yet!");
				}
			}
		};
		return inboundHandler;
	}

	/**
	 * This method initiates the hole punch procedure.
	 * 
	 * @param mainFutureDone
	 * @param originalChannelCreator
	 * @param originalFutureResponse
	 * @param natType
	 * @return mainFutureDone A FutureDone<Message> which if successful contains
	 *         the response Message from the peer we want to contact
	 */
	public FutureDone<Message> initiateHolePunch(final FutureDone<Message> mainFutureDone, final ChannelCreator originalChannelCreator,
			final FutureResponse originalFutureResponse) {
		final FutureDone<List<ChannelFuture>> fDoneChannelFutures = createChannelFutures(originalFutureResponse,
				prepareHandlers(originalFutureResponse, true, mainFutureDone), mainFutureDone, numberOfHoles);
		fDoneChannelFutures.addListener(new BaseFutureAdapter<FutureDone<List<ChannelFuture>>>() {
			@Override
			public void operationComplete(FutureDone<List<ChannelFuture>> future) throws Exception {
				if (future.isSuccess()) {
					final List<ChannelFuture> futures = future.object();
					final FutureDone<Message> initMessage = createInitMessage(futures);
					initMessage.addListener(new BaseFutureAdapter<FutureDone<Message>>() {
						@Override
						public void operationComplete(FutureDone<Message> future) throws Exception {
							if (future.isSuccess()) {
								peer.connectionBean()
										.sender()
										.sendUDP(createHolePunchInboundHandler(futures, originalFutureResponse, mainFutureDone),
												originalFutureResponse, future.object(), originalChannelCreator, idleUDPSeconds,
												BROADCAST_VALUE);
								LOG.debug("ChannelFutures successfully created. Initialization of hole punching started.");
							} else {
								mainFutureDone.failed("The creation of the initMessage failed!");
							}
						}
					});
				} else {
					mainFutureDone.failed("No ChannelFuture could be created!");
				}
			}
		});
		return mainFutureDone;
	}

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
	private List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> prepareHandlers(FutureResponse originalFutureResponse,
			final boolean initiator, final FutureDone<Message> futureDone) {
		List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> handlerList = new ArrayList<Map<String, Pair<EventExecutorGroup, ChannelHandler>>>(
				numberOfHoles);
		SimpleChannelInboundHandler<Message> inboundHandler;
		Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;

		if (initiator) {
			for (int i = 0; i < numberOfHoles; i++) {
				inboundHandler = createAfterHolePHandler(futureDone);
				handlers = peer.connectionBean().sender().configureHandlers(inboundHandler, originalFutureResponse, idleUDPSeconds, false);
				handlerList.add(handlers);
			}
		} else {
			inboundHandler = new DuplicatesHandler(peer.connectionBean().dispatcher());
			for (int i = 0; i < numberOfHoles; i++) {
				handlers = peer.connectionBean().sender().configureHandlers(inboundHandler, originalFutureResponse, idleUDPSeconds, false);
				handlerList.add(handlers);
			}
		}

		return handlerList;
	}

	/**
	 * This method initiates the hole punch procedure on the target peer side.
	 * 
	 * @return
	 */
	public FutureDone<Message> replyHolePunch() {
		originalSender = (PeerAddress) originalMessage.neighborsSetList().get(0).neighbors().toArray()[0];
		final FutureDone<Message> replyMessageFuture = new FutureDone<Message>();
		final FutureResponse frResponse = new FutureResponse(originalMessage);
		final HolePuncherStrategy thisInstance = this;

		final FutureDone<List<ChannelFuture>> rmfChannelFutures = createChannelFutures(frResponse,
				prepareHandlers(frResponse, false, replyMessageFuture), replyMessageFuture, originalMessage.intList().size());
		rmfChannelFutures.addListener(new BaseFutureAdapter<FutureDone<List<ChannelFuture>>>() {
			@Override
			public void operationComplete(FutureDone<List<ChannelFuture>> future) throws Exception {
				if (future.isSuccess()) {
					channelFutures = future.object();
					final FutureDone<Message> replyMessageFuture2 = createReplyMessage();
					replyMessageFuture2.addListener(new BaseFutureAdapter<FutureDone<Message>>() {
						@Override
						public void operationComplete(FutureDone<Message> future) throws Exception {
							if (future.isSuccess()) {
								Message replyMessage = future.object();
								Thread holePunchScheduler = new Thread(new HolePunchScheduler(NUMBER_OF_TRIALS, thisInstance));
								holePunchScheduler.start();
								replyMessageFuture.done(replyMessage);
							} else {
								replyMessageFuture2.failed("No ReplyMessage could be created!");
							}
						}
					});
				} else {
					replyMessageFuture.failed("No ChannelFuture could be created!");
				}
			}
		});
		return replyMessageFuture;
	}

	/**
	 * This methods is only called by a {@link HolePunchScheduler}. It simply
	 * creates a dummyMessage and sends it from a given localPort (
	 * {@link ChannelFuture}) to a given remotePort. This procedure then punches
	 * the holes needed by the initiating {@link Peer}.
	 * 
	 * @throws Exception
	 */
	public void tryConnect() throws Exception {
		if (channelFutures.size() != portMappings.size()) {
			throw new Exception("the number of channels does not match the number of ports!");
		}

		for (int i = 0; i < channelFutures.size(); i++) {
			Message dummyMessage = createDummyMessage(i);
			FutureResponse futureResponse = new FutureResponse(dummyMessage);
			LOG.trace("FIRE! remotePort: " + dummyMessage.recipient().udpPort() + ", localPort: " + dummyMessage.sender().udpPort());
			peer.connectionBean().sender().afterConnect(futureResponse, dummyMessage, channelFutures.get(i), FIRE_AND_FORGET_VALUE);
			// this is a workaround to avoid adding a nat peer to the offline
			// list of a peer!
			peer.peerBean().peerMap().peerFound(originalSender, originalSender, null);
		}
	}

	/**
	 * this method checks if the returned values from the replying nat peer are
	 * valid.
	 * 
	 * @param msg
	 * @return ok
	 */
	private boolean checkReplyValues(Message msg, FutureDone<Message> futureDone) {
		boolean ok = false;
		if (msg.command() == Commands.HOLEP.getNr() && msg.type() == Type.OK) {
			// the list with the ports should never be null or Empty
			if (!(msg.intList() == null || msg.intList().isEmpty())) {
				final int rawNumberOfHoles = msg.intList().size();
				// the number of the pairs of port must be even!
				if ((rawNumberOfHoles % 2) == 0) {
					ok = true;
				} else {
					futureDone.failed("The number of ports in IntList was odd! This should never happen");
				}
			} else {
				futureDone.failed("IntList in replyMessage was null or Empty! No ports available!!!!");
			}
		} else {
			futureDone.failed("Could not acquire a connection via hole punching, got: " + msg);
		}
		LOG.debug("ReplyValues of answerMessage from rendez-vous peer are: " + ok, futureDone);

		return ok;
	}
}
