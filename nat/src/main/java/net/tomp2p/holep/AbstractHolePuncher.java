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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ChannelCreator;
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

/**
 * DO NOT INSTANCIATE THIS CLASS!
 * 
 * @author Jonas Wagner
 * 
 */
public abstract class AbstractHolePuncher implements HolePuncherStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractHolePuncher.class);
	protected static final boolean BROADCAST_VALUE = HolePunchInitiator.BROADCAST;
	protected static final boolean FIRE_AND_FORGET_VALUE = false;
	protected final Peer peer;
	protected final int numberOfHoles;
	protected final int idleUDPSeconds;
	protected final Message originalMessage;

	public AbstractHolePuncher(final Peer peer, final int numberOfHoles, final int idleUDPSeconds,
			final Message originalMessage) {
		this.peer = peer;
		this.numberOfHoles = numberOfHoles;
		this.idleUDPSeconds = idleUDPSeconds;
		this.originalMessage = originalMessage;

		LOG.trace("new HolePuncher created, originalMessage {}", originalMessage.toString());
	}

	/**
	 * This is a generic method which creates a number of {@link ChannelFuture}s
	 * and calls the associated {@link FutureDone} as soon as they're done.
	 * 
	 * @param originalFutureResponse
	 * @param handlersList
	 * @return fDoneChannelFutures
	 */
	protected final FutureDone<List<ChannelFuture>> createChannelFutures(final FutureResponse originalFutureResponse,
			final List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> handlersList,
			final FutureDone<Message> mainFutureDone) {

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
						ChannelFuture cF = future.channelCreator().createUDP(BROADCAST_VALUE, handlers,
								originalFutureResponse);
						cF.addListener(new GenericFutureListener<ChannelFuture>() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								if (future.isSuccess()) {
									channelFutures.add(future);
								} else {
									handleFail("Error while creating the ChannelFutures!", mainFutureDone);
								}
								countDown.decrementAndGet();
								if (countDown.get() == 0) {
									fDoneChannelFutures.done(channelFutures);
								}
							}
						});
					} else {
						countDown.decrementAndGet();
						handleFail("Error while creating the ChannelFutures!", mainFutureDone);
					}
				}
			});
		}
		return fDoneChannelFutures;
	}

	protected void handleFail(final String failMessage, final FutureDone<Message> futureDone) {
		futureDone.failed(failMessage);
	}

	/**
	 * This method creates a {@link SimpleChannelInboundHandler} which sends the
	 * original{@link Message} to the nat peer that needs to be contacted.
	 * 
	 * @param futures
	 * @param originalFutureResponse
	 * @return holePhandler
	 */
	protected SimpleChannelInboundHandler<Message> createHolePunchInboundHandler(final List<ChannelFuture> futures,
			final FutureResponse originalFutureResponse, final FutureDone<Message> futureDone) {
		SimpleChannelInboundHandler<Message> holePunchInboundHandler = new SimpleChannelInboundHandler<Message>() {

			@Override
			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
				if (checkReplyValues(msg, futureDone)) {
					for (int i = 0; i < msg.intList().size(); i++) {
						ChannelFuture channelFuture = extractChannelFuture(futures, msg.intList().get(i));
						if (channelFuture == null) {
							handleFail("Something went wrong with the portmappings!", futureDone);
						}
						i++;
						Message sendMessage = createSendOriginalMessage(msg.intList().get(i - 1), msg.intList().get(i));
						peer.connectionBean().sender()
								.afterConnect(originalFutureResponse, sendMessage, channelFuture, false);
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
	protected ChannelFuture extractChannelFuture(List<ChannelFuture> futures, int localPort) {
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
	protected Message createSendOriginalMessage(final int localPort, final int remotePort) {
		Message sendMessage = new Message();
		PeerAddress sender = originalMessage.sender().changePorts(-1, localPort).changeFirewalledTCP(false)
				.changeFirewalledUDP(false).changeRelayed(false);
		PeerAddress recipient = originalMessage.recipient().changePorts(-1, remotePort).changeFirewalledTCP(false)
				.changeFirewalledUDP(false).changeRelayed(false);
		sendMessage.recipient(recipient);
		sendMessage.sender(sender);
		sendMessage.version(originalMessage.version());
		sendMessage.command(originalMessage.command());
		sendMessage.type(originalMessage.type());
		sendMessage.intValue(originalMessage.messageId());
		sendMessage.udp(true);
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
	protected Message createHolePunchInitMessage(final List<ChannelFuture> channelFutures, final long startingPort) {
		PeerSocketAddress socketAddress = Utils.extractRandomRelay(originalMessage);

		// we need to make a copy of the original Message
		Message holePMessage = new Message();

		// socketInfoMessage.messageId(message.messageId());
		PeerAddress recipient = originalMessage.recipient().changeAddress(socketAddress.inetAddress())
				.changePorts(socketAddress.tcpPort(), socketAddress.udpPort()).changeRelayed(false);
		holePMessage.recipient(recipient);
		holePMessage.sender(originalMessage.sender());
		holePMessage.version(originalMessage.version());
		holePMessage.udp(true);
		holePMessage.command(RPC.Commands.HOLEP.getNr());
		holePMessage.type(Message.Type.REQUEST_1);

		LOG.debug("Hole punch initMessage created {}", holePMessage.toString());
		return holePMessage;
	}

	/**
	 * This method creates the inboundHandler for the replyMessage of the peer
	 * that we want to send a message to.
	 * 
	 * @return inboundHandler
	 */
	protected abstract SimpleChannelInboundHandler<Message> createAfterHolePHandler(FutureDone<Message> futureDone);

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
	public abstract FutureDone<Message> initiateHolePunch(final FutureDone<Message> mainFutureDone,
			final ChannelCreator originalChannelCreator, final FutureResponse originalFutureResponse,
			final NATType natType);

	/**
	 * this method checks if the returned values from the replying nat peer are
	 * valid.
	 * 
	 * @param msg
	 * @return ok
	 */
	protected abstract boolean checkReplyValues(Message msg, FutureDone<Message> futureDone);
	
	/**
	 * This method initiates the hole punch procedure on the target peer side.
	 * @return
	 */
	public abstract FutureDone<Message> replyHolePunch();
}
