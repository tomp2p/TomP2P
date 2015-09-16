/*
 * Copyright 2013 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.connection;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GenericFutureListener;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Cancel;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FuturePing;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataFilter;
import net.tomp2p.message.DataFilterTTL;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.TomP2PCumulationTCP;
import net.tomp2p.message.TomP2POutbound;
import net.tomp2p.message.TomP2PSinglePacketUDP;
import net.tomp2p.p2p.builder.PingBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.ConcurrentCacheMap;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

/**
 * The class that sends out messages.
 * 
 * @author Thomas Bocek
 * 
 */
public class Sender {

	private static final Logger LOG = LoggerFactory.getLogger(Sender.class);
	private final List<PeerStatusListener> peerStatusListeners;
	private final ChannelClientConfiguration channelClientConfiguration;
	private final Dispatcher dispatcher;
	private final SendBehavior sendBehavior;
	private final Random random;
	private final PeerBean peerBean;
	private final DataFilter dataFilterTTL = new DataFilterTTL();

	// this map caches all messages which are meant to be sent by a reverse
	// connection setup
	private final ConcurrentCacheMap<Integer, Pair<FutureResponse, FutureResponse>> cachedRequests = new ConcurrentCacheMap<Integer, Pair<FutureResponse, FutureResponse>>(30, 1024);

	private PingBuilderFactory pingBuilderFactory;

	/**
	 * Creates a new sender with the listeners for offline peers.
	 * 
	 * @param peerStatusListeners
	 *            The listener for offline peers
	 * @param channelClientConfiguration
	 *            The configuration used to get the signature factory
	 * @param dispatcher
	 * @param sendBehavior
	 * @param peerBean
	 */
	public Sender(final Number160 peerId, final List<PeerStatusListener> peerStatusListeners,
			final ChannelClientConfiguration channelClientConfiguration, Dispatcher dispatcher, SendBehavior sendBehavior, PeerBean peerBean) {
		this.peerStatusListeners = peerStatusListeners;
		this.channelClientConfiguration = channelClientConfiguration;
		this.dispatcher = dispatcher;
		this.sendBehavior = sendBehavior;
		this.random = new Random(peerId.hashCode());
		this.peerBean = peerBean;
	}

	public ChannelClientConfiguration channelClientConfiguration() {
		return channelClientConfiguration;
	}

	public PingBuilderFactory pingBuilderFactory() {
		return pingBuilderFactory;
	}

	public Sender pingBuilderFactory(PingBuilderFactory pingBuilderFactory) {
		this.pingBuilderFactory = pingBuilderFactory;
		return this;
	}

	/**
	 * Sends a message via TCP.
	 * 
	 * @param handler
	 *            The handler to deal with a reply message.
	 * @param futureResponse
	 *            The future to set the response.
	 * @param message
	 *            The message to send
	 * @param channelCreator
	 *            The channel creator for the TCP channel.
	 * @param idleTCPSeconds
	 *            The idle time until message fail.
	 * @param connectTimeoutMillis
	 *            The idle time for the connection setup.
	 * @param peerConnection
	 *
	 */
	public void sendTCP(final SimpleChannelInboundHandler<Message> handler, final FutureResponse futureResponse, final Message message,
			final ChannelCreator channelCreator, final int idleTCPMillis, final int connectTimeoutMillis,
			final PeerConnection peerConnection) {
		// no need to continue if we already finished
		if (futureResponse.isCompleted()) {
			return;
		}
		// NAT reflection - rewrite recipient if we found a local address for
		// the recipient
		PeerSocketAddress reflectedRecipient = Utils.natReflection(message.recipient(), dispatcher.peerBean().serverPeerAddress());
		if(reflectedRecipient != null) {
			PeerSocketAddress orig = message.recipient().peerSocketAddress();
			message.saveOriginalRecipientBeforeTranslation(orig);
			message.recipient(message.recipient().changePeerSocketAddress(reflectedRecipient));
			message.reflected();
			LOG.debug("reflect recipient TCP {}", message);
		}
		
		removePeerIfFailed(futureResponse, message);

		// RTT calculation
		futureResponse.startRTTMeasurement(false);

		final ChannelFuture channelFuture;
		if (peerConnection != null && peerConnection.channelFuture() != null && peerConnection.channelFuture().channel().isActive()) {
			channelFuture = sendTCPPeerConnection(peerConnection, handler, channelCreator, futureResponse);
			afterConnect(futureResponse, message, channelFuture, handler == null);
		} else if (channelCreator != null) {
			final TimeoutFactory timeoutHandler = createTimeoutHandler(futureResponse, idleTCPMillis, handler == null);

			switch (sendBehavior.tcpSendBehavior(message)) {
			case DIRECT:
				connectAndSend(handler, futureResponse, channelCreator, connectTimeoutMillis, peerConnection, timeoutHandler, message);
				break;
			case RCON:
				handleRcon(handler, futureResponse, message, channelCreator, connectTimeoutMillis, peerConnection, timeoutHandler);
				break;
			case RELAY:
				handleRelay(handler, futureResponse, message, channelCreator, idleTCPMillis, connectTimeoutMillis, peerConnection,
						timeoutHandler);
				break;
			case SELF:
				sendSelf(futureResponse, message);
				break;
			default:
				throw new IllegalArgumentException("Illegal sending behavior");
			}
		}
	}

	/**
	 * This method initiates the reverse connection setup (or short: rconSetup).
	 * It creates a new Message and sends it via relay to the unreachable peer
	 * which then connects to this peer again. After the connectMessage from the
	 * unreachable peer this peer will send the original Message and its content
	 * directly.
	 * 
	 * @param handler
	 * @param futureResponse
	 * @param message
	 * @param channelCreator
	 * @param connectTimeoutMillis
	 * @param peerConnection
	 * @param timeoutHandler
	 */
	private void handleRcon(final SimpleChannelInboundHandler<Message> handler, final FutureResponse futureResponse, final Message message,
			final ChannelCreator channelCreator, final int connectTimeoutMillis, final PeerConnection peerConnection,
			final TimeoutFactory timeoutHandler) {
		message.keepAlive(true);

		LOG.debug("initiate reverse connection setup to peer with peerAddress {}", message.recipient());
		Message rconMessage = createRconMessage(message);
		final FutureResponse rconResponse = new FutureResponse(rconMessage);

		// cache the original message until the connection is established
		cachedRequests.put(message.messageId(), new Pair<FutureResponse, FutureResponse>(futureResponse, rconResponse));

		// wait for response (whether the reverse connection setup was
		// successful)

		SimpleChannelInboundHandler<Message> rconInboundHandler = new SimpleChannelInboundHandler<Message>() {
			@Override
			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
				if (msg.command() == Commands.RCON.getNr() && msg.type() == Type.OK) {
					LOG.debug("Successfully set up the reverse connection to peer {}", message.recipient().peerId());
					rconResponse.response(msg);
				} else {
					LOG.debug("Could not acquire a reverse connection, msg: {}", message);
					rconResponse.failed("Could not acquire a reverse connection, msg: " + message);
					futureResponse.failed(rconResponse);
					cachedRequests.remove(message.messageId());
				}
			}
		};

		// send reverse connection request instead of normal message
		sendTCP(rconInboundHandler, rconResponse, rconMessage, channelCreator, connectTimeoutMillis, connectTimeoutMillis, peerConnection);
	}

	/**
	 * This method makes a copy of the original Message and prepares it for
	 * sending it to the relay.
	 * 
	 * @param message
	 * @return rconMessage
	 */
	private static Message createRconMessage(final Message message) {
		// get Relay InetAddress from unreachable peer
		PeerSocketAddress socketAddress = Utils.extractRandomRelay(message);

		// we need to make a copy of the original message
		Message rconMessage = new Message();
		rconMessage.sender(message.sender());
		rconMessage.version(message.version());

		// store the message id in the payload to get the cached message later
		rconMessage.intValue(message.messageId());

		// the message must have set the keepAlive Flag true. If not, the relay
		// peer will close the PeerConnection to the unreachable peer.
		rconMessage.keepAlive(true);
		// making the message ready to send
		readyToSend(message, socketAddress, rconMessage, RPC.Commands.RCON.getNr(), Message.Type.REQUEST_1);

		return rconMessage;
	}

	/**
	 * This method was extracted from createRconMessage(...), in order to avoid
	 * duplicate code in createHolePMessage(...).
	 * 
	 * @param originalMessage
	 * @param socketAddress
	 * @param newMessage
	 * @param RPCCommand
	 * @param messageType
	 */
	private static void readyToSend(final Message originalMessage, PeerSocketAddress socketAddress, Message newMessage, byte RPCCommand,
			Type messageType) {
		PeerAddress recipient = originalMessage.recipient().changeAddress(socketAddress.inetAddress())
				.changePorts(socketAddress.tcpPort(), socketAddress.udpPort()).changeRelayed(false);
		newMessage.recipient(recipient);

		newMessage.command(RPCCommand);
		newMessage.type(messageType);
	}

	/**
	 * This method is extracted by @author jonaswagner to ensure that no
	 * duplicate code exist.
	 * 
	 * @param handler
	 * @param futureResponse
	 * @param channelCreator
	 * @param connectTimeoutMillis
	 * @param peerConnection
	 * @param timeoutHandler
	 * @param message
	 */
	private void connectAndSend(final SimpleChannelInboundHandler<Message> handler, final FutureResponse futureResponse,
			final ChannelCreator channelCreator, final int connectTimeoutMillis, final PeerConnection peerConnection,
			final TimeoutFactory timeoutHandler, final Message message) {
		InetSocketAddress recipient = message.recipient().createSocketTCP();
		final ChannelFuture channelFuture = sendTCPCreateChannel(recipient, channelCreator, peerConnection, handler, timeoutHandler,
				connectTimeoutMillis, futureResponse);
		afterConnect(futureResponse, message, channelFuture, handler == null);
	}

	/**
	 * Both peers are relayed, thus sending directly or over reverse connection
	 * is not possible. Send the message to one of the receiver's relays.
	 * 
	 * @param handler
	 * @param futureResponse
	 * @param message
	 * @param channelCreator
	 * @param idleTCPSeconds
	 * @param connectTimeoutMillis
	 * @param peerConnection
	 * @param timeoutHandler
	 */
	private void handleRelay(final SimpleChannelInboundHandler<Message> handler, final FutureResponse futureResponse,
			final Message message, final ChannelCreator channelCreator, final int idleTCPMillis, final int connectTimeoutMillis,
			final PeerConnection peerConnection, final TimeoutFactory timeoutHandler) {
		FutureDone<PeerSocketAddress> futurePing = pingFirst(message.recipient().peerSocketAddresses());
		futurePing.addListener(new BaseFutureAdapter<FutureDone<PeerSocketAddress>>() {
			@Override
			public void operationComplete(final FutureDone<PeerSocketAddress> futureDone) throws Exception {
				if (futureDone.isSuccess()) {
					InetSocketAddress recipient = PeerSocketAddress.createSocketTCP(futureDone.object());
					ChannelFuture channelFuture = sendTCPCreateChannel(recipient, channelCreator, peerConnection, handler, timeoutHandler,
							connectTimeoutMillis, futureResponse);
					afterConnect(futureResponse, message, channelFuture, handler == null);

					futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
						@Override
						public void operationComplete(FutureResponse future) throws Exception {
							if (future.isFailed()) {
								if (future.responseMessage() != null && future.responseMessage().type() != Message.Type.DENIED) {
									// remove the failed relay and try again
									clearInactivePeerSocketAddress(futureDone);
									sendTCP(handler, futureResponse, message, channelCreator, idleTCPMillis, connectTimeoutMillis,
											peerConnection);
								}
							}
						}

						private void clearInactivePeerSocketAddress(final FutureDone<PeerSocketAddress> futureDone) {
							Collection<PeerSocketAddress> tmp = new ArrayList<PeerSocketAddress>();
							for (PeerSocketAddress psa : message.recipient().peerSocketAddresses()) {
								if (psa != null) {
									if (!psa.equals(futureDone.object())) {
										tmp.add(psa);
									}
								}
							}
							message.peerSocketAddresses(tmp);
						}
					});

				} else {
					futureResponse.failed("No relay could be contacted.", futureDone);
				}
			}
		});
	}

	/**
	 * Ping all relays of the receiver. The first one answering is picked as the
	 * responsible relay for this message.
	 * 
	 * @param peerSocketAddresses
	 *            a collection of relay addresses
	 * @return
	 */
	private FutureDone<PeerSocketAddress> pingFirst(Collection<PeerSocketAddress> peerSocketAddresses) {
		final FutureDone<PeerSocketAddress> futureDone = new FutureDone<PeerSocketAddress>();

		FuturePing[] forks = new FuturePing[peerSocketAddresses.size()];
		int index = 0;
		for (PeerSocketAddress psa : peerSocketAddresses) {
			if (psa != null) {
				InetSocketAddress inetSocketAddress = PeerSocketAddress.createSocketUDP(psa);
				PingBuilder pingBuilder = pingBuilderFactory.create();
				forks[index++] = pingBuilder.inetAddress(inetSocketAddress.getAddress()).port(inetSocketAddress.getPort()).start();
			}
		}
		FutureForkJoin<FuturePing> ffk = new FutureForkJoin<FuturePing>(1, true, new AtomicReferenceArray<FuturePing>(forks));
		ffk.addListener(new BaseFutureAdapter<FutureForkJoin<FuturePing>>() {
			@Override
			public void operationComplete(FutureForkJoin<FuturePing> future) throws Exception {
				if (future.isSuccess()) {
					futureDone.done(future.first().remotePeer().peerSocketAddress());
				} else {
					futureDone.failed(future);
				}
			}
		});
		return futureDone;
	}

	/**
	 * In case a message is sent to the sender itself, this is the cutoff.
	 * 
	 * @param futureResponse
	 *            the future to respond as soon as the proper handler returns it
	 * @param message
	 *            the request
	 */
	public void sendSelf(final FutureResponse futureResponse, final Message message) {
		LOG.debug("Handle message that is intended for the sender itself {}", message);
		message.sendSelf();

		Message copy = message.duplicate(new DataFilter() {
			@Override
			public Data filter(Data data, boolean isConvertMeta, boolean isReply) {
				if (data.isSigned() && data.signature() == null) {
					data.protectEntry(message.privateKey());
				}
				// set new valid from as this data item might have an old one
				data.validFromMillis(System.currentTimeMillis());
				return data.duplicate();
			}
		});

		final DispatchHandler handler = dispatcher.associatedHandler(copy);
		handler.forwardMessage(copy, null, new Responder() {

			@Override
			public FutureDone<Void> response(final Message responseMessage) {
				Message copy = responseMessage.duplicate(dataFilterTTL);
				futureResponse.response(copy);
				return new FutureDone<Void>().done();
			}

			@Override
			public void failed(Type type, String reason) {
				futureResponse.failed("Failed with type " + type.name() + ". Reason: " + reason);
			}

			@Override
			public void responseFireAndForget() {
				futureResponse.emptyResponse();
			}

		});
	}

	private ChannelFuture sendTCPCreateChannel(InetSocketAddress recipient, ChannelCreator channelCreator, PeerConnection peerConnection,
			ChannelHandler handler, TimeoutFactory timeoutHandler, int connectTimeoutMillis, FutureResponse futureResponse) {

		final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;

		if (timeoutHandler != null) {
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>();
			handlers.put("timeout0", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.idleStateHandlerTomP2P()));
			handlers.put("timeout1", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.timeHandler()));
		} else {
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>();
		}

		handlers.put("decoder",
				new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2PCumulationTCP(channelClientConfiguration.signatureFactory(), 
						channelClientConfiguration.byteBufAllocator())));
		handlers.put(
				"encoder",
				new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2POutbound(channelClientConfiguration.signatureFactory(),
						channelClientConfiguration.byteBufAllocator())));

		if (peerConnection != null) {
			// we expect replies on this connection
			handlers.put("dispatcher", new Pair<EventExecutorGroup, ChannelHandler>(null, dispatcher));
		}

		if (timeoutHandler != null) {
			handlers.put("handler", new Pair<EventExecutorGroup, ChannelHandler>(null, handler));
		}

		HeartBeat heartBeat = null;
		if (peerConnection != null) {
			heartBeat = new HeartBeat(peerConnection.heartBeatMillis(), TimeUnit.MILLISECONDS, pingBuilderFactory);
			handlers.put("heartbeat", new Pair<EventExecutorGroup, ChannelHandler>(null, heartBeat));
		}

		ChannelFuture channelFuture = channelCreator.createTCP(recipient, connectTimeoutMillis, handlers, futureResponse);

		if (peerConnection != null && channelFuture != null) {
			peerConnection.channelFuture(channelFuture);
			heartBeat.peerConnection(peerConnection);
		}
		return channelFuture;
	}

	private ChannelFuture sendTCPPeerConnection(PeerConnection peerConnection, ChannelHandler handler, final ChannelCreator channelCreator,
			final FutureResponse futureResponse) {
		// if the channel gets closed, the future should get notified
		ChannelFuture channelFuture = peerConnection.channelFuture();
		// channelCreator can be null if we don't need to create any channels
		if (channelCreator != null) {
			channelCreator.setupCloseListener(channelFuture, futureResponse);
		}
		ChannelPipeline pipeline = channelFuture.channel().pipeline();

		// we need to replace the handler if this comes from the peer that
		// create a peerConnection, otherwise we
		// need to add a handler
		addOrReplace(pipeline, "dispatcher", "handler", handler);
		// uncomment this if the recipient should also heartbeat
		// addIfAbsent(pipeline, "handler", "heartbeat",
		// new HeartBeat(2, pingBuilder).peerConnection(peerConnection));
		return channelFuture;
	}

	// private boolean addIfAbsent(ChannelPipeline pipeline, String before,
	// String name,
	// ChannelHandler channelHandler) {
	// List<String> names = pipeline.names();
	// if (names.contains(name)) {
	// return false;
	// } else {
	// if (before == null) {
	// pipeline.addFirst(name, channelHandler);
	// } else {
	// pipeline.addBefore(before, name, channelHandler);
	// }
	// return true;
	// }
	// }

	private boolean addOrReplace(ChannelPipeline pipeline, String before, String name, ChannelHandler channelHandler) {
		List<String> names = pipeline.names();
		if (names.contains(name)) {
			pipeline.replace(name, name, channelHandler);
			return false;
		} else {
			if (before == null) {
				pipeline.addFirst(name, channelHandler);
			} else {
				pipeline.addBefore(before, name, channelHandler);
			}
			return true;
		}
	}

	/**
	 * Sends a message via UDP.
	 * 
	 * @param handler
	 *            The handler to deal with a response message
	 * @param futureResponse
	 *            The future to set the response
	 * @param message
	 *            The message to send
	 * @param channelCreator
	 *            The channel creator for the UDP channel
	 * @param idleUDPSeconds
	 *            The idle time of a message until fail
	 * @param broadcast
	 *            True, if the message is to be sent via layer 2 broadcast
	 */
	// TODO: if message.getRecipient() is me, than call dispatcher directly
	// without sending over Internet.
	public void sendUDP(final SimpleChannelInboundHandler<Message> handler, final FutureResponse futureResponse, final Message message,
			final ChannelCreator channelCreator, final int idleUDPMillis, final boolean broadcast) {

		// no need to continue if we already finished
		if (futureResponse.isCompleted()) {
			return;
		}

		// NAT reflection - rewrite recipient if we found a local address for
		// the recipient
		PeerSocketAddress reflectedRecipient = Utils.natReflection(message.recipient(), dispatcher.peerBean().serverPeerAddress());
		if(reflectedRecipient != null) {
			PeerSocketAddress orig = message.recipient().peerSocketAddress();
			message.saveOriginalRecipientBeforeTranslation(orig);
			message.recipient(message.recipient().changePeerSocketAddress(reflectedRecipient));
			message.reflected();
			LOG.debug("reflect recipient UDP {}", message);
		}

		removePeerIfFailed(futureResponse, message);

		if (message.sender().isRelayed()) {
			message.peerSocketAddresses(message.sender().peerSocketAddresses());
		}

		boolean isFireAndForget = handler == null;

		final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers = configureHandlers(handler, futureResponse, idleUDPMillis,
				isFireAndForget);

		// RTT calculation
		futureResponse.startRTTMeasurement(true);

		try {
			ChannelFuture channelFuture = null;
			switch (sendBehavior.udpSendBehavior(message)) {
			case DIRECT:
				channelFuture = channelCreator.createUDP(broadcast, handlers, futureResponse);
				break;
			case HOLEP:
				if (peerBean.holePunchInitiator() != null) {
					handleHolePunch(futureResponse, message, channelCreator, idleUDPMillis, handler, broadcast, handlers, channelFuture);
					// all the send mechanics are done in a
					// AbstractHolePuncherStrategy class.
					// Therefore we must execute this return statement.
					return;
				} else {
					LOG.debug("No hole punching possible, because There is no PeerNAT. New Attempt with Relaying");
				}
			case RELAY:
				try {
					channelFuture = prepareRelaySendUDP(futureResponse, message, channelCreator, broadcast, handlers, channelFuture);
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}
				break;
			case SELF:
				sendSelf(futureResponse, message);
				channelFuture = null;
				break;
			default:
				throw new IllegalArgumentException("UDP messages are not allowed to send over RCON");
			}
			afterConnect(futureResponse, message, channelFuture, handler == null);
		} catch (UnsupportedOperationException e) {
			LOG.warn(e.getMessage());
			futureResponse.failed(e);

		}
	}

	/**
	 * This method needed to be extracted from sendUDP(...), because it is also
	 * needed by the method handleHolePunch(...).
	 * 
	 * @param futureResponse
	 * @param message
	 * @param channelCreator
	 * @param broadcast
	 * @param handlers
	 * @param channelFuture
	 * @return
	 * @throws Exception
	 */
	private ChannelFuture prepareRelaySendUDP(final FutureResponse futureResponse, final Message message,
			final ChannelCreator channelCreator, final boolean broadcast,
			final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers, ChannelFuture channelFuture) throws Exception {
		List<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>(message.recipient().peerSocketAddresses());
		LOG.debug("send neighbor request to random relay peer {}", psa);
		if (psa.size() > 0) {
			PeerSocketAddress ps = psa.get(random.nextInt(psa.size()));
			message.recipientRelay(message.recipient().changePeerSocketAddress(ps).changeRelayed(true));
			channelFuture = channelCreator.createUDP(broadcast, handlers, futureResponse);
		} else {
			String failMessage = "Peer is relayed, but no relay given";
			futureResponse.failed(failMessage);
			throw new Exception(failMessage);
		}
		return channelFuture;
	}

	private FutureDone<Message> handleHolePunch(final FutureResponse futureResponse, final Message message,
			final ChannelCreator channelCreator, final int idleUDPMillis, final SimpleChannelInboundHandler<Message> handler,
			final boolean broadcast, final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers, final ChannelFuture channelFuture) {
		// start hole punching
		FutureDone<Message> fDone = peerBean.holePunchInitiator().handleHolePunch(idleUDPMillis, futureResponse, message);
		fDone.addListener(new BaseFutureAdapter<FutureDone<Message>>() {

			@Override
			public void operationComplete(FutureDone<Message> future) throws Exception {
				if (future.isSuccess()) {
					futureResponse.response(future.object());
				} else {
					LOG.error(future.failedReason());
					LOG.error("Message could not be sent with hole punching! New send attempt with relaying.");
					// futureResponse.failed(future.failedReason());
					// throw new Exception(future.failedReason());
					doRelayFallback(futureResponse, message, broadcast, handlers, channelFuture);
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				// futureResponse.failed(t);
				// throw new Exception(t);
				LOG.error("The setup of a connection via has been canceled, because an error was thrown");
				t.printStackTrace();
				doRelayFallback(futureResponse, message, broadcast, handlers, channelFuture);
			}

			private void doRelayFallback(final FutureResponse futureResponse, final Message message, final boolean broadcast,
					final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers, ChannelFuture channelFuture) {
				try {
					channelFuture = prepareRelaySendUDP(futureResponse, message, channelCreator, broadcast, handlers, channelFuture);
					afterConnect(futureResponse, message, channelFuture, handler == null);
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}
			}
		});
		return fDone;
	}

	/**
	 * This method was extracted in order to avoid duplicate code in the
	 * {@link HolePInitiator} and in the initHolePunch(...) method.
	 * 
	 * @param handler
	 * @param futureResponse
	 * @param idleUDPSeconds
	 * @param isFireAndForget
	 * @return handlers
	 */
	public Map<String, Pair<EventExecutorGroup, ChannelHandler>> configureHandlers(final SimpleChannelInboundHandler<Message> handler,
			final FutureResponse futureResponse, final int idleUDPMillis, boolean isFireAndForget) {
		final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;
		if (isFireAndForget) {
			final int nrTCPHandlers = 3; // 2 / 0.75
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrTCPHandlers);
		} else {
			final int nrTCPHandlers = 7; // 5 / 0.75
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrTCPHandlers);
			final TimeoutFactory timeoutHandler = createTimeoutHandler(futureResponse, idleUDPMillis, isFireAndForget);
			handlers.put("timeout0", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.idleStateHandlerTomP2P()));
			handlers.put("timeout1", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.timeHandler()));
		}

		handlers.put(
				"decoder",
				new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2PSinglePacketUDP(channelClientConfiguration.signatureFactory())));
		handlers.put(
				"encoder",
				new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2POutbound(channelClientConfiguration.signatureFactory(), channelClientConfiguration.byteBufAllocator())));
		if (!isFireAndForget) {
			handlers.put("handler", new Pair<EventExecutorGroup, ChannelHandler>(null, handler));
		}
		return handlers;
	}

	/**
	 * Creates a timeout handler or null if it is a fire and forget message.
	 * In this case we don't expect a response and we don't need a timeout.
	 * 
	 * @param futureResponse
	 *            The future to set the response
	 * @param idleMillis
	 *            The timeout
	 * @param fireAndForget
	 *            True, if we don't expect a response
	 * @return The timeout factory that will create timeout handlers
	 */
	private TimeoutFactory createTimeoutHandler(final FutureResponse futureResponse, final int idleMillis, final boolean fireAndForget) {
		return fireAndForget ? null : new TimeoutFactory(futureResponse, idleMillis, peerStatusListeners, "Sender");
	}

	/**
	 * After connecting, we check if the connect was successful.
	 * 
	 * @param futureResponse
	 *            The future to set the response
	 * @param message
	 *            The message to send
	 * @param channelFuture
	 *            the future of the connect
	 * @param fireAndForget
	 *            True, if we don't expect a message
	 */
	public void afterConnect(final FutureResponse futureResponse, final Message message, final ChannelFuture channelFuture,
			final boolean fireAndForget) {
		if (channelFuture == null) {
			futureResponse.failed("could not create a " + (message.isUdp() ? "UDP" : "TCP") + " channel");
			return;
		}
		LOG.debug("about to connect to {} with channel {}, ff={}, msg={}", message.recipient(), channelFuture.channel(), fireAndForget, message);
		final Cancel connectCancel = createCancel(channelFuture);
		futureResponse.setCancel(connectCancel);
		channelFuture.addListener(new GenericFutureListener<ChannelFuture>() {
			@Override
			public void operationComplete(final ChannelFuture future) throws Exception {
				
				if (future.isSuccess()) {
					final ChannelFuture writeFuture = future.channel().writeAndFlush(message);
					afterSend(writeFuture, futureResponse, fireAndForget);
				} else {
					LOG.debug("Channel creation failed", future.cause());
					futureResponse.failed("Channel creation failed " + future.channel() + "/" + future.cause());
					// may have been closed by the other side,
					// or it may have been canceled from this side
					if (!(future.cause() instanceof CancellationException) && !(future.cause() instanceof ClosedChannelException)
							&& !(future.cause() instanceof ConnectException)) {
						LOG.warn("Channel creation failed to {} for {}", future.channel(), message);
					}
				}
			}
		});
	}

	/**
	 * After sending, we check if the write was successful or if it was a fire
	 * and forget.
	 * 
	 * @param writeFuture
	 *            The future of the write operation. Can be UDP or TCP
	 * @param futureResponse
	 *            The future to set the response
	 * @param fireAndForget
	 *            True, if we don't expect a message
	 */
	private void afterSend(final ChannelFuture writeFuture, final FutureResponse futureResponse, final boolean fireAndForget) {
		final Cancel writeCancel = createCancel(writeFuture);
		futureResponse.setCancel(writeCancel);
		writeFuture.addListener(new GenericFutureListener<ChannelFuture>() {

			@Override
			public void operationComplete(final ChannelFuture future) throws Exception {
				if (!future.isSuccess()) {
					futureResponse.failedLater(future.cause());
					reportFailed(futureResponse, future.channel().close());
					LOG.warn("Failed to write channel the request {} {}.", futureResponse.request(), future.cause());
				}
				if (fireAndForget) {
					futureResponse.responseLater(null);
					LOG.debug("fire and forget, close channel {} now. {}", futureResponse.request(), future.channel());
					reportMessage(futureResponse, future.channel().close());
				}
			}
		});

	}

	/**
	 * Report a the response after the channel was closed.
	 * 
	 * @param futureResponse
	 *            The future to set the response
	 * @param close
	 *            The close future
	 */
	private void reportFailed(final FutureResponse futureResponse, final ChannelFuture close) {
		close.addListener(new GenericFutureListener<ChannelFuture>() {
			@Override
			public void operationComplete(final ChannelFuture arg0) throws Exception {
				futureResponse.responseNow();
			}
		});
	}

	/**
	 * Report a successful response after the channel was closed.
	 * 
	 * @param futureResponse
	 *            The future to set the response
	 * @param close
	 *            The close future
	 */
	private void reportMessage(final FutureResponse futureResponse, final ChannelFuture close) {
		close.addListener(new GenericFutureListener<ChannelFuture>() {
			@Override
			public void operationComplete(final ChannelFuture arg0) throws Exception {
				futureResponse.responseNow();
			}
		});
	}

	/**
	 * @param channelFuture
	 *            The channel future that can be canceled
	 * @return Create a cancel class for the channel future
	 */
	private static Cancel createCancel(final ChannelFuture channelFuture) {
		return new Cancel() {
			@Override
			public void cancel() {
				channelFuture.cancel(true);
			}
		};
	}

	private void removePeerIfFailed(final FutureResponse futureResponse, final Message message) {
		futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
			@Override
			public void operationComplete(FutureResponse future) throws Exception {
				if (future.isFailed()) {
					if (message.recipient().isRelayed()) {
						// TODO: make the relay go away if failed
					} else if (message.command() == RPC.Commands.HOLEP.getNr() && message.type().ordinal() == Message.Type.REQUEST_3.ordinal()) {
						//do nothing, because such a (dummy) message will never reach its target the first time
					}
					LOG.debug("peer failed: {}", message);
					synchronized (peerStatusListeners) {
						for (PeerStatusListener peerStatusListener : peerStatusListeners) {
							peerStatusListener.peerFailed(message.recipient(), new PeerException(future));
						}
					}
				}
			}
		});
	}

	/**
	 * Get currently cached requests. They are cached because for example the
	 * receiver is behind a NAT. Instead of sending the message directly, a
	 * reverse connection is set up beforehand. After a successful connection
	 * establishment, the cached messages are sent through the direct channel.
	 */
	public ConcurrentCacheMap<Integer, Pair<FutureResponse, FutureResponse>> cachedRequests() {
		return cachedRequests;
	}

	public List<PeerStatusListener> peerStatusListeners() {
		return peerStatusListeners;
	}
}
