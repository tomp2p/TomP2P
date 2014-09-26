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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GenericFutureListener;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Cancel;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FuturePing;
import net.tomp2p.futures.FutureResponse;
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
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private final Random random;

	// this map caches all messages which are meant to be sent by a reverse
	// connection setup
	private final ConcurrentHashMap<Integer, FutureResponse> cachedMessages = new ConcurrentHashMap<Integer, FutureResponse>();

	private PingBuilderFactory pingBuilderFactory;

	/**
	 * Creates a new sender with the listeners for offline peers.
	 * 
	 * @param peerStatusListeners
	 *            The listener for offline peers
	 * @param channelClientConfiguration
	 *            The configuration used to get the signature factory
	 * @param dispatcher
	 * @param concurrentHashMap 
	 */
	public Sender(final Number160 peerId, final List<PeerStatusListener> peerStatusListeners,
	        final ChannelClientConfiguration channelClientConfiguration, Dispatcher dispatcher) {
		this.peerStatusListeners = peerStatusListeners;
		this.channelClientConfiguration = channelClientConfiguration;
		this.dispatcher = dispatcher;
		this.random = new Random(peerId.hashCode());
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
	 * Send a message via TCP.
	 * 
	 * @param handler
	 *            The handler to deal with a reply message
	 * @param futureResponse
	 *            The future to set the response
	 * @param message
	 *            The message to send
	 * @param channelCreator
	 *            The channel creator for the UPD channel
	 * @param idleTCPSeconds
	 *            The idle time of a message until we fail
	 * @param connectTimeoutMillis
	 *            The idle we set for the connection setup
	 */
	public void sendTCP(final SimpleChannelInboundHandler<Message> handler, final FutureResponse futureResponse,
	        final Message message, final ChannelCreator channelCreator, final int idleTCPSeconds,
	        final int connectTimeoutMillis, final PeerConnection peerConnection) {
		// no need to continue if we already finished
		if (futureResponse.isCompleted()) {
			return;
		}
		removePeerIfFailed(futureResponse, message);
		//we need to set the neighbors if we use relays
		if(message.sender().isRelayed() && !message.sender().peerSocketAddresses().isEmpty()) {
			message.peerSocketAddresses(message.sender().peerSocketAddresses());
		}

		final ChannelFuture channelFuture;
		if (peerConnection != null && peerConnection.channelFuture() != null
		        && peerConnection.channelFuture().channel().isActive()) {
			channelFuture = sendTCPPeerConnection(peerConnection, handler, channelCreator, futureResponse);
			afterConnect(futureResponse, message, channelFuture, handler == null);
		} else if (channelCreator != null) {
			final TimeoutFactory timeoutHandler = createTimeoutHandler(futureResponse, idleTCPSeconds, handler == null);
			// check relay
			if (message.recipient().isRelayed()) {
				// check if reverse connection is possible
				if (!message.sender().isRelayed()) {
					handleRcon(handler, futureResponse, message, channelCreator, connectTimeoutMillis, peerConnection, timeoutHandler);
				} else {
					handleRelay(handler, futureResponse, message, channelCreator, idleTCPSeconds, connectTimeoutMillis, peerConnection, 
							timeoutHandler);
				}
			} else {
				connectAndSend(handler, futureResponse, channelCreator, connectTimeoutMillis, peerConnection, timeoutHandler, message);
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

		// cache the original message until the connection is established
		cachedMessages.put(message.messageId(), futureResponse);

		// wait for response (whether the reverse connection setup was successful)
		final FutureResponse rconResponse = new FutureResponse(rconMessage);
		
//		rconResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
//			@Override
//			public void operationComplete(FutureResponse rconResponse) throws Exception {
//				if(rconResponse.isSuccess()) {
//					final PeerConnection openConnection = openConnections.get(message.recipient().peerId());
//					if(openConnection == null) {
//						LOG.error("Cannot find an open connection to peer {}", message.recipient().peerId());
//						futureResponse.failed("Failed to open reverse connection to unreachable peer");
//					} else {
//						LOG.debug("Opened reverse connection channel. Send message through it");
//						FutureChannelCreator futureChannelCreator = openConnection.acquire(futureResponse);
//						futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
//							@Override
//							public void operationComplete(FutureChannelCreator future) throws Exception {
//								if (future.isSuccess()) {
//									futureResponse.request().keepAlive(true);
//									sendTCP(handler, futureResponse, message, openConnection.channelCreator(), ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP, connectTimeoutMillis, openConnection);
//								} else {
//									futureResponse.failed("Could not acquire channel of reverse connection", future);
//								}
//							}
//						});
//					}
//				}
//			}
//		});
		
		SimpleChannelInboundHandler<Message> rconInboundHandler = new SimpleChannelInboundHandler<Message>() {
			@Override
			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
				if(msg.command() == Commands.RCON.getNr() && msg.type() == Type.OK) {
					LOG.debug("Successfully set up the reverse connection to peer {}", message.recipient().peerId());
					rconResponse.response(msg);
				} else {
					LOG.debug("Could not acquire a reverse connection to peer {}", message.recipient().peerId());
					rconResponse.failed("Could not acquire a reverse connection");
					futureResponse.failed(rconResponse.failedReason());
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
		Object[] relayInetAdresses = message.recipient().peerSocketAddresses().toArray();
		PeerSocketAddress socketAddress = null;
		
		if (relayInetAdresses.length > 0) {
			// we should be fair and choose one of the relays randomly
			socketAddress = (PeerSocketAddress) relayInetAdresses[Utils.randomPositiveInt(relayInetAdresses.length)]; 
		} else {
			throw new IllegalArgumentException(
					"There are no PeerSocketAdresses available for this relayed Peer. This should not be possible!");
		}

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
		PeerAddress recipient = message.recipient().changeAddress(socketAddress.inetAddress()).changePorts(socketAddress.tcpPort(), socketAddress.udpPort()).changeRelayed(false);
		rconMessage.recipient(recipient);
		rconMessage.command(RPC.Commands.RCON.getNr());
		rconMessage.type(Message.Type.REQUEST_1);

		return rconMessage;
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
		LOG.debug("about to connect to {} with channel {}, ff={}", recipient, channelFuture.channel(), handler == null);
		afterConnect(futureResponse, message, channelFuture, handler == null);
	}

	/**
	 * TODO: document what is done here
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
	        final Message message, final ChannelCreator channelCreator, final int idleTCPSeconds,
	        final int connectTimeoutMillis, final PeerConnection peerConnection, final TimeoutFactory timeoutHandler) {
		FutureDone<PeerSocketAddress> futurePing = pingFirst(message.recipient().peerSocketAddresses(),
		        pingBuilderFactory);
		futurePing.addListener(new BaseFutureAdapter<FutureDone<PeerSocketAddress>>() {
			@Override
			public void operationComplete(final FutureDone<PeerSocketAddress> futureDone) throws Exception {
				if (futureDone.isSuccess()) {
					InetSocketAddress recipient = PeerSocketAddress.createSocketTCP(futureDone.object());
					ChannelFuture channelFuture = sendTCPCreateChannel(recipient, channelCreator, peerConnection,
					        handler, timeoutHandler, connectTimeoutMillis, futureResponse);
					afterConnect(futureResponse, message, channelFuture, handler == null);

					futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
						@Override
						public void operationComplete(FutureResponse future) throws Exception {
							if (future.isFailed()) {
								if (future.responseMessage() != null
								        && future.responseMessage().type() != Message.Type.USER1) {
									clearInactivePeerSocketAddress(futureDone);
									sendTCP(handler, futureResponse, message, channelCreator, idleTCPSeconds,
									        connectTimeoutMillis, peerConnection);
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
					futureResponse.failed("no relay could be contacted", futureDone);
				}
			}
		});
	}

	/**
	 * TODO: say what we are doing here
	 * 
	 * @param peerSocketAddresses
	 * @param pingBuilder
	 * @return
	 */
	private FutureDone<PeerSocketAddress> pingFirst(Collection<PeerSocketAddress> peerSocketAddresses,
	        PingBuilderFactory pingBuilderFactory) {
		final FutureDone<PeerSocketAddress> futureDone = new FutureDone<PeerSocketAddress>();

		FuturePing[] forks = new FuturePing[peerSocketAddresses.size()];
		int index = 0;
		for (PeerSocketAddress psa : peerSocketAddresses) {
			if (psa != null) {
				InetSocketAddress inetSocketAddress = PeerSocketAddress.createSocketUDP(psa);
				PingBuilder pingBuilder = pingBuilderFactory.create();
				forks[index++] = pingBuilder.inetAddress(inetSocketAddress.getAddress())
				        .port(inetSocketAddress.getPort()).start();
			}
		}
		FutureForkJoin<FuturePing> ffk = new FutureForkJoin<FuturePing>(1, true, new AtomicReferenceArray<FuturePing>(
		        forks));
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

	private ChannelFuture sendTCPCreateChannel(InetSocketAddress recipient, ChannelCreator channelCreator,
	        PeerConnection peerConnection, ChannelHandler handler, TimeoutFactory timeoutHandler,
	        int connectTimeoutMillis, FutureResponse futureResponse) {

		final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;

		if (timeoutHandler != null) {
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>();
			handlers.put("timeout0",
			        new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.idleStateHandlerTomP2P()));
			handlers.put("timeout1", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.timeHandler()));
		} else {
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>();
		}

		handlers.put("decoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2PCumulationTCP(
		        channelClientConfiguration.signatureFactory())));
		handlers.put("encoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2POutbound(false,
		        channelClientConfiguration.signatureFactory())));

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

		ChannelFuture channelFuture = channelCreator.createTCP(recipient, connectTimeoutMillis, handlers,
		        futureResponse);

		if (peerConnection != null && channelFuture != null) {
			peerConnection.channelFuture(channelFuture);
			heartBeat.peerConnection(peerConnection);
		}
		return channelFuture;
	}

	private ChannelFuture sendTCPPeerConnection(PeerConnection peerConnection, ChannelHandler handler,
	        final ChannelCreator channelCreator, final FutureResponse futureResponse) {
		// if the channel gets closed, the future should get notified
		ChannelFuture channelFuture = peerConnection.channelFuture();
		// channelCreator can be null if we don't need to create any channels
		if (channelCreator != null) {
			channelCreator.setupCloseListener(channelFuture, futureResponse);
		}
		ChannelPipeline pipeline = channelFuture.channel().pipeline();

		// we need to replace the handler if this comes from the peer that
		// create a peerconnection, otherwise we
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
	 * Send a message via UDP.
	 * 
	 * @param handler
	 *            The handler to deal with a reply message
	 * @param futureResponse
	 *            The future to set the response
	 * @param message
	 *            The message to send
	 * @param channelCreator
	 *            The channel creator for the UPD channel
	 * @param idleUDPSeconds
	 *            The idle time of a message until we fail
	 * @param broadcast
	 *            True to send via layer 2 broadcast
	 */
	// TODO: if message.getRecipient() is me, than call dispatcher directly
	// without sending over Internet.
	public void sendUDP(final SimpleChannelInboundHandler<Message> handler, final FutureResponse futureResponse,
	        final Message message, final ChannelCreator channelCreator, final int idleUDPSeconds,
	        final boolean broadcast) {
		// no need to continue if we already finished
		if (futureResponse.isCompleted()) {
			return;
		}
		removePeerIfFailed(futureResponse, message);

		if (message.sender().isRelayed()) {
			message.peerSocketAddresses(message.sender().peerSocketAddresses());
		}

		boolean isFireAndForget = handler == null;

		final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;
		if (isFireAndForget) {
			final int nrTCPHandlers = 3; // 2 / 0.75
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrTCPHandlers);
		} else {
			final int nrTCPHandlers = 7; // 5 / 0.75
			handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>(nrTCPHandlers);
			final TimeoutFactory timeoutHandler = createTimeoutHandler(futureResponse, idleUDPSeconds, isFireAndForget);
			handlers.put("timeout0",
			        new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.idleStateHandlerTomP2P()));
			handlers.put("timeout1", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler.timeHandler()));
		}

		handlers.put("decoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2PSinglePacketUDP(
		        channelClientConfiguration.signatureFactory())));
		handlers.put("encoder", new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2POutbound(false,
		        channelClientConfiguration.signatureFactory())));
		if (!isFireAndForget) {
			handlers.put("handler", new Pair<EventExecutorGroup, ChannelHandler>(null, handler));
		}
		if (message.recipient().isRelayed() && message.command() != RPC.Commands.NEIGHBOR.getNr()
		        && message.command() != RPC.Commands.PING.getNr()) {
			LOG.warn(
			        "Tried to send UDP message to unreachable peers. Only TCP messages can be sent to unreachable peers: {}",
			        message);
			futureResponse
			        .failed("Tried to send UDP message to unreachable peers. Only TCP messages can be sent to unreachable peers");
		} else {
			final ChannelFuture channelFuture;
			if (message.recipient().isRelayed()) {

				List<PeerSocketAddress> psa = new ArrayList<>(message.recipient().peerSocketAddresses());
				LOG.debug("send neighbor request to random relay peer {}", psa);
				if (psa.size() > 0) {
					PeerSocketAddress ps = psa.get(random.nextInt(psa.size()));
					message.recipientRelay(message.recipient().changePeerSocketAddress(ps).changeRelayed(true));
					channelFuture = channelCreator.createUDP(broadcast, handlers, futureResponse);
				} else {
					futureResponse.failed("Peer is relayed, but no relay given");
					return;
				}
			} else {
				channelFuture = channelCreator.createUDP(broadcast, handlers, futureResponse);
			}
			afterConnect(futureResponse, message, channelFuture, handler == null);
		}
	}

	/**
	 * Create a timeout handler or null if its a fire and forget. In this case
	 * we don't expect a reply and we don't need a timeout.
	 * 
	 * @param futureResponse
	 *            The future to set the response
	 * @param idleMillis
	 *            The timeout
	 * @param fireAndForget
	 *            True, if we don't expect a message
	 * @return The timeout creator that will create timeout handlers
	 */
	private TimeoutFactory createTimeoutHandler(final FutureResponse futureResponse, final int idleMillis,
	        final boolean fireAndForget) {
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
	private void afterConnect(final FutureResponse futureResponse, final Message message,
	        final ChannelFuture channelFuture, final boolean fireAndForget) {
		if (channelFuture == null) {
			futureResponse.failed("could not create a " + (message.isUdp() ? "UDP" : "TCP") + " channel");
			return;
		}
		final Cancel connectCancel = createCancel(channelFuture);
		futureResponse.addCancel(connectCancel);
		channelFuture.addListener(new GenericFutureListener<ChannelFuture>() {
			@Override
			public void operationComplete(final ChannelFuture future) throws Exception {
				futureResponse.removeCancel(connectCancel);
				if (future.isSuccess()) {
					futureResponse.progressHandler(new ProgresHandler() {
						@Override
						public void progres() {
							final ChannelFuture writeFuture = future.channel().writeAndFlush(message);
							afterSend(writeFuture, futureResponse, fireAndForget);
						}
					});
					// this needs to be called first before all other progress
					futureResponse.progressFirst();
				} else {
					LOG.debug("Channel creation failed", future.cause());
					futureResponse.failed("Channel creation failed " + future.channel() + "/" + future.cause());
					// may have been closed by the other side,
					// or it may have been canceled from this side
					if (!(future.cause() instanceof CancellationException)
					        && !(future.cause() instanceof ClosedChannelException)
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
	private void afterSend(final ChannelFuture writeFuture, final FutureResponse futureResponse,
	        final boolean fireAndForget) {
		final Cancel writeCancel = createCancel(writeFuture);
		writeFuture.addListener(new GenericFutureListener<ChannelFuture>() {

			@Override
			public void operationComplete(final ChannelFuture future) throws Exception {
				futureResponse.removeCancel(writeCancel);
				if (!future.isSuccess()) {
					futureResponse.failedLater(future.cause());
					reportFailed(futureResponse, future.channel().close());
					LOG.warn("Failed to write channel the request {} {}", futureResponse.request(), future.cause());
				}
				if (fireAndForget) {
					futureResponse.responseLater(null);
					LOG.debug("fire and forget, close channel now {}, {}", futureResponse.request(), future.channel());
					reportMessage(futureResponse, future.channel().close());
				}
			}
		});

	}

	/**
	 * Report a failure after the channel was closed.
	 * 
	 * @param futureResponse
	 *            The future to set the response
	 * @param close
	 *            The close future
	 * @param cause
	 *            The response message
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
	 * @param responseMessage
	 *            The response message
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
					} else {
						synchronized (peerStatusListeners) {
							for (PeerStatusListener peerStatusListener : peerStatusListeners) {
								peerStatusListener.peerFailed(message.recipient(), new PeerException(future));
							}
						}
					}
				}
			}
		});
	}

	// TODO Nico Rutishauser: Not used by the Recon anymore. Delete or leave?
	public ConcurrentHashMap<Integer, FutureResponse> cachedMessages() {
		return cachedMessages;
	}
}
