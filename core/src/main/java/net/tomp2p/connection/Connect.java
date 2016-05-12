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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GenericFutureListener;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Cancel;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataFilter;
import net.tomp2p.message.DataFilterTTL;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.TomP2PCumulationTCP;
import net.tomp2p.message.TomP2POutbound;
import net.tomp2p.message.TomP2PSinglePacketUDP;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
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
public class Connect {

    public static final PeerConnection SELF_PEERCONNECTION_MARKER = new PeerConnection();
	private static final Logger LOG = LoggerFactory.getLogger(Connect.class);
	private final List<PeerStatusListener> peerStatusListeners;
	private final ChannelClientConfiguration channelClientConfiguration;
	private final Dispatcher dispatcher;
	private final SendBehavior sendBehavior;
	private final Random random;
	private final PeerBean peerBean;
	private final DataFilter dataFilterTTL = new DataFilterTTL();

	// this map caches all messages which are meant to be sent by a reverse
	// connection setup
	private final ConcurrentCacheMap<Integer, Message> cachedRequests = new ConcurrentCacheMap<Integer, Message>(30, 1024);


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
	public Connect(final Number160 peerId, final List<PeerStatusListener> peerStatusListeners,
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
	
        //final FutureDone<Pair<PeerConnection,Message>> future = new FutureDone<Pair<PeerConnection,Message>>();
	public Pair<SendBehavior.SendMethod, ChannelCreator.ChannelCloseListener> connectTCP(final SimpleChannelInboundHandler<Message> replHandler, 
                final int connectTimeoutMillis, final PeerAddress sender,
                final PeerConnection peerConnection, final boolean isReflected) {
		
		if (peerConnection.isExisting()) {
                    LOG.debug("go for peer connection / TCP");
                    return new Pair<SendBehavior.SendMethod, ChannelCreator.ChannelCloseListener>(
                            SendBehavior.SendMethod.EXISTING_CONNECTION, null);
                } else {
			IdleStateHandler timeoutHandler = new IdleStateHandler(peerConnection.idleMillis() / 1000, 0, 0);
                        
                        switch (sendBehavior.tcpSendBehavior(dispatcher, sender, peerConnection.remotePeer(), isReflected)) {
			case DIRECT:
                            Pair<ChannelCreator.ChannelCloseListener, ChannelFuture> pair = createChannelTCP(
                                    peerConnection.remotePeer().createTCPSocket(sender), 
                                    peerConnection.channelCreator(), replHandler, timeoutHandler, 
                                    connectTimeoutMillis, peerConnection.isKeepAlive());
                            peerConnection.channelFuture(pair.element1());
                            return new Pair<SendBehavior.SendMethod, ChannelCreator.ChannelCloseListener>(
                                SendBehavior.SendMethod.DIRECT, pair.element0());
			/*case RCON:
                            final PeerSocketAddress peerSocketAddress = prepareRelaySend(
                                    message, peerBean.serverPeerAddress().ipInternalSocket());                            
                            if(peerSocketAddress == null) {
                                throw new IllegalArgumentException("Illegal sending behavior: no relay provided, but relay indicated RCON");
                            }
                            Message rconMessage = createRconMessage(peerSocketAddress, message);
                            if(peerSocketAddress.equals(peerBean.serverPeerAddress().ipv4Socket())) {
                                LOG.debug("Send to self-relay RCON");
                                return new Pair<PeerConnection,Message>(SELF_PEERCONNECTION_MARKER, rconMessage);
                            }
                            return handleRcon(peerSocketAddress, message, rconMessage, channelCreator, connectTimeoutMillis, 
                                        peerConnection);*/
                        /*case HOLEP_RELAY:
                                //handleHolePunch(futureResponse, message, channelCreator, idleTCPMillis, handler, true, handler, channelFuture);
                                return doRelayFallbackTCP(replHandler, futureResponse, message, channelCreator, 
                                        connectTimeoutMillis, peerConnection, timeoutHandler);*/
			case SELF:
                            return new Pair<SendBehavior.SendMethod, ChannelCreator.ChannelCloseListener>(
                                    SendBehavior.SendMethod.SELF, null);
			default:
                            throw new IllegalArgumentException("Illegal sending behavior (1)");
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
	/*private Pair<PeerConnection,Message> handleRcon(PeerSocketAddress ps, final Message message, final Message rconMessage,
			final ChannelCreator channelCreator, final int connectTimeoutMillis, 
                        final PeerConnection peerConnection) {
		LOG.debug("initiate reverse connection setup to peer with peerAddress {}", message.recipient());
                message.keepAlive(true);
		
		
		// cache the original message until the connection is established
		cachedRequests.put(message.messageId(), rconMessage);

		SimpleChannelInboundHandler<Message> rconInboundHandler = new SimpleChannelInboundHandler<Message>() {
			@Override
			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
				if (msg.command() == Commands.RCON.getNr() && msg.type() == Type.OK) {
					LOG.debug("Successfully set up the reverse connection to peer {}", message.recipient().peerId());
				} else {
					LOG.debug("Could not acquire a reverse connection, msg: {}", message);
					cachedRequests.remove(message.messageId());
				}
			}
		};

		// send reverse connection request instead of normal message
		return connectTCP(rconInboundHandler, rconMessage, channelCreator, connectTimeoutMillis, 
                        connectTimeoutMillis, peerConnection);
                
	}*/

	/**
	 * This method makes a copy of the original Message and prepares it for
	 * sending it to the relay.
	 * 
	 * @param message
	 * @return rconMessage
	 */
	private static Message createRconMessage(PeerSocketAddress ps, final Message message) {
		// get Relay InetAddress from unreachable peer
		

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
		readyToSend(message, ps, rconMessage, RPC.Commands.RCON.getNr(), Message.Type.REQUEST_1);

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
		PeerSocket4Address psa = originalMessage.recipient().ipv4Socket();
		
		if(socketAddress instanceof PeerSocket4Address) {
			PeerSocket4Address socketAddress4 = (PeerSocket4Address) socketAddress;
		
		psa = psa.withIpv4(socketAddress4.ipv4())
				.withTcpPort(socketAddress4.tcpPort())
				.withUdpPort(socketAddress4.udpPort());
		
		}
				
		PeerAddress recipient = originalMessage.recipient().withIpv4Socket(psa)
				.withRelaySize(0);
		
		newMessage.recipient(recipient);

		newMessage.command(RPCCommand);
		newMessage.type(messageType);
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
	

	
        
        private Pair<ChannelCreator.ChannelCloseListener, ChannelFuture> createChannelTCP(InetSocketAddress recipient, ChannelCreator channelCreator, 
                ChannelHandler handler, IdleStateHandler timeoutHandler, int connectTimeoutMillis, boolean isKeepAlive) {

		final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>();
		
		if (timeoutHandler != null) {
			handlers.put("timeout", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler));
		}

		handlers.put("decoder",
				new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2PCumulationTCP(channelClientConfiguration.signatureFactory(), 
						channelClientConfiguration.byteBufAllocator())));
		handlers.put(
				"encoder",
				new Pair<EventExecutorGroup, ChannelHandler>(null, new TomP2POutbound(channelClientConfiguration.signatureFactory(),
						channelClientConfiguration.byteBufAllocator())));

		if (isKeepAlive) {
			// we expect replies on this connection
			handlers.put("dispatcher", new Pair<EventExecutorGroup, ChannelHandler>(null, dispatcher));
		}

		if (timeoutHandler != null) {
			handlers.put("handler", new Pair<EventExecutorGroup, ChannelHandler>(null, handler));
		}
		
		return channelCreator.createTCP(recipient, connectTimeoutMillis, handlers);
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
	public Pair<SendBehavior.SendMethod, ChannelCreator.ChannelCloseListener> connectUDP(final SimpleChannelInboundHandler<Message> handler, 
                final ChannelCreator channelCreator, final PeerAddress sender, 
                final PeerConnection peerConnection, final boolean isReflected, final boolean broadcast) {

		final boolean isFireAndForget = handler == null;

		// RTT calculation
		//futureResponse.startRTTMeasurement(true);

		
			
                        
		switch (sendBehavior.udpSendBehavior(dispatcher, sender, peerConnection.remotePeer(), isReflected)) {
		case DIRECT:
                            
                        final IdleStateHandler timeoutHandler = new IdleStateHandler(peerConnection.idleMillis() /1000, 0, 0); 
                        
                        Pair<ChannelCreator.ChannelCloseListener, ChannelFuture> pair = createChannelUDP(peerConnection.remotePeer().createUDPSocket(
                                    sender), channelCreator, handler, timeoutHandler, isFireAndForget);
                        peerConnection.channelFuture(pair.element1());
                        return new Pair<SendBehavior.SendMethod, ChannelCreator.ChannelCloseListener>(
                                SendBehavior.SendMethod.DIRECT, pair.element0());
                            
                        /*case HOLEP_RELAY:
				if (peerBean.holePunchInitiator() != null) {
					handleHolePunch(futureResponse, message, channelCreator, idleUDPMillis, handler, broadcast, handlers);
                                        // all the send mechanics are done in a
					// AbstractHolePuncherStrategy class.
					// Therefore we must execute this return statement.
					return;
                                }
				LOG.debug("No hole punching possible, because There is no PeerNAT. New Attempt with Relaying");
                                doRelayFallbackUDP(futureResponse, message, broadcast, handlers, channelCreator, handler);
                                return;*/
		case SELF:
			LOG.debug("Send to self");
			return new Pair<SendBehavior.SendMethod, ChannelCreator.ChannelCloseListener>(
                                    SendBehavior.SendMethod.SELF, null);
                                         
		default:
			throw new IllegalArgumentException("UDP messages are not allowed to send over RCON");
		}
			
		
	}

	private void handleReflection(final Message message) {
		PeerSocket4Address reflectedRecipient = Utils.natReflection(message.recipient(), dispatcher.peerBean().serverPeerAddress());
		if(reflectedRecipient != null) {
			message.recipientReflected(message.recipient().withIpv4Socket(reflectedRecipient));
			LOG.debug("reflect recipient UDP {}", message);
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
	private PeerSocketAddress prepareRelaySend(final Message message, final PeerSocket4Address preferredAddress) {
		List<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>(message.recipient().relays());
		if (psa.size() > 0) {
			if(psa.contains(preferredAddress)) {
				LOG.debug("send neighbor request to preferred relay peer {} out of {}", preferredAddress, psa);
				return preferredAddress;
			}
			
			while(!psa.isEmpty()) {
				PeerSocketAddress ps = psa.remove(random.nextInt(psa.size()));
				//TODO: prefer local ones
				message.recipientRelay(message.recipient().withIPSocket(ps));
				LOG.debug("send neighbor request to random relay peer {} out of {}", ps, psa);
				return ps;
			}
			LOG.error("no non-reflected relays found");
			return null;
		} else {
			LOG.error("Peer is relayed, but no relay given");
			return null;
		}
	}

	/*private FutureDone<Message> handleHolePunch(final FutureResponse futureResponse, final Message message,
			final ChannelCreator channelCreator, final int idleUDPMillis, final SimpleChannelInboundHandler<Message> handler,
			final boolean broadcast, final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers) {
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
					doRelayFallbackUDP(futureResponse, message, broadcast, handlers, channelCreator, handler);
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				// futureResponse.failed(t);
				// throw new Exception(t);
				LOG.error("The setup of a connection via has been canceled, because an error was thrown");
				t.printStackTrace();
				doRelayFallbackUDP(futureResponse, message, broadcast, handlers, channelCreator, handler);
			}

			
		});
		return fDone;
	}*/
        
        /*private void doRelayFallbackUDP(final FutureResponse futureResponse, final Message message, final boolean broadcast,
		final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers, 
                final ChannelCreator channelCreator, final SimpleChannelInboundHandler<Message> handler) {
				
		PeerSocketAddress ps = prepareRelaySend(message, peerBean.serverPeerAddress().ipv4Socket());
		if(ps == null) {
			futureResponse.failed("no relay provided, but relay indicated HP");
			return;
		}
		if(ps.equals(peerBean.serverPeerAddress().ipv4Socket())) {
			LOG.debug("Send to self-relay HP");
			sendSelf(futureResponse, message);
			return;
		}
                
                ChannelFuture channelFuture = channelCreator.createUDP(broadcast, handlers, futureResponse, false);
                sendMessage(futureResponse, message, channelFuture, handler == null);
	}
        
        private void doRelayFallbackTCP(final SimpleChannelInboundHandler<Message> handler, final FutureResponse futureResponse,
			final Message message, final ChannelCreator channelCreator, final int connectTimeoutMillis,
			final PeerConnection peerConnection, final TimeoutFactory timeoutHandler) {
		
		PeerSocketAddress ps = prepareRelaySend(message, peerBean.serverPeerAddress().ipv4Socket());
		if(ps == null) {
			futureResponse.failed("no relay provided, but relay indicated TCP");
			return;
		}
		if(ps.equals(peerBean.serverPeerAddress().ipv4Socket())) {
			LOG.debug("Send to self-relay TCP");
			sendSelf(futureResponse, message);
			return;
		}
		InetSocketAddress recipient = ps.createTCPSocket();
		ChannelFuture channelFuture = sendTCPCreateChannel(recipient, channelCreator, peerConnection, handler, timeoutHandler, connectTimeoutMillis, futureResponse);
		sendMessage(futureResponse, message, channelFuture, handler == null);
	}*/

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
        
        private Pair<ChannelCreator.ChannelCloseListener, ChannelFuture> createChannelUDP(InetSocketAddress recipient, ChannelCreator channelCreator, 
                ChannelHandler handler, IdleStateHandler timeoutHandler, boolean isFireAndForget) {
            
            final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers = 
                        new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>();
		if (!isFireAndForget) {
			handlers.put("timeout", new Pair<EventExecutorGroup, ChannelHandler>(null, timeoutHandler));
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
            return channelCreator.createUDP(recipient, handlers, isFireAndForget);
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
					if (message.recipient().relaySize() > 0) {
						// TODO: make the relay go away if failed
					} else if (message.command() == RPC.Commands.HOLEP.getNr() && message.type().ordinal() == Message.Type.REQUEST_3.ordinal()) {
						//do nothing, because such a (dummy) message will never reach its target the first time
					}
					if(!future.isCanceled()) {
						LOG.debug("peer failed: {}, {}", message, future);
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

	/**
	 * Get currently cached requests. They are cached because for example the
	 * receiver is behind a NAT. Instead of sending the message directly, a
	 * reverse connection is set up beforehand. After a successful connection
	 * establishment, the cached messages are sent through the direct channel.
	 */
	/*public ConcurrentCacheMap<Integer, Pair<FutureResponse, FutureResponse>> cachedRequests() {
		return cachedRequests;
	}*/

	public List<PeerStatusListener> peerStatusListeners() {
		return peerStatusListeners;
	}
}
