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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.Getter;
import lombok.experimental.Accessors;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Cancel;
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
import net.tomp2p.rpc.RPC;
import net.tomp2p.utils.ConcurrentCacheMap;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

/**
 * The class that sends out messages.
 * 
 * @author Thomas Bocek
 * 
 */

@Accessors(chain = true, fluent = true)
public class Connect {

    public static final PeerConnection SELF_PEERCONNECTION_MARKER = new PeerConnection();
	private static final Logger LOG = LoggerFactory.getLogger(Connect.class);
	private final ChannelClientConfiguration channelClientConfiguration;
	private final Dispatcher dispatcher;
	private final SendBehavior sendBehavior;
	private final Random random;
        @Getter private final CountConnectionOutboundHandler counterUDP = new CountConnectionOutboundHandler();
        @Getter private final CountConnectionOutboundHandler counterTCP = new CountConnectionOutboundHandler();

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
	public Connect(final Number160 peerId, final ChannelClientConfiguration channelClientConfiguration, 
                Dispatcher dispatcher, SendBehavior sendBehavior) {
		this.channelClientConfiguration = channelClientConfiguration;
		this.dispatcher = dispatcher;
		this.sendBehavior = sendBehavior;
		this.random = new Random(peerId.hashCode());
	}

	public ChannelClientConfiguration channelClientConfiguration() {
		return channelClientConfiguration;
	}
	
        //final FutureDone<Pair<PeerConnection,Message>> future = new FutureDone<Pair<PeerConnection,Message>>();
	public SendBehavior.SendMethod connectTCP(final SimpleChannelInboundHandler<Message> replHandler, 
                final int connectTimeoutMillis, final PeerAddress sender,
                final PeerConnection peerConnection, final boolean isReflected) {
		
		if (peerConnection.isExisting()) {
                    LOG.debug("go for peer connection / TCP");
                    return SendBehavior.SendMethod.EXISTING_CONNECTION;
                } else {
			IdleStateHandler timeoutHandler = new IdleStateHandler(peerConnection.idleMillis() / 1000, 0, 0);
                        LOG.debug("Direct TCP connection to : {}, {}", peerConnection.remotePeer(), peerConnection.isKeepAlive());
                        switch (sendBehavior.tcpSendBehavior(dispatcher, sender, peerConnection.remotePeer(), isReflected)) {
			case DIRECT:
                            Pair<ChannelCreator.ChannelCloseListener, ChannelFuture> pair1 = createChannelTCP(
                                    peerConnection.remotePeer().createTCPSocket(sender), 
                                    peerConnection.channelCreator(), replHandler, timeoutHandler, 
                                    connectTimeoutMillis, peerConnection.isKeepAlive());
                            peerConnection.channelFuture(pair1.element1(), pair1.element0());
                            return SendBehavior.SendMethod.DIRECT;
			case RCON:
                            Pair<ChannelCreator.ChannelCloseListener, ChannelFuture> pair2 = createChannelUDP(peerConnection.remotePeer().createUDPSocket(
                                    sender), peerConnection.channelCreator(), null, timeoutHandler, true);
                            peerConnection.channelFuture(pair2.element1(), pair2.element0());
                            return SendBehavior.SendMethod.RCON;
                            /*final PeerSocketAddress peerSocketAddress = prepareRelaySend(
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
                            return SendBehavior.SendMethod.SELF;
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

		final Map<String, ChannelHandler> handlers = new LinkedHashMap<String, ChannelHandler>();
		
		if (timeoutHandler != null) {
			handlers.put("timeout", timeoutHandler);
		}

		handlers.put("decoder",
				new TomP2PCumulationTCP(channelClientConfiguration.signatureFactory(), 
						channelClientConfiguration.byteBufAllocator()));
		handlers.put(
				"encoder",
				new TomP2POutbound(channelClientConfiguration.signatureFactory(),
						channelClientConfiguration.byteBufAllocator()));

		if (isKeepAlive) {
			// we expect replies on this connection
			handlers.put("dispatcher", dispatcher);
		}

                handlers.put("initiater-counter", counterTCP);
		if (timeoutHandler != null) {
			handlers.put("handler", handler);
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
	public SendBehavior.SendMethod connectUDP(final SimpleChannelInboundHandler<Message> handler, 
                final ChannelCreator channelCreator, final PeerAddress sender, 
                final PeerConnection peerConnection, final boolean isReflected, final boolean broadcast) {

		final boolean isFireAndForget = handler == null;

		// RTT calculation
		//futureResponse.startRTTMeasurement(true);

		
			LOG.debug("Direct UDP connection to : {}, {}", peerConnection.remotePeer(), isFireAndForget);
                        
		switch (sendBehavior.udpSendBehavior(dispatcher, sender, peerConnection.remotePeer(), isReflected)) {
		case DIRECT:
                            
                        final IdleStateHandler timeoutHandler = new IdleStateHandler(peerConnection.idleMillis() /1000, 0, 0); 
                        
                        Pair<ChannelCreator.ChannelCloseListener, ChannelFuture> pair = createChannelUDP(peerConnection.remotePeer().createUDPSocket(
                                    sender), channelCreator, handler, timeoutHandler, isFireAndForget);
                        peerConnection.channelFuture(pair.element1(), pair.element0());
                        return SendBehavior.SendMethod.DIRECT;
                            
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
			return SendBehavior.SendMethod.SELF;
                                         
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
            
            final Map<String, ChannelHandler> handlers = 
                        new LinkedHashMap<String, ChannelHandler>();
		if (!isFireAndForget) {
			handlers.put("timeout", timeoutHandler);
		}

		handlers.put(
				"decoder",
				new TomP2PSinglePacketUDP(channelClientConfiguration.signatureFactory()));
		handlers.put(
				"encoder",
				new TomP2POutbound(channelClientConfiguration.signatureFactory(), channelClientConfiguration.byteBufAllocator()));
		handlers.put("initiater-counter", counterUDP);
                if (!isFireAndForget) {
			handlers.put("handler", handler);
		}
            return channelCreator.createUDP(recipient, handlers, isFireAndForget);
        }	
}
