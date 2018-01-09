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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.jdeferred.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import javassist.NotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import net.sctp4nat.connection.NetworkLink;
import net.sctp4nat.core.SctpChannel;
import net.sctp4nat.core.SctpChannelBuilder;
import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.util.SctpInitException;
import net.sctp4nat.util.SctpUtils;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Decoder;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.ProtocolType;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.MessageHeaderCodec;
import net.tomp2p.message.MessageID;
import net.tomp2p.peers.IP;
import net.tomp2p.peers.IP.IPv4;
import net.tomp2p.peers.IP.IPv6;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.utils.ConcurrentCacheMap;
import net.tomp2p.utils.ExpirationHandler;
import net.tomp2p.utils.Pair;

/**
 * The "server" part that accepts connections.
 * 
 * @author Thomas Bocek
 * 
 */

@Accessors(chain = true, fluent = true)
public final class ChannelTransceiver implements DiscoverNetworkListener {

	private static final Logger LOG = LoggerFactory.getLogger(ChannelTransceiver.class);

	private final Map<InetAddress, ServerThread> channelsUDP = Collections
			.synchronizedMap(new HashMap<InetAddress, ServerThread>());
	

	private final FutureDone<Void> futureServerDone = new FutureDone<Void>();

	private final ChannelServerConfiguration channelServerConfiguration;
	private final Dispatcher dispatcher;

	private final DiscoverNetworks discoverNetworks;

	private boolean shutdown = false;
	private boolean broadcastAddressSupported = false;
	private boolean broadcastAddressTried = false;

	private final static AtomicLong packetCounterSend = new AtomicLong();
	private final static AtomicLong packetCounterReceive = new AtomicLong();
	
	private final PeerBean peerBean;
	
	final private ConcurrentCacheMap<MessageID, FutureDone<Message>> pendingMessages = new ConcurrentCacheMap<>(3, 10000);
	
	final private ConcurrentCacheMap<InetSocketAddress, SctpChannel> openConnections = new ConcurrentCacheMap<>(60, 10000);
	
	public static final int MAX_PORT = 65535;
	public static final int MIN_DYN_PORT = 49152;
	
	public volatile DatagramChannel sendingDatagramChannel;
	
	public static long packetCounterSend() {
		return packetCounterSend.get();
	}

	public static long packetCounterReceive() {
		return packetCounterReceive.get();
	}

	public static void resetCounters() {
		packetCounterReceive.set(0);
		packetCounterSend.set(0);
	}

	/**
	 * Sets parameters and starts network device discovery.
	 * 
	 * @param channelServerConfiguration
	 *            The server configuration that contains e.g. the handlers
	 * @param dispatcher
	 *            The shared dispatcher
	 * @param peerStatusListeners
	 *            The status listeners for offline peers
	 * @throws IOException
	 *             If device discovery failed.
	 */
	public ChannelTransceiver(final ChannelServerConfiguration channelServerConfiguration, final Dispatcher dispatcher,
			final ScheduledExecutorService timer, PeerBean peerBean) throws IOException {
		this.channelServerConfiguration = channelServerConfiguration;
		this.dispatcher = dispatcher;
		this.peerBean = peerBean;
		
		this.discoverNetworks = new DiscoverNetworks(5000, channelServerConfiguration.bindings(), timer);

		discoverNetworks.addDiscoverNetworkListener(this);
		//search in this thread and call notifylisteners
		discoverNetworks.discoverInterfaces();
		
		//now start a thread to check periodically for new interfaces
		if (timer != null) {
			discoverNetworks.start();
		}
		
		pendingMessages.expirationHandler(new ExpirationHandler<FutureDone<Message>>() {
			@Override
			public void expired(FutureDone<Message> oldValue) {
				LOG.debug("Timout occured for {}", oldValue);
				oldValue.failed("Timeout occurred");
			}
		});
	}

	public DiscoverNetworks discoverNetworks() {
		return discoverNetworks;
	}

	/**
	 * @return The channel server configuration.
	 */
	public ChannelServerConfiguration channelServerConfiguration() {
		return channelServerConfiguration;
	}

	@Override
	public void discoverNetwork(DiscoverResults discoverResults) {
		if (!channelServerConfiguration.disableBind()) {
			synchronized (ChannelTransceiver.this) {
				if (shutdown) {
					return;
				}
				listenSpecificInetAddresses(discoverResults);
				IPv4 outbound4 = IPv4.outboundInterfaceAddress();
				if(outbound4 != IPv4.WILDCARD) {
					ServerThread serverThread = channelsUDP.get(outbound4.toInet4Address());
					if(serverThread!=null) {
						sendingDatagramChannel = serverThread.datagramChannel;
						return; //we are done
					} else {
						LOG.debug("no matching IPv4 channel found for {}", outbound4);
					}
				} else {
					LOG.debug("outbound IPv4 interface is wildcard");
				}
				IPv6 outbound6 = IPv6.outboundInterfaceAddress();
				if(outbound6 != IPv6.WILDCARD) {
					ServerThread serverThread = channelsUDP.get(outbound6.toInet6Address());
					if(serverThread!=null) {
						sendingDatagramChannel = serverThread.datagramChannel;
						return; //we are done
					} else {
						LOG.debug("no matching IPv6 channel found for {}", outbound6);
					}
				} else {
					LOG.debug("outbound IPv6 interface is wildcard");
				}
			}
		}

	}

	// this method has blocking calls in it
	private void listenSpecificInetAddresses(DiscoverResults discoverResults) {

		/**
		 * Travis-ci has the same inet address as the broadcast adress, handle it
		 * properly.
		 * 
		 * eth0 Link encap:Ethernet HWaddr 42:01:0a:f0:00:19 inet addr:10.240.0.25
		 * Bcast:10.240.0.25 Mask:255.255.255.255 UP BROADCAST RUNNING MULTICAST
		 * MTU:1460 Metric:1 RX packets:849 errors:0 dropped:0 overruns:0 frame:0 TX
		 * packets:914 errors:0 dropped:0 overruns:0 carrier:0 collisions:0
		 * txqueuelen:1000 RX bytes:1080397 (1.0 MB) TX bytes:123816 (123.8 KB)
		 */
		final List<InetSocketAddress> broadcastAddresses = new ArrayList<InetSocketAddress>();

		for (InetAddress inetAddress : discoverResults.newBroadcastAddresses()) {
			InetSocketAddress udpBroadcastSocket = new InetSocketAddress(inetAddress,
					channelServerConfiguration.ports().udpPort());
			broadcastAddressTried = true;
			boolean udpStartBroadcast = startupUDP(udpBroadcastSocket, false);

			if (udpStartBroadcast) {
				// if one broadcast address was found, then we don't need to bind to 0.0.0.0
				broadcastAddressSupported = true;
				broadcastAddresses.add(udpBroadcastSocket);
				LOG.info("Listening on broadcast address: {} on port udp: {}", udpBroadcastSocket,
						channelServerConfiguration.ports().udpPort());
			} else {
				LOG.warn("cannot bind broadcast UDP {}", udpBroadcastSocket);
			}
		}

		for (InetAddress inetAddress : discoverResults.removedFoundBroadcastAddresses()) {
			ServerThread channelUDP = channelsUDP.remove(inetAddress);
			if (channelUDP != null) {
				channelUDP.datagramChannel.socket().close();
			}
		}

		boolean udpStartBroadcast = false;
		// if we tried but could not bind to a broadcast address. Happens on Windows,
		// not on Mac/Linux
		if (!broadcastAddressSupported && broadcastAddressTried) {
			InetSocketAddress udpBroadcastSocket = new InetSocketAddress(channelServerConfiguration.ports().udpPort());
			LOG.info("Listening on wildcard broadcast address {}", udpBroadcastSocket);
			udpStartBroadcast = startupUDP(udpBroadcastSocket, true);
			if (!udpStartBroadcast) {
				LOG.warn("cannot bind wildcard broadcast UDP on socket {}", udpBroadcastSocket);
			}
		}

		for (InetAddress inetAddress : discoverResults.newAddresses()) {
			// as we are listening to anything on UDP, we don't need to listen to any other
			// interfaces
			if (!udpStartBroadcast) {
				InetSocketAddress udpSocket = new InetSocketAddress(inetAddress,
						channelServerConfiguration.ports().udpPort());
				// if we already bound to the inetaddress as bcast and inet are the same
				if (broadcastAddresses.contains(udpSocket)) {
					return;
				}
				boolean udpStart = startupUDP(udpSocket, false);
				if (!udpStart) {
					LOG.warn("cannot bind UDP on socket {}", udpSocket);
				} else {
					LOG.info("Listening on address: {} on port udp: {}", inetAddress,
							channelServerConfiguration.ports().udpPort());
				}
			}
		}

		for (InetAddress inetAddress : discoverResults.removedFoundAddresses()) {
			ServerThread channelUDP = channelsUDP.remove(inetAddress);
			if (channelUDP != null) {
				channelUDP.datagramChannel.socket().close();
			}
		}
	}

	@Override
	public void exception(Throwable throwable) {
		LOG.error("discovery problem", throwable);
	}

	/**
	 * Start to listen on a UPD port.
	 * 
	 * @param listenAddresses
	 *            The address to listen to
	 * @param broadcastFlag
	 * @return True if startup was successful
	 */
	private boolean startupUDP(final InetSocketAddress listenAddresses, boolean broadcastFlag) {
		DatagramChannel datagramChannel = null;
		try {
			datagramChannel = DatagramChannel.open();
			DatagramSocket datagramSocket = datagramChannel.socket();
			datagramSocket.setBroadcast(broadcastFlag);
			// default is on my machine 200K, testBroadcastUDP fails with this value, as UDP
			// packets are dropped. Increase to 2MB
			datagramSocket.setReceiveBufferSize(2 * 1024 * 1024);
			datagramSocket.setSendBufferSize(2 * 1024 * 1024);
			datagramSocket.bind(listenAddresses);
			datagramSocket.setSoTimeout(3 * 1000);
		} catch (IOException e) {
			e.printStackTrace();
			LOG.debug("could not connect {}", listenAddresses);
			if (datagramChannel != null) {
				datagramChannel.socket().close();
			}
			return false;
		}
		ServerThread serverThread = ServerThread.of(
				datagramChannel, 
				dispatcher, 
				listenAddresses, 
				channelServerConfiguration, 
				pendingMessages, 
				openConnections, 
				peerBean, 
				this);
		serverThread.start();
		channelsUDP.put(listenAddresses.getAddress(), serverThread);
		return true;
	}

	@RequiredArgsConstructor(staticName = "of")
	public static class ServerThread extends Thread implements ChannelSender {

		final private DatagramChannel datagramChannel;
		final private Dispatcher dispatcher;
		final InetSocketAddress listenAddresses;
		//final private ByteBuffer buffer = ByteBuffer.allocate(65536);
		final private ChannelServerConfiguration channelServerConfiguration;
		final private Map<MessageID, FutureDone<Message>> pendingMessages;
		final private ConcurrentCacheMap<InetSocketAddress, SctpChannel> openConnections;
		final private PeerBean peerBean;
		final private ChannelTransceiver serv;
		final private byte[] buffer= new byte[65536];

		@Override
		public void run() {
			while (datagramChannel.isOpen()) {
				try {
					LOG.debug("listening for incoming packets on {}", datagramChannel.socket().getLocalSocketAddress());
					
					//the commented code below does not timeout!!
					//https://stackoverflow.com/questions/15337845/how-to-achieve-timeout-handling-in-blocking-datagramchannel-without-using-select
					//final InetSocketAddress remote = (InetSocketAddress) datagramChannel.receive(buffer);
					//buffer.flip();
					//ByteBuf buf = Unpooled.wrappedBuffer(buffer);
					
					DatagramPacket packet = new DatagramPacket(buffer, 65536);
					datagramChannel.socket().receive(packet);
					final InetSocketAddress remote = (InetSocketAddress) packet.getSocketAddress();
					ByteBuf buf = Unpooled.wrappedBuffer(buffer, 0, packet.getLength());
					packetCounterReceive.incrementAndGet();
					//TODO: per channel counter
					
					LOG.debug("got incoming data UDP:"+buf.readableBytes() + " from " + remote);
					
					final ProtocolType type = MessageHeaderCodec.peekProtocolType(buf.getByte(0));
					if (buf.readableBytes() > 0 && type == ProtocolType.SCTP) {
						handleSCTP(remote, buf);

					} else if (buf.readableBytes() > 0 && type == ProtocolType.UDP) {

						Message m = decodeMessage(remote, buf);
						LOG.debug("Message decoded: {}", m);
						
						if(m.isAck()) {
							dispatcher.dispatch(null, m, null, null); //ack, just update peermap
							LOG.debug("ack received");
							continue;
						} else if(m.isRequest()) {
							 
							final Promise<SctpChannelFacade, Exception, Void> p;
							if(m.sctp()) {
								LOG.debug("got request for SCTP connection");
								p = connectSCTP(openConnections, datagramChannel, remote, peerBean.serverPeerAddress().ipv4Socket().udpPort(), m);
							} else {
								LOG.debug("no SCTP connection");
								p = null;
							}
							Responder r = createResponder(remote, m);
							dispatcher.dispatch(r, m, p, this);
					
						} else {
							LOG.debug("peer isVerified: {}, I'm: {}", m.isVerified(), peerBean.serverPeerAddress());
							if (!m.isVerified()) {
								sendAck(m);
							} else {
								LOG.debug("no need for sending ACK");
							}
							
							LOG.debug("looking for message with id {}, I'm {}", new MessageID(m), peerBean.serverPeerAddress());
							FutureDone<Message> currentFuture = pendingMessages.remove(new MessageID(m));
						
							if(currentFuture != null) {
								LOG.debug("message removed: {}",m);
								currentFuture.done(m);
							} else {
								LOG.warn("got response message without sending a request, ignoring... {}", m);
							}
						}
					}

				} catch (SocketTimeoutException s) {
					LOG.debug("nothingt came in...");
					//fail all pending messages. TODO: this should be made separate
					for(FutureDone<Message> future: pendingMessages.values()) {
						future.failed("timeout");
					}
					pendingMessages.clear();
					
				} catch (ClosedChannelException e) {
					LOG.debug("user shut down...");
					for(FutureDone<Message> future: pendingMessages.values()) {
						future.failed("user closed connection");
					}
					pendingMessages.clear();
				}
				catch (Throwable e) {
					LOG.error("error in transceier loop", e);
					for(FutureDone<Message> future: pendingMessages.values()) {
						future.failed("error", e);
					}
					pendingMessages.clear();
				}
			}
			LOG.debug("ending loop");
		}

		private void sendAck(Message m) throws InvalidKeyException, SignatureException, IOException {
			Message ackMessage = DispatchHandler.createAckMessage(m, Type.ACK, peerBean.serverPeerAddress());
			//PeerAddress recipientAddress = m.recipient();
			PeerAddress recipientAddress = peerBean.serverPeerAddress();
			sendNetwork(datagramChannel, m.senderSocket(), ackMessage);
		}

		private Responder createResponder(final InetSocketAddress remote, Message m) {
			Responder r = new Responder() {
				
				@Override
				public void response(Message responseMessage) {
					if (responseMessage != null) {
						if (dispatcher.peerBean().peerMap().checkPeer(m.sender())) {
							responseMessage.verified();
							LOG.debug("peer is known, don't request an ack: {}", m);
						} else {
							LOG.debug("peer is unknown, request an ack: {}", m);
						}
						try {
							sendNetwork(datagramChannel, remote, responseMessage);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							failed(e.getMessage());
						}
					} else {
						LOG.debug("not replying to {}", m);
					}
				}
				
				@Override
				public void failed(String reason) {
					LOG.error(reason);
				}
			};
			return r;
		}

		private Message decodeMessage(final InetSocketAddress remote, ByteBuf buf) {
			DatagramSocket s = datagramChannel.socket();
			InetSocketAddress local = new InetSocketAddress(s.getLocalAddress(), s.getLocalPort());

			Decoder decoder = new Decoder(new DSASignatureFactory());
			boolean finished = decoder.decode(buf, local, remote);
			if (!finished) {
				LOG.error("expecting always full packets!");
			}
			Message m = decoder.message();
			return m;
		}
		
		private void handleSCTP(final InetSocketAddress remote, ByteBuf buf) {
			buf.skipBytes(1); //attention, start offset with 1
			SctpChannel socket = openConnections.get(remote);
			if(socket != null) {
				socket.onConnIn(buf.array(), buf.arrayOffset() + buf.readerIndex(), buf.readableBytes());
				openConnections.putIfAbsent(remote, socket); //refresh timeout
			} else {
				LOG.debug("we have an SCTP message, but no open connections for {}", remote);
			}
		}

		public static Promise<SctpChannelFacade, Exception, Void> connectSCTP(
				final ConcurrentCacheMap<InetSocketAddress, SctpChannel> openConnections, 
				final DatagramChannel datagramChannel, 
				final InetSocketAddress remote,
				final int localPort,
				Message m)
				throws Exception {
			final Promise<SctpChannelFacade, Exception, Void> p;
			
				LOG.debug("setup SCTP connection: {}", m);
				
				SctpChannel sctpChannel = new SctpChannelBuilder()
						.remoteAddress(remote.getAddress())
						.remotePort(remote.getPort())
						.mapper(SctpUtils.getMapper())
						.localSctpPort(localPort).build();
				LOG.debug("local sctp port: {}, remote: {}", localPort, remote);
				openConnections.put(remote, sctpChannel);
				sctpChannel.setLink(new NetworkLink() {
					
					@Override
					public void onConnOut(final SctpChannelFacade so, final byte[] packet, final int tos) throws IOException, NotFoundException {
						try {
							ByteBuffer buf = ByteBuffer.allocate(packet.length+1);
							buf.put((byte)(1 << 6));
							buf.put(packet);
							buf.flip();
							
							if(LOG.isDebugEnabled()) {
								Formatter formatter = new Formatter();
								for (byte b : packet) {
									formatter.format("%02x", b);
								}
								LOG.debug("server out SCTP({}): to {} - {} ",packet.length, remote, formatter.toString());
							}
							
							datagramChannel.send(buf, remote);
						} catch (Throwable t) {
							LOG.error("cannot send",t);
						}
					}
					
					@Override
					public void close() {
						//do nothing, server keeps its connection open
						//TODO
					}
				});

				p = sctpChannel.connect(remote);
			
			return p;
		}

		

		@Override
		public Pair<FutureDone<Message>, FutureDone<SctpChannelFacade>> send(Message message) {
			return serv.send(message, datagramChannel);
		}
		
		
	}

	/**
	 * Shuts down the server.
	 * 
	 * @return The future when the shutdown is complete. This includes the worker
	 *         and boss event loop
	 */
	public FutureDone<Void> shutdown() {
		synchronized (this) {
			shutdown = true;
		}
		discoverNetworks.stop();
		LOG.debug("shutdown servers");
		synchronized (channelsUDP) {
			// TODO: wait until thread is finished
			for (ServerThread channelUDP : channelsUDP.values()) {
				try {
					channelUDP.datagramChannel.close();
				} catch (IOException e) {
					LOG.debug("could not close {}", channelUDP);
				}
			}
		}
		shutdownFuture().done();
		return shutdownFuture();
	}
	
	

	/**
	 * @return The shutdown future that is used when calling {@link #shutdown()}
	 */
	public FutureDone<Void> shutdownFuture() {
		return futureServerDone;
	}

	public Pair<FutureDone<Message>, FutureDone<SctpChannelFacade>> sendUDP(Message message) {
		return send(message, sendingDatagramChannel);
	}
	
	public Pair<FutureDone<Message>, FutureDone<SctpChannelFacade>> send(Message message, DatagramChannel datagramChannel) {
		
		FutureDone<Message> futureMessage = new FutureDone<Message>();
		FutureDone<SctpChannelFacade> futureSCTP = new FutureDone<>();
		
		final InetSocketAddress recipient = findRecipient(message); 
		LOG.debug("sending a UDP message to {} with message {}", recipient, message);
		
		try {
			handleInitSctpSender(recipient, peerBean.serverPeerAddress().ipv4Socket().udpPort(), futureSCTP, openConnections, message);		
		} catch (net.sctp4nat.util.SctpInitException e) {
			LOG.error("cannot init SCTP from the sender", e);
			return Pair.create(futureMessage.failed(e),  futureSCTP.failed(e));
		}
		
		sendRequest(datagramChannel, message, futureMessage, recipient, pendingMessages);
		
		return Pair.create(futureMessage,  futureSCTP);
	}
	
	/**
	 * Find recipient from a message. If the message is a request message, the recipient socket is not
	 * set and a new socket has to be created. If it was a reply, the recipient socket is set and we need
	 * to send it back to that socket 
	 * @param message
	 * @return
	 */
	private static InetSocketAddress findRecipient(Message message) {
		if(message.recipientSocket() != null) {
			LOG.debug("the message has a socket, its a reply: {}", message);
			return message.recipientSocket();
		}
		LOG.debug("the message has no socket, its a request: {}", message);
		return message.recipient().createUDPSocket(message.sender());
	}
	
	private static void handleInitSctpSender(
			final InetSocketAddress recipient,
			final int localPort,
			final FutureDone<SctpChannelFacade> futureSCTP,
			final ConcurrentCacheMap<InetSocketAddress, SctpChannel> openConnections,
			final Message message) throws SctpInitException {
		if(message.sctp()) {
			SctpChannel sctpChannel = ChannelUtils.creatSCTPSocket(
				recipient.getAddress(),
				recipient.getPort(), 
				localPort, 
				futureSCTP);
			openConnections.put(recipient, sctpChannel);
			LOG.debug("SCTP init was requested, port ({}): {}", localPort, message);
		} else {
			LOG.debug("no SCTP init was requested: {}", message);
			futureSCTP.failed("no sctp requested");
		}
	}
	
	private static void sendNetwork(DatagramChannel datagramChannel, final InetSocketAddress remote, Message m2)
			throws InvalidKeyException, SignatureException, IOException {
		
		LOG.debug("peer isVerified: {}", m2.isVerified());

		CompositeByteBuf buf2 = Unpooled.compositeBuffer();
		Encoder encoder = new Encoder(new DSASignatureFactory());
		encoder.write(buf2, m2, null);
		packetCounterSend.incrementAndGet();
				
		LOG.debug("server out UDP {}: {} to {}", m2, ByteBufUtil.prettyHexDump(buf2), remote);
		
		datagramChannel.send(ChannelUtils.convert(buf2), remote);
	}
	
	private static void sendRequest(
			DatagramChannel datagramChannel, 
			Message message, 
			FutureDone<Message> futureMessage, 
			final InetSocketAddress recipient,
			final ConcurrentCacheMap<MessageID, FutureDone<Message>> pendingMessages) {
		//CompositeByteBuf buf2 = Unpooled.compositeBuffer();
		//Encoder encoder = new Encoder(new DSASignatureFactory());
		try {
			//encoder.write(buf2, message, null);
			//LOG.debug("send raw UDP packet to {} with length {} for message {}", recipient, buf2.readableBytes(), message);
			//datagramChannel.send(ChannelUtils.convert(buf2), recipient);
			sendNetwork(datagramChannel, recipient, message);
			// if we send an ack, don't expect any incoming packets
			if (!message.isAck()) {
				LOG.debug("pending message add: {} with id {}", message, new MessageID(message));
				pendingMessages.put(new MessageID(message), futureMessage);
				LOG.debug("we have the following pending messages: {}", pendingMessages.keySet()); 
			}
		} catch (Throwable t) {
			LOG.error("could not send", t);
			futureMessage.failed(t);
		}
	}

}
