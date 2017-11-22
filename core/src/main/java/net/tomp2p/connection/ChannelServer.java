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
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import javassist.NotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import net.sctp4nat.connection.SctpConnection;
import net.sctp4nat.connection.SctpUtils;
import net.sctp4nat.core.NetworkLink;
import net.sctp4nat.core.SctpChannel;
import net.sctp4nat.core.SctpChannelFacade;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Decoder;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.ProtocolType;
import net.tomp2p.message.MessageHeaderCodec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The "server" part that accepts connections.
 * 
 * @author Thomas Bocek
 * 
 */

@Accessors(chain = true, fluent = true)
public final class ChannelServer implements DiscoverNetworkListener {

	private static final Logger LOG = LoggerFactory.getLogger(ChannelServer.class);

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

	public static long packetCounterSend() {
		return packetCounterSend.get();
	}

	public static long packetCounterReceive() {
		return packetCounterReceive.get();
	}

	public static void resetCounters() {
		packetCounterSend.set(0);
		packetCounterReceive.set(0);
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
	public ChannelServer(final ChannelServerConfiguration channelServerConfiguration, final Dispatcher dispatcher,
			final ScheduledExecutorService timer) throws IOException {
		this.channelServerConfiguration = channelServerConfiguration;
		this.dispatcher = dispatcher;

		this.discoverNetworks = new DiscoverNetworks(5000, channelServerConfiguration.bindings(), timer);

		discoverNetworks.addDiscoverNetworkListener(this);
		if (timer != null) {
			discoverNetworks.start().awaitUninterruptibly();
		}
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
		if (!channelServerConfiguration.isDisableBind()) {
			synchronized (ChannelServer.this) {
				if (shutdown) {
					return;
				}

				if (discoverResults.isListenAny()) {
					listenAny();
				} else {
					listenSpecificInetAddresses(discoverResults);
				}
			}
		}

	}

	private void listenAny() {

		final InetSocketAddress udpSocket = new InetSocketAddress(channelServerConfiguration.ports().udpPort());
		final boolean udpStart = startupUDP(udpSocket, true);
		if (!udpStart) {
			final boolean udpStart2 = startupUDP(udpSocket, false);
			if (!udpStart2) {
				LOG.warn("cannot bind UDP on socket at all {}", udpSocket);
			} else {
				LOG.warn("can only bind to UDP without broadcast support {}", udpSocket);
			}
		} else {
			LOG.info("Listening UDP on socket {}", udpSocket);
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
		} catch (IOException e) {
			e.printStackTrace();
			LOG.debug("could not connect {}", listenAddresses);
			if (datagramChannel != null) {
				datagramChannel.socket().close();
			}
			return false;
		}
		ServerThread serverThread = ServerThread.of(datagramChannel, dispatcher, listenAddresses);
		serverThread.start();
		channelsUDP.put(listenAddresses.getAddress(), serverThread);
		return true;
	}

	@RequiredArgsConstructor(staticName = "of")
	private static class ServerThread extends Thread {

		final private DatagramChannel datagramChannel;
		final private Dispatcher dispatcher;
		final InetSocketAddress listenAddresses;
		final private ByteBuffer buffer = ByteBuffer.allocate(65536);

		@Override
		public void run() {
			while (datagramChannel.isOpen()) {
				try {
					//LOG.debug("listening for incoming packets on {}", datagramChannel.socket().getLocalSocketAddress());
					buffer.clear();
					// blocks until
					final InetSocketAddress remote = (InetSocketAddress) datagramChannel.receive(buffer);
					packetCounterReceive.incrementAndGet();

					buffer.flip();
					ByteBuf buf = Unpooled.wrappedBuffer(buffer);

					if (buf.readableBytes() > 0
							&& MessageHeaderCodec.peekProtocolType(buf.getByte(0)) == ProtocolType.SCTP) {
						//System.err.println(".");
						buf.skipBytes(1);
						//attention, start offset with 1
						SctpChannel socket = SctpUtils.getMapper().locate(remote.getAddress().getHostAddress(), remote.getPort());
						socket.onConnIn(buf.array(), buf.arrayOffset() + buf.readerIndex(), buf.readableBytes());

					} else if (buf.readableBytes() > 0
							&& MessageHeaderCodec.peekProtocolType(buf.getByte(0)) == ProtocolType.UDP) {

						DatagramSocket s = datagramChannel.socket();
						InetSocketAddress local = new InetSocketAddress(s.getLocalAddress(), s.getLocalPort());

						Decoder decoder = new Decoder(new DSASignatureFactory());
						boolean finished = decoder.decode(buf, local, remote);
						if (!finished) {
							continue;
						}
						Message m = decoder.message();

						Message m2 = dispatcher.dispatch(m);
						if (m2 != null) {

							if(m.sctpChannel()!=null) {
								SctpConnection c = m.sctpChannel();
								LOG.debug("About to connect via SCTP");
								try {
									c.connect(new NetworkLink() {
										
										@Override
										public void onConnOut(SctpChannelFacade so, byte[] packet) throws IOException, NotFoundException {
											try {
											ByteBuffer buf = ByteBuffer.allocate(packet.length+1);
											buf.put((byte)(1 << 6));
											buf.put(packet);
											buf.flip();
											ByteBuf bb =Unpooled.wrappedBuffer(buf);
											byte[] bi = new byte[bb.readableBytes()];
											bb.getBytes(0,bi);
											//System.err.println("server SCTP out:"+ByteBufUtil.prettyHexDump(bb)+" to "+remote);
											datagramChannel.send(ByteBuffer.wrap(bi), remote);
											} catch (Throwable t) {
												t.printStackTrace();										}
											
										}
										
										@Override
										public void close() {
											//TODO handle close
										}
									});
								} catch (Exception e) {
									LOG.error(e.getMessage());
								}
								
							}
							
							if (dispatcher.peerBean().peerMap().checkPeer(m.sender())) {
								m2.verified();
							}
							LOG.debug("peer isVerified: {}", m2.isVerified());

							CompositeByteBuf buf2 = Unpooled.compositeBuffer();
							Encoder encoder = new Encoder(new DSASignatureFactory());
							encoder.write(buf2, m2, null);
							packetCounterSend.incrementAndGet();
							
							System.err.println("server out UDP:"+ByteBufUtil.prettyHexDump(buf2)+" to "+remote);
							
							datagramChannel.send(ChannelUtils.convert(buf2), remote);
						} else {
							LOG.debug("not replying to {}", m);
						}
					}

				} catch (IOException | InvalidKeyException | SignatureException e) {
					if (!datagramChannel.isOpen()) {
						LOG.debug("shutting down {}", listenAddresses);
					} else {
						dispatcher.exceptionCaught(datagramChannel, e);
					}
				}
			}
			LOG.debug("ending loop");
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

}
