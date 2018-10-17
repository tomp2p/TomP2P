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
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import net.tomp2p.network.KCP;
import net.tomp2p.network.KCPListener;
import net.tomp2p.rpc.DataStream;
import net.tomp2p.utils.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.experimental.Accessors;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Decoder;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.ProtocolType;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.MessageHeaderCodec;
import net.tomp2p.message.MessageID;
import net.tomp2p.peers.IP.IPv4;
import net.tomp2p.peers.IP.IPv6;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
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

	private final Map<InetAddress, Pair<ServerThread, PacketThread>> channelsUDP = Collections
			.synchronizedMap(new HashMap<InetAddress, Pair<ServerThread,PacketThread>>());
	

	private final FutureDone<Void> futureServerDone = new FutureDone<Void>();

	private final ChannelServerConfiguration channelServerConfiguration;
	private final Dispatcher dispatcher;

	private final DiscoverNetworks discoverNetworks;

	private boolean shutdown = false;
	private boolean broadcastAddressSupported = false;
	private boolean broadcastAddressTried = false;

	private final static AtomicLong packetCounterSend = new AtomicLong();
	private final static AtomicLong packetCounterReceive = new AtomicLong();
    private final static AtomicLong packetCounterReceiveKCP = new AtomicLong();
	
	private final PeerBean peerBean;
	
	final private ConcurrentCacheMap<MessageID, Pair<Long, FutureDone<Message>>> pendingMessages = new ConcurrentCacheMap<>(3, 10000);

	final private ConcurrentCacheMap<Pair<InetAddress, Integer>, KCP> openConnections = new ConcurrentCacheMap<>(60, 10000);


    final private ConcurrentCacheMap<Pair<InetAddress, Integer>, DataStream> handlers = new ConcurrentCacheMap<>(60, 10000);
	
	public static final int MAX_PORT = 65535;
	public static final int MIN_DYN_PORT = 49152;
	
	public volatile OutgoingData sendingDatagramChannel;
	
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
		
		pendingMessages.expirationHandler(new ExpirationHandler<Pair<Long, FutureDone<Message>>>() {
			@Override
			public void expired(Pair<Long, FutureDone<Message>> oldValue) {
				LOG.debug("Timout occured for {}", oldValue);
				oldValue.element1().failed("Timeout occurred");
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
					Pair<ServerThread, PacketThread> pair = channelsUDP.get(outbound4.toInet4Address());
					if(pair!=null) {
					    ServerThread serverThread = pair.e0();
						sendingDatagramChannel = serverThread.asyncUDPSvr;
						return; //we are done
					} else {
						LOG.debug("no matching IPv4 channel found for {}", outbound4);
					}
				} else {
					LOG.debug("outbound IPv4 interface is wildcard");
				}
				IPv6 outbound6 = IPv6.outboundInterfaceAddress();
				if(outbound6 != IPv6.WILDCARD) {
                    Pair<ServerThread, PacketThread> pair = channelsUDP.get(outbound6.toInet6Address());
					if(pair!=null) {
                        ServerThread serverThread = pair.e0();
						sendingDatagramChannel = serverThread.asyncUDPSvr;
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
					channelServerConfiguration.portLocal());
			broadcastAddressTried = true;
			boolean udpStartBroadcast = startupUDP(udpBroadcastSocket, false);

			if (udpStartBroadcast) {
				// if one broadcast address was found, then we don't need to bind to 0.0.0.0
				broadcastAddressSupported = true;
				broadcastAddresses.add(udpBroadcastSocket);
				LOG.info("Listening on broadcast address: {} on port udp: {}", udpBroadcastSocket,
						channelServerConfiguration.portLocal());
			} else {
				LOG.warn("cannot bind broadcast UDP {}", udpBroadcastSocket);
			}
		}

		for (InetAddress inetAddress : discoverResults.removedFoundBroadcastAddresses()) {
            Pair<ServerThread, PacketThread> pair = channelsUDP.remove(inetAddress);
			if (pair != null) {
                try {
                    pair.e0().shutdown();
                    pair.e1().shutdown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
		}

		boolean udpStartBroadcast = false;
		// if we tried but could not bind to a broadcast address. Happens on Windows,
		// not on Mac/Linux
		if (!broadcastAddressSupported && broadcastAddressTried) {
			InetSocketAddress udpBroadcastSocket = new InetSocketAddress(channelServerConfiguration.portLocal());
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
						channelServerConfiguration.portLocal());
				// if we already bound to the inetaddress as bcast and inet are the same
				if (broadcastAddresses.contains(udpSocket)) {
					return;
				}
				boolean udpStart = startupUDP(udpSocket, false);
				if (!udpStart) {
					LOG.warn("cannot bind UDP on socket {}", udpSocket);
				} else {
					LOG.info("Listening on address: {} on port udp: {}", inetAddress,
							channelServerConfiguration.portLocal());
				}
			}
		}

		for (InetAddress inetAddress : discoverResults.removedFoundAddresses()) {
            Pair<ServerThread, PacketThread> pair = channelsUDP.remove(inetAddress);
			if (pair != null) {
                try {
                    pair.e0().shutdown();
                    pair.e1().shutdown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
		//DatagramChannel datagramChannel = null;
		try {
			/*datagramChannel = DatagramChannel.open();
			DatagramSocket datagramSocket = datagramChannel.socket();
			datagramSocket.setBroadcast(broadcastFlag);
			// default is on my machine 200K, testBroadcastUDP fails with this value, as UDP
			// packets are dropped. Increase to 2MB
			datagramSocket.setReceiveBufferSize(2 * 1024 * 1024);
			datagramSocket.setSendBufferSize(2 * 1024 * 1024);
			datagramSocket.bind(listenAddresses);
			datagramSocket.setSoTimeout(100);*/

            PacketThread packetThread = new PacketThread();
            packetThread.start();

            ServerThread serverThread = new ServerThread(listenAddresses, packetQueue);
            serverThread.start();
            channelsUDP.put(listenAddresses.getAddress(), Pair.of(serverThread, packetThread));

		} catch (IOException e) {
			e.printStackTrace();
			LOG.debug("could not connect {}", listenAddresses);
			/*if (datagramChannel != null) {
				datagramChannel.socket().close();
			}*/
			return false;
		}

		return true;
	}

    public void dataReply(int sessionId, DataStream dataStream) {
	    Pair p = Pair.of(null, sessionId);
        LOG.debug("adding generic reply handler: {}", p);
        handlers.put(p, dataStream);
    }

    public void dataReply(int sessionId, InetAddress inet, DataStream dataStream) {
        Pair p = Pair.of(inet, sessionId);
        LOG.debug("adding specific reply handler: {}", p);
        handlers.put(p, dataStream);
    }

    final private BlockingQueue<Triple<InetSocketAddress, ByteBuffer, OutgoingData>> packetQueue = new LinkedBlockingQueue<>();

    public class PacketThread extends Thread {

        private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
        private boolean running = true;

        @Override
        public void run() {
            while(running) {
                try {
                    Triple<InetSocketAddress, ByteBuffer, OutgoingData> pair = packetQueue.poll(100, TimeUnit.MILLISECONDS);
                    if(!running || (pair!=null && pair.isEmpty())) {
                        packetQueue.clear();
                        shutdownFuture.complete(null);
                        return;
                    }
                    //probably called too often...
                    for(KCP kcp: openConnections.values()) {
                        kcp.update(System.currentTimeMillis());
                    }
                    for(Pair<Long, FutureDone<Message>> future: pendingMessages.values()) {
                        if(future.element0() + 3000 < System.currentTimeMillis()) {
                            future.element1().failed("timeout");
                        }
                    }

                    if(pair == null) {
                        continue;
                    }
                    LOG.debug("got a new packet");
                    OutgoingData outgoingData = pair.e2();

                    final ByteBuffer buffer = pair.e1();
                    if(buffer.remaining() == 0) {
                        continue;
                    }

                    LOG.debug("we have data to send, len: {}", pair.e1().remaining());

                    final InetSocketAddress remote = pair.e0();

                    packetCounterReceive.incrementAndGet();

                    LOG.debug("got incoming data UDP:"+buffer.remaining() + " from " + remote);

                    int offset = buffer.arrayOffset()+buffer.position();

                    byte header = buffer.get(0);
                    buffer.put(0, (byte) (header & 0x3f));

                    byte[] buf = new byte[buffer.remaining()];
                    buffer.get(buf);

                    final ProtocolType type = MessageHeaderCodec.peekProtocolType(header);
                    if (type == ProtocolType.KCP) {
                        LOG.debug("we have KCP!!, count: {}, size: {}. Local {} Remote {}", packetCounterReceiveKCP.incrementAndGet(), buf.length, sendingDatagramChannel.localSocket(), remote);
                        handleKCP(remote, buf);
                    } else if (type == ProtocolType.UDP) {
                        Message m = decodeMessage(remote, buf, outgoingData.localSocket());
                        LOG.debug("Message decoded: {}", m);

                        if(m.isAck()) {
                            dispatcher.dispatch(null, m, null, null); //ack, just update peermap
                            LOG.debug("ack received");
                            continue;
                        } else if(m.isRequest()) {

                            final KCP kcp;
                            if(m.kcp()) {
                                LOG.debug("got request for KCP connection");

                                if(m.relayed() && m.target()) {
                                    InetSocketAddress remoteUdpSocket;
                                    if(!m.peerSocket4AddressList().isEmpty()) {
                                        remoteUdpSocket = m.peerSocket4Address(0).createUDPSocket();
                                    } else if(!m.peerSocket6AddressList().isEmpty()) {
                                        remoteUdpSocket = m.peerSocket6Address(0).createUDPSocket();
                                    } else {
                                        remoteUdpSocket = null; //TOOD: fail
                                    }
                                    int sessionId = m.intAt(0);
                                    kcp = openKCP(sessionId, remote);
                                } else if(!m.relayed()) {
                                    int sessionId = m.intAt(0);
                                    kcp = openKCP(sessionId, remote);
                                } else {
                                    kcp = null;
                                }
                            } else {
                                kcp = null;
                                LOG.debug("no KCP connection");
                            }
                            Responder r = createResponder(remote, m, outgoingData);
                            dispatcher.dispatch(r, m, kcp, new ChannelSender() {
                                @Override
                                public Pair<FutureDone<Message>, KCP> send(Message message) {
                                    return ChannelTransceiver.this.send(message, outgoingData);
                                }
                            });

                        } else {
                            LOG.debug("peer isVerified: {}, I'm: {}", m.isVerified(), peerBean.serverPeerAddress());
                            if (!m.isVerified()) {
                                sendAck(m, outgoingData);
                            } else {
                                LOG.debug("no need for sending ACK");
                            }

                            LOG.debug("looking for message with id {}, I'm {}", new MessageID(m), peerBean.serverPeerAddress());
                            Pair<Long, FutureDone<Message>> currentFuture = pendingMessages.remove(new MessageID(m));

                            if(currentFuture != null) {
                                LOG.debug("message removed: {}",m);
                                currentFuture.element1().done(m);
                            } else {
                                LOG.warn("got response message without sending a request, ignoring... {}", m);
                            }
                        }
                    }



                } catch (Throwable t) {
                    t.printStackTrace();
                }
                System.out.println("done?????");
            }
            packetQueue.clear();
            shutdownFuture.complete(null);
            return;
        }

        private void sendAck(Message m, OutgoingData outgoingData) throws InvalidKeyException, SignatureException, IOException {
            Message ackMessage = DispatchHandler.createAckMessage(m, Type.ACK, peerBean.serverPeerAddress());
            //PeerAddress recipientAddress = m.recipient();
            PeerAddress recipientAddress = peerBean.serverPeerAddress();
            sendNetwork(outgoingData, m.senderSocket(), ackMessage);
        }

        private Responder createResponder(final InetSocketAddress remote, Message m, OutgoingData outgoingData) {
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
                            sendNetwork(outgoingData, remote, responseMessage);
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

        private Message decodeMessage(final InetSocketAddress remote, byte[] buf, InetSocketAddress local) {
            Decoder decoder = new Decoder(new DSASignatureFactory());
            ByteBuf byteBuf = Unpooled.wrappedBuffer(buf);
            boolean finished = decoder.decode(byteBuf, local, remote);
            if (!finished) {
                LOG.error("expecting always full packets!");
            }
            Message m = decoder.message();
            return m;
        }

        private void handleKCP(InetSocketAddress remote, byte[] buf) {
            int sessionId = KCP.conv(buf);
            KCP socket = openKCP(sessionId, remote);

            int ret = socket.input(buf);
            LOG.debug("pass buffer to kcp {}/{}, session: {}", buf.length, ret, sessionId);

            Pair specific = Pair.of(remote.getAddress(), sessionId);
            DataStream ds = handlers.get(specific);
            if(ds == null) {
                Pair generic = Pair.of(null, sessionId);
                System.out.println(generic);
                ds = handlers.get(generic);
            }

            if(ds == null) {
                LOG.warn("no handler found, abort for session: " + sessionId+ "/"+handlers.keySet());
                return;
            }

            byte[] tmp = new byte[32* (1400-24)];
            //byte[] tmp = new byte[9000];
            int r = socket.recv(tmp);

            LOG.debug("recv: {}, myself: {}", r, sendingDatagramChannel.localSocket());

            final KCP socket2 = socket;
            if(r > 0) {
                ByteBuffer b = ByteBuffer.wrap(tmp);
                b.position(0).limit(r);
                ByteBuffer tosend = ds.receiveSend(b, new DataSend(){
                    @Override
                    public void send(ByteBuffer data) {
                        socket2.send(data);
                    }
                });

                if(tosend != null && tosend.remaining() > 0) {
                    System.out.println("send back");
                    socket.send(tosend);
                }
            }
            socket.update(System.currentTimeMillis());
        }

        public CompletableFuture<Void> shutdown() {
            running = false;
            packetQueue.add(Triple.empty());
            return shutdownFuture;
        }
	}

    public class ServerThread extends Thread {

        final AsyncUDPServer asyncUDPSvr;
        public ServerThread(final InetSocketAddress listenAddresses,  BlockingQueue<Triple<InetSocketAddress, ByteBuffer, OutgoingData>> packetBlockingQueue) throws IOException {
            this.asyncUDPSvr = new AsyncUDPServer(new AsyncUDPServer.IncomingData() {
                @Override
                public void incoming(InetSocketAddress sa, ByteBuffer buffer, OutgoingData outgoingData) {
                    if (!packetBlockingQueue.offer(Triple.of(sa, buffer, outgoingData))) {
                        LOG.debug("Very busy right now. Dropping packet..");
                    }
                }
            }, listenAddresses);
        }


        @Override
        public void run() {
            asyncUDPSvr.process();

            LOG.debug("Server shutdown");
            for (Pair<Long, FutureDone<Message>> future : pendingMessages.values()) {
                future.element1().failed("Server shutdown");
            }

            pendingMessages.clear();
            openConnections.clear();


            LOG.debug("ending loop");
        }

        public CompletableFuture<Void> shutdown() throws IOException {
            return asyncUDPSvr.shutdown();
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
		List<CompletableFuture<Void>> list = new ArrayList<>();
		synchronized (channelsUDP) {
			// TODO: wait until thread is finished
			for (Pair<ServerThread, PacketThread> pair : channelsUDP.values()) {
				try {
				    list.add(pair.e0().shutdown());
                    list.add(pair.e1().shutdown());
				} catch (IOException e) {
					LOG.error("could not close", e);
				}
			}
		}
		CompletableFuture.allOf(list.toArray(new CompletableFuture<?>[0])).thenRun(new Runnable() {
            @Override
            public void run() {
                shutdownFuture().done();
            }
        });

		return shutdownFuture();
	}
	
	

	/**
	 * @return The shutdown future that is used when calling {@link #shutdown()}
	 */
	public FutureDone<Void> shutdownFuture() {
		return futureServerDone;
	}

	public Pair<FutureDone<Message>, KCP> sendUDP(Message message) {
		return send(message, sendingDatagramChannel);
	}
	
	
	
	public Pair<FutureDone<Message>, KCP> send(Message message, OutgoingData outgoingData) {
		
		FutureDone<Message> future = new FutureDone<Message>();
        Pair<Long, FutureDone<Message>> pair = new Pair<>(System.currentTimeMillis(), future);
		KCP kcp = null;
		
		InetSocketAddress recipient = findRecipient(message);
		InetSocketAddress firewalledRecipientSocket = recipient;
		LOG.debug("sending a UDP message to {} with message {}", recipient, message);
		
		PeerAddress sender = message.sender();
		PeerAddress receiver = message.recipient();
		
		
		//check if relay is necessary
		if(!message.recipient().reachable4UDP() && !message.relayed() && !message.target()) {
			//we need relay
			if(message.kcp() && message.recipient().portPreserving()) {
				//we need holepunching
				if(peerBean.serverPeerAddress().portPreserving() && !peerBean.serverPeerAddress().reachable4UDP()) {
					//need holepunching -- message 4
					Message holepunching = message.duplicate();
					holepunching.command(RPC.Commands.HOLEPUNCHING.getNr());
					try {
						LOG.debug("fire message {}",message);
						sendNetwork(outgoingData, recipient, holepunching);
					} catch (Exception e) {
						LOG.error("fire hole", e);
						return Pair.create(future.failed(e),  null);
					}
				}
			} else {
				LOG.debug("impossible to connect directly!");
				kcp = null;
			}
			message.relayed(true);
			message.target(false);
			
			if(peerBean.serverPeerAddress().ipv4Socket() != null) {
				message.peerSocketAddress(peerBean.serverPeerAddress().ipv4Socket());
			} else if(peerBean.serverPeerAddress().ipv6Socket() != null) {
				message.peerSocketAddress(peerBean.serverPeerAddress().ipv6Socket());
			} else {
				return null; //todo return failure
			}
			firewalledRecipientSocket = recipient;
			recipient = message.recipient().relays().iterator().next().createUDPSocket();
		}

		if(message.kcp() && message.isRequest()) {
			Pair p = Pair.create(message.recipientSocket().getAddress(), 100);
			kcp = openKCP(100, message.recipientSocket());
		}
		
		try {
			sendNetwork(outgoingData, recipient, message);
			// if we send an ack, don't expect any incoming packets
			if (!message.isAck()) {
				LOG.debug("pending message add: {} with id {}", message, new MessageID(message));
				pendingMessages.put(new MessageID(message), pair);
				LOG.debug("we have the following pending messages: {}", pendingMessages.keySet()); 
			}
		} catch (Throwable t) {
			LOG.error("could not send", t);
            future.failed(t);
		}
		
		return Pair.create(future,  kcp);
	}

	public KCP openKCP(final int sessionId, final InetSocketAddress recipient) {
		Pair p = Pair.of(recipient.getAddress(), sessionId);
		KCP kcp = openConnections.get(p);
		if(kcp == null) {
		    LOG.debug("we have no open connection for {},{}, open a connection", recipient.getAddress(), sessionId);
			kcp = new KCP(sessionId, new KCPListener() {
				@Override
				public void output(byte[] buffer, int offset, int length) {
					buffer[0] = (byte) (1 << 6 | buffer[0] & 0x3F); //flag as KCP
					DatagramPacket p = new DatagramPacket(buffer, offset, length, recipient);
					System.out.println("send datagram to "+p.getSocketAddress());
                    sendingDatagramChannel.send(recipient, buffer, offset, length);
				}
			});
			openConnections.put(p, kcp);
		}
		return kcp;
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

    private static void sendNetwork(OutgoingData outgoingData, final InetSocketAddress remote, Message m2)
            throws InvalidKeyException, SignatureException, IOException {
        LOG.debug("peer isVerified: {}", m2.isVerified());

        CompositeByteBuf buf2 = Unpooled.compositeBuffer();
        Encoder encoder = new Encoder(new DSASignatureFactory());
        encoder.write(buf2, m2, null);
        packetCounterSend.incrementAndGet();

        LOG.debug("server out UDP {}: {} to {}", m2, ByteBufUtil.prettyHexDump(buf2), remote);
        byte[] me = ChannelUtils.convert2(buf2);
        outgoingData.send(remote, me, 0, me.length);
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
}
