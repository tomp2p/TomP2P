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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jdeferred.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import javassist.NotFoundException;
import lombok.RequiredArgsConstructor;
import net.sctp4nat.core.NetworkLink;
import net.sctp4nat.core.SctpChannel;
import net.sctp4nat.core.SctpChannelBuilder;
import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.core.SctpMapper;
import net.sctp4nat.core.SctpPorts;
import net.sctp4nat.exception.SctpInitException;
import net.sctp4nat.origin.SctpAcceptable;
import net.sctp4nat.origin.SctpNotification;
import net.sctp4nat.origin.SctpSocket.NotificationListener;
import net.sctp4nat.util.SctpUtils;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Decoder;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.message.MessageHeaderCodec;
import net.tomp2p.message.Message.ProtocolType;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.MessageID;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.utils.ConcurrentCacheMap;
import net.tomp2p.utils.Triple;

/**
 * Creates the channels. With this class one can create TCP or UDP channels up
 * to a certain extent. Thus it must be know beforehand how much connections
 * will be created.
 * 
 * @author Thomas Bocek
 */
public class ChannelClient { // TODO: rename to ChannelClient
	private static final Logger LOG = LoggerFactory.getLogger(ChannelClient.class);

	private final int maxPermitsUDP;
	private final Semaphore semaphoreUPD;

	// we should be fair, otherwise we see connection timeouts due to unfairness
	// if busy
	private final ReadWriteLock readWriteLockUDP = new ReentrantReadWriteLock(true);
	private final Lock readUDP = readWriteLockUDP.readLock();
	private final Lock writeUDP = readWriteLockUDP.writeLock();

	//private final ChannelClientConfiguration channelClientConfiguration;

	//private final InetAddress sendFromAddress;

	private volatile boolean shutdownUDP = false;

	private final FutureDone<Void> futureChannelClose;

	private final Set<MyLink> channelsUDP = Collections.newSetFromMap(new ConcurrentHashMap<>());
	
	final private Map<MessageID, FutureDone<Message>> pendingMessages = new ConcurrentCacheMap<>(60, 10000);
	
	private final Dispatcher dispatcher;

	ChannelClient(int maxPermitsUDP, final ChannelClientConfiguration channelClientConfiguration,
			Dispatcher dispatcher, FutureDone<Void> futureChannelClose) {
		this.maxPermitsUDP = maxPermitsUDP;
		this.semaphoreUPD = new Semaphore(maxPermitsUDP);
		//this.channelClientConfiguration = channelClientConfiguration;
		this.dispatcher = dispatcher;
		this.futureChannelClose = futureChannelClose;
	}

	/**
	 * Creates a "channel" to the given address. This won't send any message unlike
	 * TCP.
	 *
	 * @return The channel future object or null if we are shut down
	 * @throws SctpInitException 
	 */
	public Triple<FutureDone<Message>, FutureDone<SctpChannelFacade>, FutureDone<Void>> sendUDP(Message message) {
		int port = SctpPorts.getInstance().generateDynPort();
		FutureDone<Message> futureMessage = new FutureDone<Message>();
		FutureDone<Void> futureClose = new FutureDone<>();
		FutureDone<SctpChannelFacade> futureSCTP = new FutureDone<>();
		DatagramChannel datagramChannel = null;
		readUDP.lock();
		try {
			if (shutdownUDP) {
				return Triple.create(futureMessage.failed("shutdown"), futureSCTP.failed("shutdown"), futureClose.done());
			}
			if (!semaphoreUPD.tryAcquire()) {
				final String errorMsg = "Tried to acquire more resources (UDP) than announced.";
				LOG.error(errorMsg);
				return Triple.create(futureMessage.failed(errorMsg),  futureSCTP.failed(errorMsg), futureClose.done());
			}
			try {
				datagramChannel = createDatagramSocket(port);
			} catch (IOException e) {
				if(datagramChannel != null) {
					try {
						datagramChannel.close();
					} catch (IOException e1) {
						LOG.debug("cannot close {}", e);
					}
				}
				return Triple.create(futureMessage.failed(e),  futureSCTP.failed(e), futureClose.done());
			}
			LOG.debug("bound to {}", datagramChannel.socket().getLocalSocketAddress());
			MyLink link = MyLink.of(futureClose, datagramChannel, pendingMessages, new MessageID(message), channelsUDP, semaphoreUPD);
			
			final SctpChannel sctpChannel;
			if(message.sctp()) {
				try {
					sctpChannel = creatSCTPSocket(
							message.recipient().ipv4Socket().ipv4().toInetAddress(), 
							message.recipient().ipv4Socket().udpPort(), 
							port, futureSCTP);
					sctpChannel.setLink(link);
				} catch (SctpInitException e) {
					return Triple.create(futureMessage.failed(e),  futureSCTP.failed(e), futureClose.done());
				}
			} else {
				sctpChannel = null;
				futureSCTP.failed("no sctp requested");
			}
			
			CommunictationThread reading = CommunictationThread.of(datagramChannel, pendingMessages, link, sctpChannel, message.sender(), dispatcher);
			reading.start();
			reading.send(message, futureMessage);
			channelsUDP.add(link);
			//remove the link when channel is closed
		} finally {
			readUDP.unlock();
		}
		return Triple.create(futureMessage,  futureSCTP, futureClose);
	}

	private DatagramChannel createDatagramSocket(int port) throws IOException, SocketException {
		DatagramChannel datagramChannel;
		datagramChannel = DatagramChannel.open();
		DatagramSocket datagramSocket = datagramChannel.socket();
		// default is on my machine 200K, testBroadcastUDP fails with this value, as UDP
		// packets are dropped. Increase to 2MB
		datagramSocket.setReceiveBufferSize(2 * 1024 * 1024);
		datagramSocket.setSendBufferSize(2 * 1024 * 1024);
		datagramSocket.setSoTimeout(3 * 1000);
		datagramSocket.bind(new InetSocketAddress(port));
		return datagramChannel;
	}
	
	private SctpChannel creatSCTPSocket(InetAddress remoteAddress, int remoteSctpPort, int localSctpPort, 
			FutureDone<SctpChannelFacade> futureSCTP) throws SctpInitException {
		final SctpChannel socket = new SctpChannelBuilder().localSctpPort(localSctpPort)
				.remoteAddress(remoteAddress).remotePort(remoteSctpPort).mapper(SctpUtils.getMapper()).build();
		socket.listen();

		SctpUtils.getMapper().register(new InetSocketAddress(remoteAddress, remoteSctpPort), socket);
		
		socket.setNotificationListener(new NotificationListener() {

			@Override
			public void onSctpNotification(SctpAcceptable socket2, SctpNotification notification) {
				LOG.debug("SCTP notification {}", notification.toString());
				if (notification.toString().indexOf("ADDR_CONFIRMED") >= 0) {
					futureSCTP.done((SctpChannelFacade) socket);
				} else if (notification.toString().indexOf("SHUTDOWN_COMP") >= 0) {
					socket.close();
				} else if (notification.toString().indexOf("ADDR_UNREACHABLE") >= 0){
					LOG.error("Heartbeat missing! Now shutting down the SCTP connection...");
					socket.close();
				}  else if (notification.toString().indexOf("COMM_LOST") >= 0){
					LOG.error("Communication aborted! Now shutting down the udp connection...");
					socket.close();
				} 
			}
		});
		return socket;
	}

	public boolean isShutdown() {
		return shutdownUDP;
	}

	/**
	 * Shuts down this channel creator. This means that no more TCP or UDP
	 * connections can be established.
	 * 
	 * @return The shutdown future.
	 */
	public void shutdown() {
		// set shutdown flag for UDP and TCP
		// if we acquire a write lock, all read locks are blocked as well
		writeUDP.lock();
		try {
			if (shutdownUDP) {
				return;
			}
			shutdownUDP = true;

			// TODO: wait until thread is finished
			for (MyLink channelUDP : channelsUDP) {
				channelUDP.close();
			}

			if (semaphoreUPD.tryAcquire(maxPermitsUDP)) {
				futureChannelClose.done();
			}

		} finally {
			writeUDP.unlock();
		}
	}

	/**
	 * @return The shutdown future that is used when calling {@link #shutdown()}
	 */
	public FutureDone<Void> futureChannelClose() {
		return futureChannelClose;
	}

	public int availableUDPPermits() {
		return semaphoreUPD.availablePermits();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("sem-udp:");
		sb.append(semaphoreUPD.availablePermits());
		sb.append(",addrUDP:");
		sb.append(semaphoreUPD);
		return sb.toString();
	}
	
	private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
	public static String bytesToHex(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}
	
	@RequiredArgsConstructor(staticName = "of")
	private static class MyLink implements NetworkLink {

		final private FutureDone<Void> futureClose;
		final private DatagramChannel datagramChannel;
		final private Map<MessageID, FutureDone<Message>> pendingMessages;
		final private MessageID messageID;
		private final Set<MyLink> channelsUDP;
		private final Semaphore semaphoreUPD;
		
		@Override
		public void onConnOut(SctpChannelFacade so, byte[] packet, int tos) throws IOException, NotFoundException {
			try {
				ByteBuffer buf = ByteBuffer.allocate(packet.length + 1);
				buf.put((byte) (1 << 6));
				buf.put(packet);
				buf.flip();
				LOG.debug("sending SCTP packet with len:"+packet.length);
				datagramChannel.send(buf, so.getRemote());
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
		
		@Override
		public void close() {
			close("closed before we got the message");
		}

		public void close(String message) {
			channelsUDP.remove(this);
			semaphoreUPD.release();
			LOG.debug("close {}", message);
			try {
				datagramChannel.close();
			} catch (IOException e) {
				// best effort
			}
			futureClose.done();
			FutureDone<Message> future = pendingMessages.remove(messageID);
			if(future != null) {
				future.failed(message);
			}
		}
	}

	@RequiredArgsConstructor(staticName = "of")
	private static class CommunictationThread extends Thread  {
		final private DatagramChannel datagramChannel;
		final private Map<MessageID, FutureDone<Message>> pendingMessages;
		final private MyLink link;
		final private SctpChannel so;
		final private PeerAddress senderPeerAddress;
		final private Dispatcher dispatcher;
		//final private ByteBuffer buffer = ByteBuffer.allocate(65536);
		final private byte[] buffer= new byte[65536];

		@Override
		public void run() {
			try {
				boolean isKeepAlive = true;
				while (datagramChannel.isOpen() && isKeepAlive) {
					LOG.debug("in loop");
					
					//does not timout!!
					//https://stackoverflow.com/questions/15337845/how-to-achieve-timeout-handling-in-blocking-datagramchannel-without-using-select
					//final InetSocketAddress remote = (InetSocketAddress) datagramChannel.receive(buffer);
					
					DatagramPacket packet = new DatagramPacket(buffer, 65536);
					datagramChannel.socket().receive(packet);
					final InetSocketAddress remote = (InetSocketAddress) packet.getSocketAddress();
					
					ByteBuf buf = Unpooled.wrappedBuffer(buffer, 0, packet.getLength());
					LOG.debug("got incoming packet: "+buf.readableBytes());
					
					DatagramSocket s = datagramChannel.socket();
					InetSocketAddress recipient = new InetSocketAddress(s.getLocalAddress(), s.getLocalPort());

					if (buf.readableBytes() > 0
							&& MessageHeaderCodec.peekProtocolType(buf.getByte(0)) == ProtocolType.SCTP) {
						LOG.debug("handling SCTP for so: "+so);
						buf.skipBytes(1);
						if (so != null) {
							// attention, start offset with 1
							//byte[] tmp = new byte[p.getLength()-1];
							//System.arraycopy(buf.array(), buf.arrayOffset() + buf.readerIndex(), tmp, 0, p.getLength()- 1);
							//so.onConnIn(tmp,0, tmp.length);
							//attention, start offset with 1
							so.onConnIn(buf.array(), buf.arrayOffset() + buf.readerIndex(), buf.readableBytes());
							
						} else {
							LOG.debug("don't know what to do...");
						}

					} else if (buf.readableBytes() > 0
							&& MessageHeaderCodec.peekProtocolType(buf.getByte(0)) == ProtocolType.UDP) {
						LOG.debug("handling UDP");
						Decoder decoder = new Decoder(new DSASignatureFactory());
						boolean finished = decoder.decode(buf, recipient, remote);
						if (!finished) {
							return;
						}
						Message responseMessage = decoder.message().senderSocket(remote);

						// Check if message is good, if not return, channel will be closed
						if (!checkMessage(responseMessage)) {
							// both futures are set!!
							return;
						}

						// TODO make sure request was also keepalive
						if (!responseMessage.isKeepAlive()) {
							// start SCTP
							isKeepAlive = false;
						}
						
						if(responseMessage.isRequest() || responseMessage.isAck()) {
							final Promise<SctpChannelFacade, Exception, Void> p = ChannelServer.ServerThread.connectSCTP(datagramChannel, remote, responseMessage);
							Message m2 = dispatcher.dispatch(responseMessage, p);
							if (m2 != null) {
								if (dispatcher.peerBean().peerMap().checkPeer(responseMessage.sender())) {
									m2.verified();
								}
								//send(remote, m2);
								send(m2, null);
							} else {
								LOG.debug("not replying to {}", responseMessage);
							}
						} else {
							LOG.debug("peer isVerified: {}", responseMessage.isVerified());
							if (!responseMessage.isVerified()) {
								Message ackMessage = DispatchHandler.createAckMessage(responseMessage, Type.ACK, senderPeerAddress);
								send(ackMessage, null);
							} else {
								//nothing to send anymore
							}
							
							FutureDone<Message> currentFuture = pendingMessages.get(new MessageID(responseMessage));
						
							if(currentFuture != null) {
								currentFuture.done(responseMessage);
							} else {
								LOG.warn("got response message without sending a request, ignoring...");
							}
						}
					}
				}
				link.close("regular close of datagram channel in reader");
			} catch (SocketTimeoutException s) {
				link.close("TIMEOUT");
			} catch (ClosedChannelException s) {
				link.close("Closed channel");
			} catch (Throwable t) {
				t.printStackTrace();
				LOG.debug("in client {}", t);
				link.close();
			}
		}
		
		public void send(final Message message, final FutureDone<Message> futureMessage) {
			CompositeByteBuf buf2 = Unpooled.compositeBuffer();
			Encoder encoder = new Encoder(new DSASignatureFactory());
			try {
				encoder.write(buf2, message, null);
				PeerAddress recipientAddress = message.recipient();
				datagramChannel.send(ChannelUtils.convert(buf2), recipientAddress.createUDPSocket(message.sender()));

				// if we send an ack, don't expect any incoming packets
				if (!message.isAck() && futureMessage != null) {
					pendingMessages.put(new MessageID(message), futureMessage);
				}
			} catch (Throwable t) {
				t.printStackTrace();
				if (futureMessage != null) {
					futureMessage.failed(t);
				}
				link.close(t.getMessage());
			}
		}

		private boolean checkMessage(Message responseMessage) throws IOException {

			// Error handling
			if (responseMessage.type() == Message.Type.UNKNOWN_ID) {
				String msg = "Message was not delivered successfully, unknown ID (peer may be offline or unknown RPC handler): "
						+ responseMessage;
				link.close(msg);
				return false;
			}
			if (responseMessage.type() == Message.Type.EXCEPTION) {
				String msg = "Message caused an exception on the other side, handle as peer_abort: " + responseMessage;
				link.close(msg);
				return false;
			}

			return true;
		}		
	}
}
