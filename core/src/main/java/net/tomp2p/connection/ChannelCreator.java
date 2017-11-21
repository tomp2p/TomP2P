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
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import javassist.NotFoundException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import net.sctp4nat.core.NetworkLink;
import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.core.SctpChannel;
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
import net.tomp2p.utils.Pair;

/**
 * Creates the channels. With this class one can create TCP or UDP channels up
 * to a certain extent. Thus it must be know beforehand how much connections
 * will be created.
 * 
 * @author Thomas Bocek
 */
public class ChannelCreator { // TODO: rename to ChannelClient
	private static final Logger LOG = LoggerFactory.getLogger(ChannelCreator.class);

	private final int maxPermitsUDP;
	private final Semaphore semaphoreUPD;

	// we should be fair, otherwise we see connection timeouts due to unfairness
	// if busy
	private final ReadWriteLock readWriteLockUDP = new ReentrantReadWriteLock(true);
	private final Lock readUDP = readWriteLockUDP.readLock();
	private final Lock writeUDP = readWriteLockUDP.writeLock();

	private final ChannelClientConfiguration channelClientConfiguration;

	private final InetAddress sendFromAddress;

	private volatile boolean shutdownUDP = false;

	private final FutureDone<Void> futureChannelClose;

	private final Set<ClientChannel> channelsUDP = Collections.newSetFromMap(new ConcurrentHashMap<>());

	ChannelCreator(int maxPermitsUDP, final ChannelClientConfiguration channelClientConfiguration,
			InetAddress sendFromAddress, FutureDone<Void> futureChannelClose) {
		this.maxPermitsUDP = maxPermitsUDP;
		this.semaphoreUPD = new Semaphore(maxPermitsUDP);
		this.channelClientConfiguration = channelClientConfiguration;
		this.sendFromAddress = sendFromAddress;
		this.futureChannelClose = futureChannelClose;
	}

	/**
	 * Creates a "channel" to the given address. This won't send any message unlike
	 * TCP.
	 *
	 * @return The channel future object or null if we are shut down
	 */
	public Pair<FutureDone<Message>, FutureDone<ClientChannel>> sendUDP(Message message, int port) {
		FutureDone<Message> futureMessage = new FutureDone<Message>();
		FutureDone<ClientChannel> futureClose = new FutureDone<>();
		DatagramChannel datagramChannel = null;
		readUDP.lock();
		try {
			if (shutdownUDP) {
				return Pair.create(futureMessage.failed("shutdown"), futureClose.done());
			}
			if (!semaphoreUPD.tryAcquire()) {
				final String errorMsg = "Tried to acquire more resources (UDP) than announced.";
				LOG.error(errorMsg);
				return Pair.create(futureMessage.failed(errorMsg), futureClose.done());
			}
			try {
				datagramChannel = DatagramChannel.open();
				DatagramSocket datagramSocket = datagramChannel.socket();
				// default is on my machine 200K, testBroadcastUDP fails with this value, as UDP
				// packets are dropped. Increase to 2MB
				datagramSocket.setReceiveBufferSize(2 * 1024 * 1024);
				datagramSocket.setSendBufferSize(2 * 1024 * 1024);
				datagramSocket.setSoTimeout(13 * 1000);
				datagramSocket.bind(new InetSocketAddress(port));
			} catch (IOException e) {
				if (datagramChannel != null) {
					try {
						datagramChannel.close();
					} catch (IOException e1) {
						// best effort
					}
				}
				futureMessage.failed(e);
				futureClose.failed(e);
			}
			LOG.debug("bound to {}", datagramChannel.socket().getLocalSocketAddress());
			Queue<Message> messageQueue = new LinkedBlockingQueue<>();
			messageQueue.offer(message);
			ClientChannelImpl thread = ClientChannelImpl.of(datagramChannel, futureMessage, futureClose, messageQueue);
			channelsUDP.add(thread);
			futureClose.addListener(new BaseFutureAdapter<FutureDone<ClientChannel>>() {
				@Override
				public void operationComplete(FutureDone<ClientChannel> future) throws Exception {
					channelsUDP.remove(future.object());
					semaphoreUPD.release();
					if (shutdownUDP && semaphoreUPD.availablePermits() == maxPermitsUDP) {
						futureChannelClose.done();
					}
				}
			});
			thread.start();
		} finally {
			readUDP.unlock();
		}
		return Pair.create(futureMessage, futureClose);
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
			for (ClientChannel channelUDP : channelsUDP) {
				try {
					channelUDP.close();
				} catch (IOException e) {
					LOG.debug("could not close {}", channelUDP);
				}
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
	private static class ClientChannelImpl extends Thread implements ClientChannel, NetworkLink {
		final private DatagramChannel datagramChannel;
		final private FutureDone<Message> futureMessage;
		@Getter
		final private FutureDone<ClientChannel> futureClose;
		final private Queue<Message> requestMessages;
		// final private ByteBuffer buffer = ByteBuffer.allocate(65536);
		final private byte[] buffer = new byte[65536];
		private SctpChannel so;

		@Override
		public void run() {
			try {
				boolean isKeepAlive = true;
				Message currentRequestMessage = null;
				while (datagramChannel.isOpen() && isKeepAlive) {
					
					

					if (!requestMessages.isEmpty()) {
						currentRequestMessage = requestMessages.poll();
						if (so == null) {
							so = currentRequestMessage.sctpSocketAdapter();
							so.setLink(this);
						}
						CompositeByteBuf buf2 = Unpooled.compositeBuffer();
						Encoder encoder = new Encoder(new DSASignatureFactory());
						encoder.write(buf2, currentRequestMessage, null);
						PeerAddress recipientAddress = currentRequestMessage.recipient();
						datagramChannel.send(ChannelUtils.convert(buf2),
								recipientAddress.createUDPSocket(currentRequestMessage.sender()));
					}
					
					// TomP2POutbound out = new TomP2POutbound();
					// out.write(datagramChannel, requestMessage, true);
					// if not SCTP, wait for UDP packet
					DatagramPacket p = new DatagramPacket(buffer, buffer.length);
					datagramChannel.socket().receive(p);
					//System.err.println(",");
					InetSocketAddress sender = (InetSocketAddress) p.getSocketAddress();

					// buffer.flip();
					ByteBuf buf = Unpooled.wrappedBuffer(buffer);
					DatagramSocket s = datagramChannel.socket();
					InetSocketAddress recipient = new InetSocketAddress(s.getLocalAddress(), s.getLocalPort());

					if (buf.readableBytes() > 0
							&& MessageHeaderCodec.peekProtocolType(buf.getByte(0)) == ProtocolType.SCTP) {
						buf.skipBytes(1);
						if (so != null) {
							// attention, start offset with 1
							byte[] tmp = new byte[p.getLength()-1];
							System.arraycopy(buf.array(), buf.arrayOffset() + buf.readerIndex(), tmp, 0, p.getLength()- 1);
							so.onConnIn(tmp,0, tmp.length);
						} 

					} else if (buf.readableBytes() > 0
							&& MessageHeaderCodec.peekProtocolType(buf.getByte(0)) == ProtocolType.UDP) {

						Decoder decoder = new Decoder(new DSASignatureFactory());
						boolean finished = decoder.decode(buf, recipient, sender);
						if (!finished) {
							return;
						}
						Message responseMessage = decoder.message().senderSocket(sender);

						// Check if message is good, if not return, channel will be closed
						if (!checkMessage(responseMessage, responseMessage)) {
							// both futures are set!!
							return;
						}

						// TODO make sure request was also keepalive
						if (!responseMessage.isKeepAlive()) {
							// start SCTP
							isKeepAlive = false;
						} 

						// We got a good answer, let's mark the sender as alive
						// if its an announce, the peer status will be handled in the RPC
						if (responseMessage.isOk() || responseMessage.isNotOk()) {

						}
						LOG.debug("peer isVerified: {}", responseMessage.isVerified());
						if (!responseMessage.isVerified()) {
							Message ackMessage = DispatchHandler.createResponseMessage(responseMessage, Type.ACK,
									currentRequestMessage.sender());
							CompositeByteBuf buf2 = Unpooled.compositeBuffer();
							Encoder encoder = new Encoder(new DSASignatureFactory());
							encoder.write(buf2, ackMessage, null);
							PeerAddress recipientAddress = currentRequestMessage.recipient();
							datagramChannel.send(ChannelUtils.convert(buf2),
									recipientAddress.createUDPSocket(currentRequestMessage.sender()));
						}

						futureMessage.done(responseMessage);
					}
				}
			} catch (SocketTimeoutException s) {
				LOG.debug("timout in client", s);
				close("TIMEOUT");
			} catch (Throwable t) {
				LOG.debug("in client", t);
				close();
			}
		}

		@Override
		public void onConnOut(SctpChannelFacade so, byte[] packet) throws IOException, NotFoundException {
			try {
			ByteBuffer buf = ByteBuffer.allocate(packet.length + 1);
			buf.put((byte) (1 << 6));
			buf.put(packet);
			buf.flip();
			//System.err.println("server SCTP in:"+ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(buf))+" to ");
			datagramChannel.send(buf, so.getRemote());
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}

		public void close() {
			close("closed before we got the message");
		}

		public void close(String message) {
			try {
				datagramChannel.close();
			} catch (IOException e) {
				// best effort
			}
			futureClose.done(this);
			futureMessage.failed(message);
		}

		private boolean checkMessage(Message requestMessage, Message responseMessage) throws IOException {

			// Error handling
			if (responseMessage.type() == Message.Type.UNKNOWN_ID) {
				String msg = "Message was not delivered successfully, unknown ID (peer may be offline or unknown RPC handler): "
						+ requestMessage;
				close();
				futureMessage.failed(msg);
				return false;
			}
			if (responseMessage.type() == Message.Type.EXCEPTION) {
				String msg = "Message caused an exception on the other side, handle as peer_abort: " + requestMessage;
				close();
				futureMessage.failed(msg);
				return false;
			}

			final MessageID recvMessageID = new MessageID(responseMessage);
			final MessageID sendMessageID = new MessageID(requestMessage);
			if (!sendMessageID.equals(recvMessageID)) {
				String msg = "Response message [" + responseMessage
						+ "] sent to the node is not the same as we expect. We sent [" + requestMessage + "]";
				close();
				futureMessage.failed(msg);
				return false;
			}
			return true;
		}
	}

}
