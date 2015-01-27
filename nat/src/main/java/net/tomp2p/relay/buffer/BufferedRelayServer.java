package net.tomp2p.relay.buffer;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayServer;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.RelayUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BufferedRelayServer extends BaseRelayServer implements MessageBufferListener<Message> {

	private static final Logger LOG = LoggerFactory.getLogger(BufferedRelayServer.class);

	private final MessageBuffer<Message> buffer;
	private final MessageBufferConfiguration bufferConfig;

	// holds the messages that have already been released from the buffer (because any limit has been
	// triggered or the buffer has been flushed)
	private final List<Message> bufferedMessages;

	protected BufferedRelayServer(Peer peer, PeerAddress unreachablePeer, RelayType relayType,
			MessageBufferConfiguration bufferConfig) {
		super(peer, unreachablePeer, relayType);
		this.bufferConfig = bufferConfig;
		this.buffer = new MessageBuffer<Message>(bufferConfig);
		this.bufferedMessages = Collections.synchronizedList(new ArrayList<Message>());

		buffer.addListener(this);
	}

	/**
	 * Helper method to add the message to the buffer
	 */
	protected void addToBuffer(Message message) throws InvalidKeyException, SignatureException, IOException {
		int messageSize = RelayUtils.getMessageSize(message, connectionBean().channelServer().channelServerConfiguration()
				.signatureFactory());
		buffer.addMessage(message, messageSize);
	}

	@Override
	public void bufferFull(List<Message> messages) {
		synchronized (bufferedMessages) {
			bufferedMessages.addAll(messages);
		}

		onBufferFull();
	}

	/**
	 * Called when the buffer is full and has been triggered. The messages in the buffer are kept in
	 * {@link BufferedRelayServer}.
	 */
	public abstract void onBufferFull();

	@Override
	public void bufferFlushed(List<Message> messages) {
		synchronized (bufferedMessages) {
			bufferedMessages.addAll(messages);
		}
	}

	/**
	 * Retrieves the messages that are ready to send. Ready to send means that they have been buffered and the
	 * Android device has already been notified.
	 * 
	 * @return the buffer containing all buffered messages or <code>null</code> in case no message has been
	 *         buffered
	 */
	public Buffer collectBufferedMessages() {
		// flush the current buffer to get all messages
		buffer.flushNow();

		Buffer buffer = null;;
		synchronized (bufferedMessages) {
			if (bufferedMessages.isEmpty()) {
				LOG.trace("Currently there are no buffered messages");
			} else {
				ByteBuf byteBuffer = RelayUtils.composeMessageBuffer(bufferedMessages, connectionBean().channelServer()
						.channelServerConfiguration().signatureFactory());
				LOG.debug("Buffer of {} messages collected", bufferedMessages.size());
				bufferedMessages.clear();
				buffer = new Buffer(byteBuffer);
			}
		}

		onBufferCollected();
		return buffer;
	}

	/**
	 * Called when the buffer has been collected by the unreachable peer
	 */
	protected abstract void onBufferCollected();

	@Override
	protected void peerMapUpdated(Message originalMessage, Message preparedResponse) {
		// Use the situation to send the buffer to the mobile phone
		Buffer bufferedMessages = collectBufferedMessages();
		if (bufferedMessages != null) {
			preparedResponse.buffer(bufferedMessages);
		}
	}

	/**
	 * Get the buffer configuration. Note, changing the configuraton does not affect the behavior.
	 */
	public MessageBufferConfiguration bufferConfiguration() {
		return bufferConfig;
	}
}
