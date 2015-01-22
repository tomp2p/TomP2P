package net.tomp2p.relay.buffer;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.SignatureException;

import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayServer;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.RelayUtils;

public abstract class BufferedRelayServer extends BaseRelayServer implements MessageBufferListener<Message> {

	protected final MessageBuffer<Message> buffer;

	protected BufferedRelayServer(Peer peer, PeerAddress unreachablePeer, RelayType relayType,
			MessageBufferConfiguration bufferConfig) {
		super(peer, unreachablePeer, relayType);
		buffer = new MessageBuffer<Message>(bufferConfig);
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

	/**
	 * Retrieves the messages that are ready to send. Ready to send means that they have been buffered and the
	 * Android device has already been notified.
	 * 
	 * @return the buffer containing all buffered messages or <code>null</code> in case no message has been
	 *         buffered
	 */
	public abstract Buffer collectBufferedMessages();

	@Override
	protected void peerMapUpdated(Message originalMessage, Message preparedResponse) {
		// Use the situation to send the buffer to the mobile phone
		Buffer bufferedMessages = collectBufferedMessages();
		if (bufferedMessages != null) {
			preparedResponse.buffer(bufferedMessages);
		}
	}
}
