package net.tomp2p.relay;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.relay.android.MessageBuffer;
import net.tomp2p.utils.MessageUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayUtils {

	private static final Logger LOG = LoggerFactory.getLogger(RelayUtils.class);
	
	private RelayUtils() {
		// only static methods
	}

	public static List<Map<Number160, PeerStatistic>> unflatten(Collection<PeerAddress> map, PeerAddress sender) {
		PeerMapConfiguration peerMapConfiguration = new PeerMapConfiguration(sender.peerId());
		PeerMap peerMap = new PeerMap(peerMapConfiguration);
		for (PeerAddress peerAddress : map) {
			peerMap.peerFound(peerAddress, null, null);
		}
		return peerMap.peerMapVerified();
	}

	public static Collection<PeerAddress> flatten(List<Map<Number160, PeerStatistic>> maps) {
		Collection<PeerAddress> result = new ArrayList<PeerAddress>();
		for (Map<Number160, PeerStatistic> map : maps) {
			for (PeerStatistic peerStatatistic : map.values()) {
				result.add(peerStatatistic.peerAddress());
			}
		}
		return result;
	}

	/**
	 * Basically does the same as
	 * {@link MessageUtils#decodeMessage(Buffer, InetSocketAddress, InetSocketAddress, SignatureFactory)}, but
	 * in addition checks that the relay peers of the decoded message are set correctly
	 */
	public static Message decodeRelayedMessage(Buffer buf, InetSocketAddress recipient, InetSocketAddress sender,
			SignatureFactory signatureFactory) throws InvalidKeyException, NoSuchAlgorithmException,
			InvalidKeySpecException, SignatureException, IOException {
		Message decodedMessage = MessageUtils.decodeMessage(buf, recipient, sender, signatureFactory);
		boolean isRelay = decodedMessage.sender().isRelayed();
		if (isRelay && !decodedMessage.peerSocketAddresses().isEmpty()) {
			PeerAddress tmpSender = decodedMessage.sender().changePeerSocketAddresses(decodedMessage.peerSocketAddresses());
			decodedMessage.sender(tmpSender);
		}
		return decodedMessage;
	}
	
	/**
	 * Composes all messages of a list into a single buffer object, ready to be transmitted over the network.
	 * The composing happens in-order. Alternatively, the message size and then the message is written to the
	 * buffer. Use {@link MessageBuffer#decomposeCompositeBuffer(ByteBuf)} to disassemble.
	 * 
	 * @param messages the messages to compose
	 * @param signatureFactory the signature factory, necessary for encoding the messages
	 * @return a single buffer holding all messages of the list
	 */
	public static ByteBuf composeMessageBuffer(List<Message> messages, SignatureFactory signatureFactory) {
		ByteBuf buffer = Unpooled.buffer();
		for (Message msg : messages) {
			try {
				msg.restoreContentReferences();
				msg.restoreBuffers();
				Buffer encoded = MessageUtils.encodeMessage(msg, signatureFactory);

				buffer.writeInt(encoded.length());
				buffer.writeBytes(encoded.buffer());
			} catch (Exception e) {
				LOG.error("Cannot encode the buffered message. Skip it.", e);
			}
		}
		return buffer;
	}

	/**
	 * Decomposes a buffer containing multiple buffers into an (ordered) list of small buffers. Alternating,
	 * the size of the message and the message itself are encoded in the message buffer. First, the size is
	 * read, then k bytes are read from the buffer (the message). Then again, the size of the next messages is
	 * determined.
	 * 
	 * @param messageBuffer the message buffer
	 * @return a list of buffers
	 */
	public static List<Message> decomposeCompositeBuffer(ByteBuf messageBuffer, InetSocketAddress recipient, InetSocketAddress sender,
			SignatureFactory signatureFactory) {
		List<Message> messages = new ArrayList<Message>();
		while (messageBuffer.readableBytes() > 0) {
			int size = messageBuffer.readInt();
			ByteBuf message = messageBuffer.readBytes(size);
			
			try {
				Message decodedMessage = decodeRelayedMessage(new Buffer(message), recipient, sender, signatureFactory);
				messages.add(decodedMessage);
			} catch (Exception e) {
				LOG.error("Cannot decode buffered message. Skip it.", e);
			}
		}

		return messages;
	}
}
