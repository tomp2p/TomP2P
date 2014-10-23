package net.tomp2p.relay;

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
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.utils.MessageUtils;

public class RelayUtils {

	private RelayUtils() {
		// only static methods
	}

	public static List<Map<Number160, PeerStatatistic>> unflatten(Collection<PeerAddress> map, PeerAddress sender) {
		PeerMapConfiguration peerMapConfiguration = new PeerMapConfiguration(sender.peerId());
		PeerMap peerMap = new PeerMap(peerMapConfiguration);
		for (PeerAddress peerAddress : map) {
			peerMap.peerFound(peerAddress, null, null);
		}
		return peerMap.peerMapVerified();
	}

	public static Collection<PeerAddress> flatten(List<Map<Number160, PeerStatatistic>> maps) {
		Collection<PeerAddress> result = new ArrayList<PeerAddress>();
		for (Map<Number160, PeerStatatistic> map : maps) {
			for (PeerStatatistic peerStatatistic : map.values()) {
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
}
