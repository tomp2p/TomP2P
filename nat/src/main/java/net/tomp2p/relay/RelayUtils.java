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

import net.tomp2p.message.Buffer;
import net.tomp2p.message.Decoder;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class RelayUtils {

	public static Buffer encodeMessage(Message message) throws InvalidKeyException, SignatureException, IOException {
		Encoder e = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		e.write(buf, message);
		return new Buffer(buf);
	}

	public static Message decodeMessage(Buffer buf, InetSocketAddress recipient, InetSocketAddress sender)
	        throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException {
		Decoder d = new Decoder(null);
		d.decodeHeader(buf.buffer(), recipient, sender);
		d.decodePayload(buf.buffer());
		return d.message();
	}

	public static List<Map<Number160, PeerStatatistic>> unflatten(Collection<PeerAddress> map, PeerAddress sender) {
		PeerMapConfiguration peerMapConfiguration = new PeerMapConfiguration(sender.getPeerId());
		PeerMap peerMap = new PeerMap(peerMapConfiguration);
		for(PeerAddress peerAddress:map) {
			peerMap.peerFound(peerAddress, null);
		}
		return peerMap.peerMapVerified();
	}

	public static Collection<PeerAddress> flatten(List<Map<Number160, PeerStatatistic>> maps) {
		Collection<PeerAddress> result = new ArrayList<PeerAddress>();
		for (Map<Number160, PeerStatatistic> map : maps) {
			for (PeerStatatistic peerStatatistic : map.values()) {
				result.add(peerStatatistic.getPeerAddress());
			}
		}
		return result;
	}
}
