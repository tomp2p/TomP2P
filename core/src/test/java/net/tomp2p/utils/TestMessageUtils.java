package net.tomp2p.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.Utils2;
import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;

import org.junit.Assert;
import org.junit.Test;

public class TestMessageUtils {

	private final DSASignatureFactory signatureFactory;

	public TestMessageUtils() {
		signatureFactory = new DSASignatureFactory();
	}

	@Test
	public void testEncodeDecodeRelayedMessage() throws InvalidKeyException, SignatureException, IOException, NoSuchAlgorithmException,
			InvalidKeySpecException {
		Message message = Utils2.createDummyMessage();

		List<PeerSocketAddress> relays = new ArrayList<PeerSocketAddress>();
		relays.add(new PeerSocketAddress(InetAddress.getLocalHost(), 8000, 9000));
		relays.add(new PeerSocketAddress(InetAddress.getLocalHost(), 8001, 9001));
		relays.add(new PeerSocketAddress(InetAddress.getLocalHost(), 8002, 9002));

		PeerAddress sender = Utils2.createAddress().changeRelayed(true).changePeerSocketAddresses(relays)
				.changeFirewalledTCP(true).changeFirewalledUDP(true);
		message.sender(sender);
		message.senderSocket(sender.createSocketTCP());
		
		PeerAddress receiver = Utils2.createAddress();
		message.recipient(receiver);
		message.recipientSocket(receiver.createSocketTCP());
		
		Buffer encoded = MessageUtils.encodeMessage(message, signatureFactory);
		Message decoded = MessageUtils.decodeMessage(encoded, message.recipientSocket(), message.senderSocket(),
				signatureFactory);
		Assert.assertEquals(message.peerSocketAddresses().size(), decoded.peerSocketAddresses().size());
	}
}
