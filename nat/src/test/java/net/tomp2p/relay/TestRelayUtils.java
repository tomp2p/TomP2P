package net.tomp2p.relay;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.InetAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.relay.RelayUtils;

import org.junit.Assert;
import org.junit.Test;

public class TestRelayUtils {

	private final SignatureFactory signature = new DSASignatureFactory();

	@Test
	public void composeDecompose() {
		List<Message> messages = new ArrayList<Message>();
		messages.add(UtilsNAT.createRandomMessage());
		messages.add(UtilsNAT.createRandomMessage());
		messages.add(UtilsNAT.createRandomMessage());
		messages.add(UtilsNAT.createRandomMessage());

		ByteBuf buffer = RelayUtils.composeMessageBuffer(messages, signature);
		List<Message> decomposed = RelayUtils.decomposeCompositeBuffer(buffer, messages.get(0).recipientSocket(), messages.get(1).recipientSocket(), signature);
		
		assertEquals(messages.size(), decomposed.size());
		assertTrue(UtilsNAT.messagesEqual(messages.get(0), decomposed.get(0)));
		assertTrue(UtilsNAT.messagesEqual(messages.get(1), decomposed.get(1)));
		assertTrue(UtilsNAT.messagesEqual(messages.get(2), decomposed.get(2)));
		assertTrue(UtilsNAT.messagesEqual(messages.get(3), decomposed.get(3)));
	}
	

	@Test
	public void testEncodeDecodeRelayedMessage() throws InvalidKeyException, SignatureException, IOException, NoSuchAlgorithmException,
			InvalidKeySpecException {
		Message message = UtilsNAT.createRandomMessage();

		List<PeerSocketAddress> relays = new ArrayList<PeerSocketAddress>();
		relays.add(PeerSocketAddress.create(InetAddress.getLocalHost(), 8000, 9000, 9001));
		relays.add(PeerSocketAddress.create(InetAddress.getLocalHost(), 8001, 9001, 9002));
		relays.add(PeerSocketAddress.create(InetAddress.getLocalHost(), 8002, 9002, 9003));

		PeerAddress sender = UtilsNAT.createRandomAddress().withRelays(relays);;
		PeerAddress receiver = UtilsNAT.createRandomAddress();
		
		message.sender(sender);
		message.senderSocket(sender.createUDPSocket(receiver));
		
		
		message.recipient(receiver);
		message.recipientSocket(receiver.createUDPSocket(sender));
		
		Buffer encoded = RelayUtils.encodeMessage(message, signature);
		Message decoded = RelayUtils.decodeMessage(encoded.buffer(), message.recipientSocket(), message.senderSocket(), signature);
		Assert.assertEquals(message.sender().relays(), decoded.sender().relays());
	}
	
	@Test
	public void testEncodeDecodeString() {
		String test = "dummy";
		Buffer encoded = RelayUtils.encodeString(test);
		String decoded = RelayUtils.decodeString(encoded);
		Assert.assertEquals(test, decoded);
	}
}
